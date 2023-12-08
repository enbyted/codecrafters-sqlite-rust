use anyhow::{bail, Context};
use sqlite_starter_rust::error::Result;
use sqlite_starter_rust::sql::data::{ColumnConstraint, ColumnDefinition, Query, TypeName};
use sqlite_starter_rust::sql::{
    data::{BinaryOperator, Expression, FunctionArguments, Literal},
    parser,
};
use sqlite_starter_rust::{
    db::{
        schema::SchemaEntry,
        table::{TableRow, TableRowCell},
        Database,
    },
    error::DbError,
};
use std::collections::HashMap;
use std::path::PathBuf;

trait ExpressionExecutor {
    fn is_aggregate(&self) -> bool;
    fn begin_aggregate_group(&mut self);
    fn on_row(&mut self, row: &TableRow);
    fn value(&self) -> TableRowCell;
}

struct CountExecutor {
    count: i64,
}

impl CountExecutor {
    pub fn new() -> CountExecutor {
        CountExecutor { count: 0 }
    }
}

impl ExpressionExecutor for CountExecutor {
    fn is_aggregate(&self) -> bool {
        true
    }

    fn begin_aggregate_group(&mut self) {
        self.count = 0;
    }

    fn on_row(&mut self, _row: &TableRow) {
        self.count += 1;
    }

    fn value(&self) -> TableRowCell {
        TableRowCell::Integer(self.count)
    }
}

struct ColumnExtractionExecutor(usize, TableRowCell);

impl ColumnExtractionExecutor {
    pub fn new(column_index: usize) -> ColumnExtractionExecutor {
        ColumnExtractionExecutor(column_index, TableRowCell::Null)
    }
}

impl ExpressionExecutor for ColumnExtractionExecutor {
    fn is_aggregate(&self) -> bool {
        false
    }

    fn begin_aggregate_group(&mut self) {}

    fn on_row(&mut self, row: &TableRow) {
        if let Some(col) = row.cells().skip(self.0).next() {
            self.1 = col.clone();
        } else {
            self.1 = TableRowCell::Null;
        }
    }

    fn value(&self) -> TableRowCell {
        self.1.clone()
    }
}

struct LiteralExecutor(TableRowCell);

impl LiteralExecutor {
    pub fn new(literal: Literal) -> LiteralExecutor {
        match literal {
            Literal::String(str) => LiteralExecutor(TableRowCell::String(str.to_owned())),
            Literal::Integer(val) => LiteralExecutor(TableRowCell::Integer(val)),
        }
    }
}

impl ExpressionExecutor for LiteralExecutor {
    fn is_aggregate(&self) -> bool {
        false
    }

    fn begin_aggregate_group(&mut self) {}

    fn on_row(&mut self, _row: &TableRow) {}

    fn value(&self) -> TableRowCell {
        self.0.clone()
    }
}

struct BinaryOpExecutor {
    left: Box<dyn ExpressionExecutor>,
    right: Box<dyn ExpressionExecutor>,
    op: BinaryOperator,
}

impl BinaryOpExecutor {
    pub fn new(
        left: &Expression,
        right: &Expression,
        op: BinaryOperator,
        column_map: &HashMap<&str, Option<usize>>,
    ) -> Result<'static, BinaryOpExecutor> {
        Ok(BinaryOpExecutor {
            left: create_executor_for_expr(left, column_map)?,
            right: create_executor_for_expr(right, column_map)?,
            op,
        })
    }
}

impl ExpressionExecutor for BinaryOpExecutor {
    fn is_aggregate(&self) -> bool {
        self.left.is_aggregate() || self.right.is_aggregate()
    }

    fn begin_aggregate_group(&mut self) {
        self.left.begin_aggregate_group();
        self.right.begin_aggregate_group();
    }

    fn on_row(&mut self, row: &TableRow) {
        self.left.on_row(row);
        self.right.on_row(row);
    }

    fn value(&self) -> TableRowCell {
        match self.op {
            BinaryOperator::Equals => {
                let left = self.left.value();
                let right = self.right.value();
                if left == right {
                    TableRowCell::Integer(1)
                } else {
                    TableRowCell::Integer(0)
                }
            }
        }
    }
}

fn create_executor_for_expr(
    expr: &Expression,
    column_map: &HashMap<&str, Option<usize>>,
) -> Result<'static, Box<dyn ExpressionExecutor>> {
    match expr {
        Expression::Function { name, arguments } => match *name {
            "COUNT" | "count" => match arguments {
                FunctionArguments::Star => Ok(Box::new(CountExecutor::new())),
                _ => Err(DbError::InvalidArgument(
                    "COUNT(...) function",
                    format!("Only COUNT(*) is implemented, got: {expr:?}"),
                )),
            },
            _ => Err(DbError::InvalidArgument(
                "function",
                format!("Unimplemented, got: {expr:?}"),
            )),
        },
        Expression::ColRef {
            schema: None,
            table: None,
            column,
        } => match column_map.get(column) {
            Some(Some(column_index)) => Ok(Box::new(ColumnExtractionExecutor::new(*column_index))),
            Some(None) => Err(DbError::InvalidArgument(
                "result-column",
                format!("Reading rowid columns is not implemented yet, got: {expr:?}"),
            )),
            None => Err(DbError::ColumnNotFound(column.to_string())),
        },
        Expression::Literal(lit) => Ok(Box::new(LiteralExecutor::new(lit.clone()))),
        Expression::BinaryOp {
            left,
            right,
            operator,
        } => Ok(Box::new(BinaryOpExecutor::new(
            left,
            right,
            operator.clone(),
            column_map,
        )?)),
        _ => Err(DbError::InvalidArgument(
            "result-column",
            format!("Unimplemented, got: {expr:?}"),
        )),
    }
}

fn main() -> anyhow::Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    let db = Database::open(&PathBuf::from(&args[1])).context("Reading database file")?;

    match parser::query(command)? {
        Query::DotCmd("dbinfo") => {
            println!("database page size: {}", db.header().page_size());
            let schema = db.read_schema()?;
            let table_count = schema
                .iter()
                .filter(|t| match t {
                    SchemaEntry::Table { name, .. } => !name.starts_with("sqlite_"),
                    _ => false,
                })
                .count();
            println!("number of tables: {table_count}");
        }
        Query::DotCmd("tables") => {
            let schema = db.read_schema()?;
            let table_names = schema.iter().filter_map(|t| match t {
                SchemaEntry::Table { name, .. } => {
                    if !name.starts_with("sqlite_") {
                        Some(name)
                    } else {
                        None
                    }
                }
                _ => None,
            });
            for table in table_names {
                print!("{table} ");
            }
            println!();
        }
        Query::Select(select) => {
            eprintln!("{select:?}");
            let table = db.read_table(select.table)?;
            eprintln!("Table sql: {}", table.sql());
            let sql = parser::stmt_create_table(table.sql())?;
            let is_rowid = |c: &&ColumnDefinition| {
                c.type_name == TypeName::Integer
                    && c.constraints
                        .iter()
                        .find(|cn| **cn == ColumnConstraint::PrimaryKey)
                        .is_some()
            };

            let mut column_map: HashMap<&str, Option<usize>> = sql
                .columns
                .iter()
                .enumerate()
                .map(|(k, v)| (v.name, Some(k)))
                .collect();

            if let Some(row_id_col) = sql.columns.iter().find(is_rowid) {
                // The rowid column is marked with None, as it will always have NULLs in the payload section
                column_map.insert(row_id_col.name, None);
            }

            eprintln!("Parsed table sql: {sql:?}");
            eprintln!("Column map: {column_map:?}");
            let mut executors = select
                .cols
                .iter()
                .map(|c| create_executor_for_expr(&c.value, &column_map))
                .collect::<Result<Vec<_>>>()?;

            let mut conditions = select
                .conditions
                .iter()
                .map(|c| create_executor_for_expr(c, &column_map))
                .collect::<Result<Vec<_>>>()?;

            let mut results = Vec::new();
            let is_aggregate = executors.iter().any(|e| e.is_aggregate());

            let input = table.iter().filter(move |row| {
                conditions.iter_mut().all(|c| {
                    c.on_row(row);
                    c.value().is_truthy()
                })
            });

            if is_aggregate {
                executors.iter_mut().for_each(|r| r.begin_aggregate_group());
                for row in input {
                    executors.iter_mut().for_each(|e| e.on_row(&row));
                }
                let res: Vec<_> = executors.iter().map(|e| e.value()).collect();
                results.push(res);
            } else {
                for row in input {
                    executors.iter_mut().for_each(|e| e.on_row(&row));
                    let res: Vec<_> = executors.iter().map(|e| e.value()).collect();
                    results.push(res);
                }
            }
            for res in results {
                for (i, col) in res.iter().enumerate() {
                    if i != 0 {
                        print!("|");
                    }
                    print!("{}", col.to_string());
                }
                println!();
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

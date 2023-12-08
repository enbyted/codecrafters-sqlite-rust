use anyhow::{bail, Context};
use sqlite_starter_rust::error::Result;
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

#[derive(Debug, Eq, PartialEq)]
pub enum TypeName {
    Integer,
    Int,
    String,
    Blob,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ColumnConstraint {
    PrimaryKey,
    AutoIncrement,
}

#[derive(Debug)]
pub struct ColumnDefinition<'a> {
    name: &'a str,
    type_name: TypeName,
    constraints: Vec<ColumnConstraint>,
}

#[derive(Debug)]
pub enum FunctionArguments<'a> {
    Star,
    List(Vec<Expression<'a>>),
}

#[derive(Debug)]
pub enum Expression<'a> {
    ColRef {
        schema: Option<&'a str>,
        table: Option<&'a str>,
        column: &'a str,
    },
    Function {
        name: &'a str,
        arguments: FunctionArguments<'a>,
    },
}

#[derive(Debug)]
pub struct ResultColumn<'a> {
    value: Expression<'a>,
    as_name: Option<&'a str>,
}

#[derive(Debug)]
pub struct StmtSelect<'a> {
    cols: Vec<ResultColumn<'a>>,
    table: &'a str,
}

#[derive(Debug)]
pub struct StmtCreateTable<'a> {
    name: &'a str,
    columns: Vec<ColumnDefinition<'a>>,
}

#[derive(Debug)]
pub enum Query<'a> {
    DotCmd(&'a str),
    Select(StmtSelect<'a>),
    CreateTable(StmtCreateTable<'a>),
}

peg::parser! {
    grammar query_parser() for str {
        rule dotcmd() -> Query<'input>
            = "." cmd:$(['a'..='z']+) { Query::DotCmd(cmd) }

        rule _()
            = quiet!{[' ' | '\n' | '\r' | '\t']+}

        rule comma_separator()
            = _? "," _?

        rule farg_star() -> FunctionArguments<'input>
            = "*" { FunctionArguments::Star }

        rule farg_list() -> FunctionArguments<'input>
            = args:expr() ** comma_separator() { FunctionArguments::List(args) }

        rule function_arguments() -> FunctionArguments<'input>
            = farg_star() / farg_list()

        rule expr_function() -> Expression<'input>
            = name:$(['a'..='z' | 'A'..='Z']+) "(" _? arguments:function_arguments() _? ")" { Expression::Function { name, arguments } }

        rule ident_with_dot() -> &'input str
            = value:ident() "." { value }

        rule expr_col_ref() -> Expression<'input>
            = schema:ident_with_dot()? table:ident_with_dot()? column:ident(){ Expression::ColRef { schema, table, column } }

        rule expr() -> Expression<'input>
            = expr_function() / expr_col_ref()

        rule ident() -> &'input str
            = ident_quoted() / ident_unquoted()

        rule ident_unquoted() -> &'input str
            = val:$(['a'..='z' | 'A'..='Z']['a'..='z' | 'A'..='Z' | '0'..='9' | '_']*) { val }

        rule ident_quoted() -> &'input str
            = "`" val:$([^'`']*) "`" { val }

        rule as_ident() -> &'input str
            = _ k("AS")  _ val:ident() { val }

        rule result_column() -> ResultColumn<'input>
            = _? value:expr() as_name:as_ident()? { ResultColumn { value, as_name } }

        rule k(kw: &'static str)
            = input:$([_]*<{kw.len()}>) {? if input.eq_ignore_ascii_case(kw) { Ok(()) } else { Err(kw) }}

        rule stmt_select() -> Query<'input>
            = k("SELECT") _ cols:result_column() ++ comma_separator() _ k("FROM") _ table:ident() { Query::Select(StmtSelect { cols, table }) }

        rule type_name() -> TypeName
            = (k("INTEGER") {TypeName::Integer}) /
              (k("INT") {TypeName::Int}) /
              ((k("CHARACTER") / k("VARCHAR") / k("TEXT")) {TypeName::String}) /
              (k("BLOB") {TypeName::Blob})

        rule signed_number() -> i64
            = val:$(['+' | '-']?['0'..='9']+) {? val.parse().map_err(|e| "Expected signed number") }

        rule column_type() -> TypeName
            = type_name:type_name() (_? "(" _? signed_number()**<1,2> comma_separator() _? ")"  )? { type_name }

        rule column_constraint() -> ColumnConstraint
            = (k("PRIMARY") _ k("KEY") {ColumnConstraint::PrimaryKey}) /
              (k("AUTOINCREMENT") {ColumnConstraint::AutoIncrement})

        rule column_def() -> ColumnDefinition<'input>
            = name:ident() type_name:(_ t:column_type() {t})? _? constraints:column_constraint() ** _ { ColumnDefinition { name, type_name: type_name.unwrap_or(TypeName::Blob), constraints } }

        pub rule stmt_create_table() -> StmtCreateTable<'input>
            = k("CREATE") _ k("TABLE") _ name:ident() _? "(" _? columns:column_def() ++ comma_separator() _? ")" { StmtCreateTable { name, columns } }

        pub rule query() -> Query<'input>
            = dotcmd() / stmt_select()
    }
}

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

    match query_parser::query(command)? {
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
            let sql = query_parser::stmt_create_table(table.sql())?;
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

            let mut results = Vec::new();
            let is_aggregate = executors.iter().any(|e| e.is_aggregate());
            if is_aggregate {
                executors.iter_mut().for_each(|r| r.begin_aggregate_group());
                for row in table.iter() {
                    executors.iter_mut().for_each(|e| e.on_row(&row));
                }
                let res: Vec<_> = executors.iter().map(|e| e.value()).collect();
                results.push(res);
            } else {
                for row in table.iter() {
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

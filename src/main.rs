use anyhow::{bail, Context};
use nom::bytes::complete::is_a;
use sqlite_starter_rust::error::Result;
use sqlite_starter_rust::{
    db::{
        schema::SchemaEntry,
        table::{TableRow, TableRowCell},
        Database,
    },
    error::DbError,
};
use std::path::PathBuf;

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
pub enum Query<'a> {
    DotCmd(&'a str),
    Select(StmtSelect<'a>),
}

peg::parser! {
    grammar query_parser() for str {
        rule dotcmd() -> Query<'input>
            = "." cmd:$(['a'..='z']+) { Query::DotCmd(cmd) }

        rule _()
            = quiet!{[' ']+}

        rule farg_star() -> FunctionArguments<'input>
            = _* "*" { FunctionArguments::Star }

        rule farg_list() -> FunctionArguments<'input>
            = _* args:expr() ** "," { FunctionArguments::List(args) }

        rule function_arguments() -> FunctionArguments<'input>
            = farg_star() / farg_list()

        rule expr_function() -> Expression<'input>
            = _* name:$(['a'..='z' | 'A'..='Z']+) "(" _* arguments:function_arguments() _* ")" { Expression::Function { name, arguments } }

        rule ident_with_dot() -> &'input str
            = value:ident() "." { value }

        rule expr_col_ref() -> Expression<'input>
            = _* schema:ident_with_dot()? table:ident_with_dot()? column:ident(){ Expression::ColRef { schema, table, column } }

        rule expr() -> Expression<'input>
            = expr_function() / expr_col_ref()

        rule ident() -> &'input str
            = ident_quoted() / ident_unquoted()

        rule ident_unquoted() -> &'input str
            = _* val:$(['a'..='z' | 'A'..='Z']['a'..='z' | 'A'..='Z' | '0'..='9' | '_']*) { val }

        rule ident_quoted() -> &'input str
            = _* "`" val:$([^'`']*) "`" { val }

        rule as_ident() -> &'input str
            = "AS" _+ val:ident() { val }

        rule result_column() -> ResultColumn<'input>
            = value:expr() _* as_name:as_ident()? { ResultColumn { value, as_name } }


        rule stmt_select() -> Query<'input>
            = "SELECT" _ cols:result_column() ++ "," _ "FROM" _ table:ident() { Query::Select(StmtSelect { cols, table }) }

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

fn create_executor_for_expr(expr: &Expression) -> Result<'static, Box<dyn ExpressionExecutor>> {
    match expr {
        Expression::Function { name, arguments } => match *name {
            "COUNT" => match arguments {
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
            let mut executors = select
                .cols
                .iter()
                .map(|c| create_executor_for_expr(&c.value))
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
                for col in res {
                    print!("{} ", col.to_string());
                }
                println!();
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

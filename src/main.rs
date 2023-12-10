use anyhow::{bail, Context};
use sqlite_starter_rust::db::query::{ColumnRef, ExecutorFactory};
use sqlite_starter_rust::db::table::TableRow;
use sqlite_starter_rust::db::{schema::SchemaEntry, Database};
use sqlite_starter_rust::error::Result;
use sqlite_starter_rust::sql::data::Query;
use sqlite_starter_rust::sql::parser;
use std::collections::HashMap;
use std::path::PathBuf;

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
        Query::DotCmd(c) => match c.as_ref() {
            "dbinfo" => {
                println!("database page size: {}", db.header().page_size());
                let schema = db.read_schema()?;
                let table_count = schema
                    .iter()
                    .filter(|t| match t {
                        SchemaEntry::Table(table) => !table.is_internal(),
                        _ => false,
                    })
                    .count();
                println!("number of tables: {table_count}");
            }
            "tables" => {
                let schema = db.read_schema()?;
                let table_names = schema.iter().filter_map(|t| match t {
                    SchemaEntry::Table(table) => {
                        if !table.is_internal() {
                            Some(table.name())
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
            "schema" => {
                let schema = db.read_schema()?;
                for table in schema.iter() {
                    println!("{table:?}");
                }
            }
            _ => {
                anyhow::bail!("Invalid command .{}", c);
            }
        },
        Query::Select(select) => {
            eprintln!("{select:?}");
            let table = db.read_table(select.table.as_ref())?;
            eprintln!("Table sql: {:?}", table.sql());

            let column_map: HashMap<&str, ColumnRef> = table
                .sql()
                .columns
                .iter()
                .enumerate()
                .map(|(k, v)| {
                    if v.is_rowid() {
                        (v.name.as_ref(), ColumnRef::Rowid)
                    } else if let Some(index) = table.get_index(v.name.as_ref()) {
                        (v.name.as_ref(), ColumnRef::ColumnWithIndex(k, index))
                    } else {
                        (v.name.as_ref(), ColumnRef::ColumnIndex(k))
                    }
                })
                .collect();

            eprintln!("Column map: {column_map:?}");
            let executor_factory = ExecutorFactory::new(&column_map);
            let mut executors = select
                .cols
                .iter()
                .map(|c| executor_factory.create_executor_for_expr(&c.value))
                .collect::<Result<Vec<_>>>()?;

            let mut conditions = select
                .conditions
                .iter()
                .map(|c| executor_factory.create_executor_for_expr(c))
                .collect::<Result<Vec<_>>>()?;

            if conditions.iter().any(|c| c.is_aggregate()) {
                anyhow::bail!("Conditions with aggregate expressions are unsupported")
            }

            let mut results = Vec::new();
            let is_aggregate = executors.iter().any(|e| e.is_aggregate());

            let input: Box<dyn Iterator<Item = TableRow>> =
                if let Some(condition) = conditions.first() {
                    if condition.can_provide_rowids_from_index() {
                        eprintln!("Using index to accelerate first condition");
                        let (first, rest) = conditions
                            .split_first_mut()
                            .expect("We've already checked that first exists");

                        Box::new(first.filter_map(|rowid| table.get_row(rowid)).filter(
                            move |row| {
                                rest.iter_mut().all(|c| {
                                    c.on_row(row);
                                    c.value().is_truthy()
                                })
                            },
                        ))
                    } else {
                        eprintln!("Not using any indexes");
                        Box::new(table.iter().filter(move |row| {
                            conditions.iter_mut().all(|c| {
                                c.on_row(row);
                                c.value().is_truthy()
                            })
                        }))
                    }
                } else {
                    Box::new(table.iter())
                };

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

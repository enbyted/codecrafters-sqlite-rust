use anyhow::{bail, Context};
use sqlite_starter_rust::db::{schema::SchemaEntry, Database};
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

    match command.as_str() {
        ".dbinfo" => {
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
        ".tables" => {
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
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

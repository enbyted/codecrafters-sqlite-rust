use crate::{
    db::table::TableRowCell,
    error::{DbError, Result},
};

use super::table::TableRow;

#[derive(Debug)]
pub enum SchemaEntry {
    Table {
        name: String,
        table_name: String,
        root_page: u32,
        sql: String,
    },
    Unknown {
        type_text: String,
        name: String,
        table_name: String,
        root_page: u32,
        sql: String,
    },
}

macro_rules! parse_column_with_value {
    ($iter:ident, $name:literal, $type:ident) => {
        match $iter.next() {
            Some(TableRowCell::$type(v)) => Ok(v),
            Some(other) => Err(DbError::InvalidColumnType {
                expected: stringify!($type),
                got: format!("{other:?}"),
            }),
            None => Err(DbError::MissingColumn($name)),
        }
    };
}

impl SchemaEntry {
    pub fn from_row(row: TableRow) -> Result<'static, SchemaEntry> {
        let mut cells = row.cells();

        let type_text = parse_column_with_value!(cells, "type", String)?.clone();
        let name = parse_column_with_value!(cells, "name", String)?.clone();
        let table_name = parse_column_with_value!(cells, "table_name", String)?.clone();
        let root_page = *parse_column_with_value!(cells, "root_page", Integer)? as u32;
        let sql = parse_column_with_value!(cells, "sql", String)?.clone();

        Ok(match type_text.as_str() {
            "table" => SchemaEntry::Table {
                name,
                table_name,
                root_page,
                sql,
            },
            _ => SchemaEntry::Unknown {
                type_text,
                name,
                table_name,
                root_page,
                sql,
            },
        })
    }
}

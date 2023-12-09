use crate::{
    db::table::TableRowCell,
    error::{DbError, Result},
};

use super::table::TableRow;

#[derive(Debug, Clone)]
pub struct SchemaItem {
    name: String,
    table_name: String,
    root_page: u32,
    sql: String,
}

impl SchemaItem {
    pub(crate) fn for_schema_table() -> SchemaItem {
        SchemaItem {
            name: "schema".to_owned(),
            table_name: "schema".to_owned(),
            root_page: 1,
            sql: String::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn is_internal(&self) -> bool {
        self.name.starts_with("sqlite_")
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn root_page(&self) -> u32 {
        self.root_page
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }
}

#[derive(Debug, Clone)]
pub enum SchemaEntry {
    Table(SchemaItem),
    Index(SchemaItem),
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
            "table" => SchemaEntry::Table(SchemaItem {
                name,
                table_name,
                root_page,
                sql,
            }),
            "index" => SchemaEntry::Index(SchemaItem {
                name,
                table_name,
                root_page,
                sql,
            }),
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

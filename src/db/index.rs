use std::fmt::Debug;

use super::{page::btree::ParsedBTreePage, schema::SchemaItem, Database};
use crate::{
    error::{Context, Result},
    sql::parser,
};

#[derive(Clone)]
pub struct Index<'a> {
    database: &'a Database,
    table_name: String,
    column_name: String,
    root_page: ParsedBTreePage<'a>,
}

impl Debug for Index<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Index")
            .field("table_name", &self.table_name)
            .field("column_name", &self.column_name)
            .field("root_page", &self.root_page)
            .finish()
    }
}

impl<'a> Index<'a> {
    pub(crate) fn new(database: &'a Database, schema: SchemaItem) -> Result<'static, Self> {
        let root_page = database
            .parse_btree_page(schema.root_page())
            .add_context("index", schema.name().into())?;

        let sql = parser::stmt_create_index(schema.sql())
            .add_context("index", schema.name().into())
            .add_context("sql", schema.sql().into())?;

        Ok(Index {
            database,
            table_name: schema.table_name().to_string(),
            column_name: sql.column.to_string(),
            root_page,
        })
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }
}

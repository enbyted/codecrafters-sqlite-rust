use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek},
    path::PathBuf,
    sync::{Arc, RwLock},
};

use self::{header::Header, page::Page, parse::Parse, schema::SchemaEntry, table::Table};
use crate::error::{DbError, Result};

pub mod header;
pub(crate) mod page;
pub(crate) mod parse;
pub mod schema;
pub mod table;

#[derive(Debug)]
pub struct Database {
    page_cache: Arc<RwLock<HashMap<u64, Arc<Page>>>>,
    file: Arc<RwLock<File>>,
    header: Header,
}

impl Database {
    pub fn open(path: &PathBuf) -> Result<'static, Database> {
        let mut file = File::open(path)?;
        let mut header_buf = [0u8; 100];
        file.seek(std::io::SeekFrom::Start(0))?;
        file.read_exact(&mut header_buf)?;
        let (_, header) = Header::parse(&header_buf).map_err(|e| e.to_owned())?;

        Ok(Database {
            page_cache: Arc::new(RwLock::new(HashMap::new())),
            file: Arc::new(RwLock::new(file)),
            header,
        })
    }

    pub fn read_schema(&self) -> Result<'static, Vec<SchemaEntry>> {
        let schema_table = Table::new(self, 1, String::new()).map_err(|e| e.to_owned())?;
        schema_table
            .iter()
            .map(|r| SchemaEntry::from_row(r))
            .collect()
    }

    pub fn read_table(&self, table_name: &str) -> Result<'static, Table<'_>> {
        let (root_page, sql) = self
            .read_schema()?
            .into_iter()
            .find_map(|schema| match schema {
                SchemaEntry::Table {
                    name,
                    root_page,
                    sql,
                    ..
                } if name == table_name => Some((root_page, sql)),
                _ => None,
            })
            .ok_or_else(|| DbError::TableNotFound(table_name.to_owned()))?;

        Table::new(self, root_page, sql).map_err(|e| e.to_owned())
    }

    fn read_btree_page(&self, page_id: u32) -> Result<'static, Arc<Page>> {
        let mut file = self.file.write()?;
        let mut page_cache = self.page_cache.write()?;

        let page_offset = match page_id {
            0 => Err(DbError::InvalidArgument("page_id", "0".into())),
            _ => Ok((page_id as u64 - 1) * self.header.page_size() as u64),
        }?;
        let page = if let Some(cached_page) = page_cache.get(&page_offset) {
            cached_page.clone()
        } else {
            file.seek(std::io::SeekFrom::Start(page_offset))?;
            let mut page_data = Vec::new();
            page_data.resize(self.header.page_size(), 0u8);
            file.read_exact(&mut page_data)?;
            let offset_from_start = match page_offset {
                0 => 100,
                _ => 0,
            };
            page_data.rotate_left(offset_from_start);
            page_data.truncate(self.header.usable_page_size() - offset_from_start);
            let page = Arc::new(Page::new(page_data, offset_from_start));
            page_cache.insert(page_offset, page.clone());
            page
        };

        Ok(page)
    }

    pub fn read_overflow_data(&self, _first_overflow_page_id: u32) -> Result<'static, Vec<u8>> {
        todo!();
    }

    pub fn header(&self) -> &Header {
        &self.header
    }
}

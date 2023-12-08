use crate::error::{ParseResult, Result};
use nom::bytes::complete as bytes;
use std::sync::Arc;

use super::{
    page::{
        btree::{CellData, ParsedBTreePage},
        Page,
    },
    parse::{parse_int, parse_varint, Parse, ParseWithBlockOffset},
    Database,
};

#[derive(Debug)]
pub struct Table<'a> {
    database: &'a Database,
    #[allow(dead_code)]
    /// This exists to guarantee that the reference in root_page will be valid
    root_page_data: Arc<Page>,
    root_page: ParsedBTreePage<'a>,
    sql: String,
}

impl<'a> Table<'a> {
    pub(crate) fn new(
        database: &'a Database,
        root_page_id: u32,
        sql: String,
    ) -> Result<'a, Table<'a>> {
        let root_page_data = database.read_btree_page(root_page_id)?;
        let (_, root_page) = ParsedBTreePage::parse_in_block(
            unsafe { std::mem::transmute(root_page_data.data()) },
            database.header().usable_page_size(),
            root_page_data.offset_from_start(),
        )
        .map_err(|e| e.to_owned())?;

        Ok(Table {
            database,
            root_page_data: root_page_data.clone(),
            root_page: root_page as ParsedBTreePage<'a>,
            sql,
        })
    }

    pub fn iter<'b>(&'b self) -> TableIterator<'a, 'b> {
        TableIterator::new(self)
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }
}

#[derive(Debug, Clone)]
pub enum TableRowCell {
    Null,
    Integer(i64),
    Float(f64),
    Blob(Vec<u8>),
    String(String),
}

impl ToString for TableRowCell {
    fn to_string(&self) -> String {
        match self {
            Self::Null => "NULL".to_owned(),
            Self::String(s) => s.clone(),
            Self::Integer(i) => i.to_string(),
            Self::Float(f) => f.to_string(),
            Self::Blob(b) => format!("Blob: {b:x?}"),
        }
    }
}

impl TableRowCell {
    fn parse<'a>(tag: i64, data: &'a [u8]) -> ParseResult<'a, Self> {
        let (data, cell) = match tag {
            0 => (data, TableRowCell::Null),
            1 => parse_int(data, 1).map(|(d, v)| (d, TableRowCell::Integer(v)))?,
            2 => parse_int(data, 2).map(|(d, v)| (d, TableRowCell::Integer(v)))?,
            3 => parse_int(data, 3).map(|(d, v)| (d, TableRowCell::Integer(v)))?,
            4 => parse_int(data, 4).map(|(d, v)| (d, TableRowCell::Integer(v)))?,
            5 => parse_int(data, 6).map(|(d, v)| (d, TableRowCell::Integer(v)))?,
            6 => parse_int(data, 8).map(|(d, v)| (d, TableRowCell::Integer(v)))?,
            7 => f64::parse(data).map(|(d, v)| (d, TableRowCell::Float(v)))?,
            8 => (data, TableRowCell::Integer(0)),
            9 => (data, TableRowCell::Integer(0)),
            10 => todo!(),
            11 => todo!(),
            _ => {
                if tag % 2 == 0 {
                    let len = ((tag - 12) / 2) as usize;
                    let (data, value) = bytes::take(len)(data)?;
                    (data, TableRowCell::Blob(value.into()))
                } else {
                    let len = ((tag - 13) / 2) as usize;
                    let (data, value) = bytes::take(len)(data)?;
                    // TODO: Support other text encodings
                    let value = std::str::from_utf8(value)?;
                    (data, TableRowCell::String(value.to_owned()))
                }
            }
        };
        Ok((data, cell))
    }
}

#[derive(Debug)]
pub struct TableRow {
    rowid: i64,
    cells: Vec<TableRowCell>,
}

impl TableRow {
    fn from_cell_data(data: &[u8]) -> ParseResult<'_, TableRow> {
        let (data, rowid) = parse_varint(data)?;
        let mut cells = Vec::new();

        let prev_data = data;
        let (data, header_len) = parse_varint(data)?;
        let header_size_len = data.as_ptr() as usize - prev_data.as_ptr() as usize;
        let (header, data) = data.split_at(header_len as usize - header_size_len);

        let mut tags = Vec::new();
        let mut header = header;
        while !header.is_empty() {
            let (rest, tag) = parse_varint(header)?;
            header = rest;
            tags.push(tag);
        }

        let mut data = data;
        for tag in tags {
            let (rest, cell) = TableRowCell::parse(tag, data)?;
            data = rest;
            cells.push(cell);
        }

        Ok((data, TableRow { rowid, cells }))
    }

    pub fn cells(&self) -> impl Iterator<Item = &TableRowCell> {
        self.cells.iter()
    }

    pub fn rowid(&self) -> i64 {
        self.rowid
    }
}

pub struct TableIterator<'a, 'b> {
    table: &'a Table<'b>,
    current_cell: usize,
}

impl TableIterator<'_, '_> {
    pub fn new<'a, 'b>(table: &'a Table<'b>) -> TableIterator<'a, 'b> {
        TableIterator {
            table: table,
            current_cell: 0,
        }
    }

    fn parse_row<'a>(&self, cell: &'a CellData) -> Result<'a, TableRow> {
        let data = cell.read(self.table.database)?;
        TableRow::from_cell_data(&data)
            .map(|d| d.1)
            .map_err(|e| e.to_owned())
    }
}

impl<'a, 'b> Iterator for TableIterator<'a, 'b> {
    type Item = TableRow;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.table.root_page {
            ParsedBTreePage::TableLeaf(data) => {
                if self.current_cell < data.cell_count() {
                    let cell = data.read_cell(self.current_cell).ok()?.1;
                    self.current_cell += 1;
                    match self.parse_row(&cell) {
                        Ok(row) => Some(row),
                        Err(err) => {
                            eprintln!("Error while parsing row: {:?}", err);
                            None
                        }
                    }
                } else {
                    None
                }
            }
            ParsedBTreePage::TableInterior => todo!(),
            _ => None,
        }
    }
}

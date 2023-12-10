use crate::{
    error::{Context, ParseResult, Result},
    sql::{data::StmtCreateTable, parser},
};
use nom::bytes::complete as bytes;

use super::{
    index::Index,
    page::btree::{LeafCellData, ParsedBTreePage},
    parse::{parse_int, parse_varint, Parse, ParseWithBlockOffset},
    schema::SchemaItem,
    Database,
};

#[derive(Debug)]
pub struct Table<'a> {
    database: &'a Database,
    root_page: ParsedBTreePage<'a>,
    sql: StmtCreateTable<'static>,
    indexes: Vec<Index<'a>>,
}

impl<'a> Table<'a> {
    pub(crate) fn schema_table(database: &'a Database) -> Result<'a, Table<'a>> {
        Self::new(database, SchemaItem::for_schema_table(), Vec::new())
    }

    pub(crate) fn new(
        database: &'a Database,
        schema: SchemaItem,
        indexes: Vec<SchemaItem>,
    ) -> Result<'a, Table<'a>> {
        let root_page = database
            .parse_btree_page(schema.root_page())
            .add_context("table", schema.table_name().to_string())?;

        let indexes = indexes
            .into_iter()
            .map(|schema| Index::new(database, schema))
            .collect::<Result<'_, Vec<_>>>()
            .add_context("table", schema.table_name().to_string())?;

        Ok(Table {
            database,
            root_page,
            sql: parser::stmt_create_table(schema.sql())
                .add_context("SQL", schema.sql().to_string())
                .add_context("table", schema.name().to_string())?
                .to_owned(),
            indexes,
        })
    }

    pub fn get_row(&self, rowid: i64) -> Option<TableRow> {
        todo!()
    }

    pub fn get_index(&self, column_name: &str) -> Option<Index<'a>> {
        self.indexes
            .iter()
            .find(|i| i.column_name() == column_name)
            .map(|i| i.clone())
    }

    pub fn iter<'b>(&'b self) -> TableIterator<'a, 'b> {
        TableIterator::new(self)
    }

    pub fn sql(&self) -> &StmtCreateTable<'a> {
        &self.sql
    }
}

#[derive(Debug, Clone, PartialEq)]
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

    pub fn is_truthy(&self) -> bool {
        match self {
            TableRowCell::Null => false,
            TableRowCell::Integer(v) => *v != 0,
            TableRowCell::Float(_) => todo!(),
            TableRowCell::Blob(_) => todo!(),
            TableRowCell::String(_) => todo!(),
        }
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

#[derive(Debug)]
pub struct TableIterator<'a, 'b> {
    table: &'a Table<'b>,
    current_cell: Vec<(ParsedBTreePage<'b>, usize)>,
}

#[derive(Debug, Clone)]
enum PageItem<'a> {
    NextPage(u32),
    Cell(LeafCellData<'a>),
    EndOfPage,
}

impl<'db> TableIterator<'_, 'db> {
    pub fn new<'a>(table: &'a Table<'db>) -> TableIterator<'a, 'db> {
        TableIterator {
            table: table,
            current_cell: Vec::from([(table.root_page.clone(), 0)]),
        }
    }

    fn parse_row<'a>(&self, cell: LeafCellData<'a>) -> Result<'a, TableRow> {
        let data = cell.read(self.table.database).map_err(|e| e.to_owned())?;
        TableRow::from_cell_data(&data)
            .map(|d| d.1)
            .map_err(|e| e.to_owned())
    }

    fn read_page_item<'a, 'b>(
        page: &'a ParsedBTreePage<'b>,
        index: usize,
    ) -> Result<'a, PageItem<'b>> {
        match page {
            ParsedBTreePage::TableLeaf(data) => {
                if index < data.cell_count() {
                    let cell = data.read_cell(index).map_err(|e| e.to_owned())?.1;
                    Ok(PageItem::Cell(cell))
                } else {
                    Ok(PageItem::EndOfPage)
                }
            }
            ParsedBTreePage::TableInterior(data) => {
                if index < data.cell_count() {
                    let cell = data.read_cell(index)?.1;
                    Ok(PageItem::NextPage(cell.left_page()))
                } else if index == data.cell_count() {
                    Ok(PageItem::NextPage(data.right_most_page()))
                } else {
                    Ok(PageItem::EndOfPage)
                }
            }
            _ => panic!("Unexpected page type {page:?}"),
        }
    }

    fn resolve_current_item(&mut self) -> Result<'db, Option<LeafCellData<'db>>> {
        if self.current_cell.len() == 0 {
            return Ok(None);
        }

        loop {
            let (innermost_page, index) = self
                .current_cell
                .last()
                .expect("We should alway have at least one page");
            let innermost_item =
                Self::read_page_item(innermost_page, *index).map_err(|e| e.to_owned())?;
            let current_cell = &mut self.current_cell;

            match innermost_item {
                PageItem::NextPage(page) => {
                    let page = self.table.database.read_btree_page(page)?;
                    let page = ParsedBTreePage::parse_in_block(
                        unsafe { std::mem::transmute(page.data()) },
                        self.table.database.header().usable_page_size(),
                        page.offset_from_start(),
                    )
                    .map_err(|e| e.to_owned())?
                    .1;
                    current_cell.push((page, 0));
                }
                PageItem::Cell(data) => return Ok(Some(data)),
                PageItem::EndOfPage => {
                    current_cell.pop();
                    if let Some((_page, index)) = self.current_cell.last_mut() {
                        *index += 1;
                    } else {
                        return Ok(None);
                    }
                }
            }
        }
    }
}

impl<'a, 'b> Iterator for TableIterator<'a, 'b> {
    type Item = TableRow;

    fn next(&mut self) -> Option<Self::Item> {
        match self.resolve_current_item() {
            Ok(Some(data)) => match self.parse_row(data.clone()) {
                Ok(row) => {
                    let (_, index) = self
                        .current_cell
                        .last_mut()
                        .expect("We just got item, the current_cell should not be empty");
                    *index += 1;

                    Some(row)
                }
                Err(e) => {
                    eprintln!("Error while parsing row {e:?}, row: {data:?}");
                    None
                }
            },
            Ok(None) => None,
            Err(e) => {
                eprintln!("Error while resolving item: {:?}, self: {:?}", e, self);
                None
            }
        }
    }
}

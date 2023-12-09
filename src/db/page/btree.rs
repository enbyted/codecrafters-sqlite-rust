use std::borrow::Cow;

use crate::{
    db::{
        parse::{parse_varint, Parse, ParseWithBlockOffset},
        Database,
    },
    error::{DbError, ParseResult, Result},
};
use nom::bytes::complete as bytes;

#[derive(Debug, Clone)]
pub(crate) struct TableLeafData<'a> {
    cells: &'a [u16],
    content_area: &'a [u8],
    offset_to_start_of_content: usize,
    usable_page_size: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct LeafCellData<'a> {
    data_in_leaf: &'a [u8],
    total_data_length: usize,
    overflow_page: Option<u32>,
}

impl LeafCellData<'_> {
    pub(crate) fn read(&self, db: &Database) -> Result<Cow<[u8]>> {
        if let Some(overflow_page) = self.overflow_page {
            let mut data = db.read_overflow_data(overflow_page)?;
            data.extend_from_slice(self.data_in_leaf);
            assert_eq!(data.len(), self.total_data_length);
            data.rotate_right(self.total_data_length - self.data_in_leaf.len());

            Ok(Cow::Owned(data))
        } else {
            Ok(Cow::Borrowed(self.data_in_leaf))
        }
    }
}

impl<'a> TableLeafData<'a> {
    pub(crate) fn read_cell(&self, index: usize) -> ParseResult<LeafCellData<'a>> {
        let offset = u16::from_be(self.cells[index]) as usize;
        assert!(offset >= self.offset_to_start_of_content);
        let offset = offset - self.offset_to_start_of_content;

        let cell_data = &self.content_area[offset..];
        let (cell_data, length) = parse_varint(cell_data)?;
        let length = length as usize;

        // Variable names and calculations copied from sqlite spec
        let u = self.usable_page_size;
        let x = u - 35;
        let p = length;
        let bytes_stored_on_page = if p <= x {
            p
        } else {
            let m = ((u - 12) * 32 / 255) - 23;
            let k = m + ((p - m) % (u - 4));
            if k <= x {
                k
            } else {
                m
            }
        };

        let (data_after_rowid, _rowid) = parse_varint(cell_data)?;
        let rowid_len = data_after_rowid.as_ptr() as usize - cell_data.as_ptr() as usize;
        let bytes_stored_on_page = bytes_stored_on_page + rowid_len;
        let length = length + rowid_len;

        let cell_data = &cell_data[..bytes_stored_on_page];
        let (cell_data, overflow_page) = if bytes_stored_on_page < length {
            let (cell_data, overflow_page_bytes) = cell_data.split_at(bytes_stored_on_page - 4);
            let (_, overflow_page) = u32::parse(overflow_page_bytes)?;
            (cell_data, Some(overflow_page))
        } else {
            (cell_data, None)
        };

        Ok((
            &[],
            LeafCellData {
                data_in_leaf: cell_data,
                total_data_length: length,
                overflow_page,
            },
        ))
    }

    pub(crate) fn cell_count(&self) -> usize {
        self.cells.len()
    }
}

impl<'a> ParseWithBlockOffset<'a> for TableLeafData<'a> {
    fn parse_in_block(
        data: &'a [u8],
        usable_page_size: usize,
        offset_from_block_start: usize,
    ) -> ParseResult<'a, TableLeafData<'a>> {
        let input_data = data;
        let (data, _first_freeblock) = u16::parse(data)?;
        let (data, number_of_cells) = u16::parse(data)?;
        let (data, offset_to_start_of_content) = u16::parse(data)?;
        let (data, _fragmented_free_bytes_count) = u8::parse(data)?;
        let (_, cell_data) = bytes::take(number_of_cells as usize * 2)(data)?;
        let offset_to_start_of_content = if offset_to_start_of_content == 0 {
            65536
        } else {
            offset_to_start_of_content as usize
        } - offset_from_block_start;

        let content_area = &input_data[offset_to_start_of_content..];
        let offset_to_start_of_content = offset_from_block_start
            + (content_area.as_ptr() as usize - input_data.as_ptr() as usize);

        // Safety:
        //  1. The cell data is guaranteed to be properly aligned within block
        //  2. We are transmuting primitive types, i.e. all bit patterns of 2-byte range are valid u16
        //  3. The reads of these u16s will happen via u16::from_be() to guarantee endianness independence
        let (begin, cells, end) = unsafe { cell_data.align_to::<u16>() };
        // cell_data must have been properly aligned, unless we started with a not-aligned block, which is invalid
        assert!(begin.is_empty());
        assert!(end.is_empty());

        Ok((
            &[],
            TableLeafData {
                cells,
                content_area,
                usable_page_size,
                offset_to_start_of_content,
            },
        ))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TableInteriorData<'a> {
    cells: &'a [u16],
    content_area: &'a [u8],
    offset_to_start_of_content: usize,
    right_most_page: u32,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TableInteriorCellData {
    left_page: u32,
    rowid: i64,
}

impl TableInteriorCellData {
    pub(crate) fn left_page(&self) -> u32 {
        self.left_page
    }

    pub(crate) fn rowid(&self) -> i64 {
        self.rowid
    }
}

impl TableInteriorData<'_> {
    pub(crate) fn read_cell(&self, index: usize) -> ParseResult<TableInteriorCellData> {
        let offset = u16::from_be(self.cells[index]) as usize;
        assert!(offset >= self.offset_to_start_of_content);
        let offset = offset - self.offset_to_start_of_content;

        let cell_data = &self.content_area[offset..];
        let (cell_data, left_page) = u32::parse(cell_data)?;
        let (cell_data, rowid) = parse_varint(cell_data)?;

        Ok((cell_data, TableInteriorCellData { left_page, rowid }))
    }

    pub(crate) fn cell_count(&self) -> usize {
        self.cells.len()
    }

    pub(crate) fn right_most_page(&self) -> u32 {
        self.right_most_page
    }
}

impl<'a> ParseWithBlockOffset<'a> for TableInteriorData<'a> {
    fn parse_in_block(
        data: &'a [u8],
        _usable_page_size: usize,
        offset_from_block_start: usize,
    ) -> ParseResult<'a, TableInteriorData<'a>> {
        let input_data = data;
        let (data, _first_freeblock) = u16::parse(data)?;
        let (data, number_of_cells) = u16::parse(data)?;
        let (data, offset_to_start_of_content) = u16::parse(data)?;
        let (data, _fragmented_free_bytes_count) = u8::parse(data)?;
        let (data, right_most_page) = u32::parse(data)?;
        let (_, cell_data) = bytes::take(number_of_cells as usize * 2)(data)?;
        let offset_to_start_of_content = if offset_to_start_of_content == 0 {
            65536
        } else {
            offset_to_start_of_content as usize
        } - offset_from_block_start;

        let content_area = &input_data[offset_to_start_of_content..];
        let offset_to_start_of_content = offset_from_block_start
            + (content_area.as_ptr() as usize - input_data.as_ptr() as usize);

        // Safety:
        //  1. The cell data is guaranteed to be properly aligned within block
        //  2. We are transmuting primitive types, i.e. all bit patterns of 2-byte range are valid u16
        //  3. The reads of these u16s will happen via u16::from_be() to guarantee endianness independence
        let (begin, cells, end) = unsafe { cell_data.align_to::<u16>() };
        // cell_data must have been properly aligned, unless we started with a not-aligned block, which is invalid
        assert!(begin.is_empty());
        assert!(end.is_empty());

        Ok((
            &[],
            TableInteriorData {
                cells,
                content_area,
                offset_to_start_of_content,
                right_most_page,
            },
        ))
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ParsedBTreePage<'a> {
    TableLeaf(TableLeafData<'a>),
    TableInterior(TableInteriorData<'a>),
    IndexLeaf,
    IndexInterior,
}

impl<'a> ParseWithBlockOffset<'a> for ParsedBTreePage<'a> {
    fn parse_in_block(
        data: &'a [u8],
        page_size: usize,
        offset_from_block_start: usize,
    ) -> ParseResult<Self> {
        let (data, type_value) = u8::parse(data)?;
        let ret = match type_value {
            2 => Ok((data, ParsedBTreePage::IndexInterior)),
            10 => Ok((data, ParsedBTreePage::IndexLeaf)),
            5 => {
                let (rest, data) = TableInteriorData::parse_in_block(
                    data,
                    page_size,
                    offset_from_block_start + 1,
                )?;
                Ok((rest, ParsedBTreePage::TableInterior(data)))
            }
            13 => {
                let (rest, data) =
                    TableLeafData::parse_in_block(data, page_size, offset_from_block_start + 1)?;
                Ok((rest, ParsedBTreePage::TableLeaf(data)))
            }
            _ => Err(DbError::InvalidEnum("page type", type_value as u32)),
        }?;
        Ok(ret)
    }
}

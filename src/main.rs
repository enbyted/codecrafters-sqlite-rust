use anyhow::{bail, Context};
use nom::bytes::complete as bytes;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::{Arc, PoisonError, RwLock};
use thiserror::Error;

#[derive(Error, Debug)]
enum DbError<'a> {
    #[error("parsing failed")]
    ParseError(nom::Err<nom::error::Error<&'a [u8]>>),
    #[error("parsing failed")]
    OwnedParseError(nom::Err<nom::error::Error<Vec<u8>>>),
    #[error("invalid enum value for {0} (got {1})")]
    InvalidEnum(&'static str, u32),
    #[error("Invalid varint of length {0}, with last byte {1}")]
    InvalidVarint(u32, u8),
    #[error("invalid argument value for {0} (got {1})")]
    InvalidArgument(&'static str, String),
    #[error("filesystem i/o error")]
    IoError {
        #[from]
        source: std::io::Error,
    },
    #[error("failed to get lock")]
    PoisonError(String),
    #[error("failed to parse string")]
    Utf8Error {
        #[from]
        source: std::str::Utf8Error,
    },
}

impl DbError<'_> {
    pub fn to_owned<'a>(self) -> DbError<'static> {
        match self {
            DbError::ParseError(nom::Err::Error(err)) => DbError::OwnedParseError(nom::Err::Error(
                nom::error::Error::new(Vec::from(err.input), err.code),
            )),
            DbError::ParseError(nom::Err::Failure(err)) => DbError::OwnedParseError(
                nom::Err::Failure(nom::error::Error::new(Vec::from(err.input), err.code)),
            ),
            DbError::ParseError(nom::Err::Incomplete(needed)) => {
                DbError::ParseError(nom::Err::Incomplete(needed))
            }
            DbError::OwnedParseError(err) => DbError::OwnedParseError(err),
            DbError::InvalidEnum(msg, value) => DbError::InvalidEnum(msg, value),
            DbError::InvalidVarint(len, last_byte) => DbError::InvalidVarint(len, last_byte),
            DbError::InvalidArgument(msg, value) => DbError::InvalidArgument(msg, value),
            DbError::IoError { source } => DbError::IoError { source },
            DbError::PoisonError(msg) => DbError::PoisonError(msg),
            DbError::Utf8Error { source } => DbError::Utf8Error { source },
        }
    }
}

impl<G> From<PoisonError<G>> for DbError<'_> {
    fn from(value: PoisonError<G>) -> Self {
        DbError::PoisonError(format!("{value:?}"))
    }
}

impl<'a> From<nom::Err<nom::error::Error<&'a [u8]>>> for DbError<'a> {
    fn from(value: nom::Err<nom::error::Error<&'a [u8]>>) -> Self {
        DbError::ParseError(value)
    }
}

impl<'a> Into<nom::Err<DbError<'a>>> for DbError<'a> {
    fn into(self) -> nom::Err<DbError<'a>> {
        match self {
            DbError::ParseError(nom::Err::Error(_)) => nom::Err::Error(self),
            DbError::ParseError(nom::Err::Failure(_)) => nom::Err::Failure(self),
            DbError::ParseError(nom::Err::Incomplete(needed)) => nom::Err::Incomplete(needed),
            _ => nom::Err::Error(self),
        }
    }
}

type Result<'a, O> = std::result::Result<(&'a [u8], O), DbError<'a>>;

trait Parse<'a> {
    fn parse(data: &'a [u8]) -> Result<'a, Self>
    where
        Self: Sized;
}

trait ParseWithBlockOffset<'a> {
    fn parse_in_block(
        data: &'a [u8],
        usable_page_size: usize,
        offset_from_block_start: usize,
    ) -> Result<'a, Self>
    where
        Self: Sized + 'a;
}

impl<'a, T> ParseWithBlockOffset<'a> for T
where
    T: Sized + Parse<'a>,
{
    fn parse_in_block(
        data: &'a [u8],
        _usable_page_size: usize,
        _offset_from_block_start: usize,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        T::parse(data)
    }
}

impl Parse<'_> for u32 {
    fn parse(data: &[u8]) -> Result<Self> {
        let (data, value) = bytes::take(4usize)(data)?;
        let value = u32::from_be_bytes(
            value
                .try_into()
                .expect("We've taken 4 bytes, this should be OK"),
        );
        Ok((data, value))
    }
}

impl Parse<'_> for i32 {
    fn parse(data: &[u8]) -> Result<Self> {
        let (data, value) = bytes::take(4usize)(data)?;
        let value = i32::from_be_bytes(
            value
                .try_into()
                .expect("We've taken 4 bytes, this should be OK"),
        );
        Ok((data, value))
    }
}

impl Parse<'_> for u16 {
    fn parse(data: &[u8]) -> Result<Self> {
        let (data, value) = bytes::take(2usize)(data)?;
        let value = u16::from_be_bytes(
            value
                .try_into()
                .expect("We've taken 2 bytes, this should be OK"),
        );
        Ok((data, value))
    }
}

impl Parse<'_> for u8 {
    fn parse(data: &[u8]) -> Result<Self> {
        let (data, value) = bytes::take(1usize)(data)?;
        let value = u8::from_be_bytes(
            value
                .try_into()
                .expect("We've taken 1 byte, this should be OK"),
        );
        Ok((data, value))
    }
}

impl Parse<'_> for f64 {
    fn parse(data: &[u8]) -> Result<Self> {
        let (data, value) = bytes::take(8usize)(data)?;
        Ok((
            data,
            f64::from_be_bytes(
                value
                    .try_into()
                    .expect("We've taken 8 bytes, this should be OK"),
            ),
        ))
    }
}

fn parse_varint(data: &[u8]) -> Result<i64> {
    let taken = RefCell::new(0);
    let (data, varint_bytes) = bytes::take_while(|b| {
        let mut taken = taken.borrow_mut();
        if *taken < 8 {
            *taken += 1;
            0 != (b & 0x80)
        } else {
            false
        }
    })(data)?;

    let taken = *taken.borrow();

    let mut ret = 0;
    for b in varint_bytes {
        ret <<= 7;
        ret |= (b & 0x7F) as i64;
    }
    let (data, last_byte) = bytes::take(1usize)(data)?;
    let last_byte = last_byte[0];
    let nine_byte_long = taken >= 8;
    if nine_byte_long && 0 != (last_byte & 0x80) {
        return Err(DbError::InvalidVarint(taken + 1, last_byte));
    }
    // The ninth byte of a varint uses all of it's

    ret <<= if nine_byte_long { 8 } else { 7 };
    ret |= last_byte as i64;

    Ok((data, ret))
}

fn parse_int(data: &[u8], byte_len: usize) -> Result<'_, i64> {
    let (data, value_bytes) = bytes::take(byte_len)(data)?;
    // Sign extent
    let fill = if 0 != (data[0] & 0x80) { 0xFF } else { 0x00 };
    let mut buffer = [fill; 8];

    buffer[8 - byte_len..].copy_from_slice(value_bytes);

    Ok((data, i64::from_be_bytes(buffer)))
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum TextEncoding {
    UTF8 = 1,
    UTF16le = 2,
    UTF16be = 3,
}

impl Parse<'_> for TextEncoding {
    fn parse(data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let (data, value) = u32::parse(data)?;
        let value = match value {
            1 => Ok(TextEncoding::UTF8),
            2 => Ok(TextEncoding::UTF16le),
            3 => Ok(TextEncoding::UTF16be),
            _ => Err(DbError::InvalidEnum("TextEncoding", value)),
        }?;
        Ok((data, value))
    }
}

#[derive(Debug, Clone, Copy)]
struct Header {
    /// Determines the page size of the database.
    ///
    /// For SQLite versions 3.7.0.1 (2010-08-04) and earlier, this value is interpreted
    /// as a big-endian integer and must be a power of two between 512 and 32768, inclusive.
    /// Beginning with SQLite version 3.7.1 (2010-08-23), a page size of 65536 bytes is supported.
    /// The value 65536 will not fit in a two-byte integer, so to specify a 65536-byte page size,
    /// the value at offset 16 is 0x00 0x01. This value can be interpreted as a big-endian 1
    /// and thought of as a magic number to represent the 65536 page size.
    page_size: u16,
    /// The file format write version and file format read version are intended to allow for enhancements
    /// of the file format in future versions of SQLite. In current versions of SQLite, both of these values
    /// are 1 for rollback journalling modes and 2 for WAL journalling mode.
    ///
    /// If a version of SQLite coded to the current file format specification encounters a database file
    /// where the read version is 1 or 2 but the write version is greater than 2, then the database file
    /// must be treated as read-only. If a database file with a read version greater than 2 is encountered,
    /// then that database cannot be read or written.
    write_version: u8,
    read_version: u8,
    /// SQLite has the ability to set aside a small number of extra bytes at the end of every page for
    /// use by extensions. These extra bytes are used, for example, by the SQLite Encryption Extension
    /// to store a nonce and/or cryptographic checksum associated with each page. The "reserved space"
    /// size in the 1-byte integer is the number of bytes of space at the end of each page to reserve
    /// for extensions. This value is usually 0. The value can be odd.
    ///
    /// The "usable size" of a database page is the page size specified by field `page_size` in the header
    /// minus the value of `reserved_bytes_per_page`. The usable size of a page might be an odd number.
    /// However, the usable size is not allowed to be less than 480. In other words, if the page size is 512,
    /// then the reserved space size cannot exceed 32.
    reserved_bytes_per_page: u8,
    /// The file change counter is a 4-byte big-endian integer at offset 24 that is incremented whenever
    /// the database file is unlocked after having been modified. When two or more processes are reading
    /// the same database file, each process can detect database changes from other processes by monitoring
    /// the change counter. A process will normally want to flush its database page cache when another process
    /// modified the database, since the cache has become stale. The file change counter facilitates this.
    ///
    /// In WAL mode, changes to the database are detected using the wal-index and so the change counter is
    /// not needed. Hence, the change counter might not be incremented on each transaction in WAL mode.
    change_counter: u32,
    /// The size of the database file in pages. If this in-header datasize size is not valid, then the
    /// database size is computed by looking at the actual size of the database file. Older versions
    /// of SQLite ignored the in-header database size and used the actual file size exclusively.
    /// Newer versions of SQLite use the in-header database size if it is available but fall back to
    /// the actual file size if the in-header database size is not valid.
    ///
    /// The in-header database size is only considered to be valid if it is non-zero and if the 4-byte
    /// change counter at offset 24 exactly matches the 4-byte version-valid-for number at offset 92.
    /// The in-header database size is always valid when the database is only modified using recent
    /// versions of SQLite, versions 3.7.0 (2010-07-21) and later. If a legacy version of SQLite writes
    /// to the database, it will not know to update the in-header database size and so the in-header
    /// database size could be incorrect. But legacy versions of SQLite will also leave the version-valid-for
    /// number at offset 92 unchanged so it will not match the change-counter. Hence, invalid in-header
    /// database sizes can be detected (and ignored) by observing when the change-counter does not match
    /// the version-valid-for number.
    page_count: u32,
    /// Unused pages in the database file are stored on a freelist.
    ///
    /// Stores the page number of the first page of the freelist, or zero if the freelist is empty.
    first_freelist_page: u32,
    /// Stores the total number of pages on the freelist.
    total_freelist_pages: u32,
    /// Incremented whenever the database schema changes.
    ///
    /// A prepared statement is compiled against a specific version of the database schema. When
    /// the database schema changes, the statement must be reprepared. When a prepared statement runs,
    /// it first checks the schema version to ensure the value is the same as when the statement was prepared
    /// and if the schema version has changed, the statement either automatically reprepares and reruns or it
    /// aborts with an SQLITE_SCHEMA error.
    schema_version: u32,
    /// The schema format version is similar to the file format read and write version numbers  except that the
    /// schema format number refers to the high-level SQL formatting rather than the low-level b-tree formatting.
    ///
    /// Four schema format numbers are currently defined:
    ///  - Format 1 is understood by all versions of SQLite back to version 3.0.0 (2004-06-18).
    ///  - Format 2 adds the ability of rows within the same table to have a varying number of columns, in order to
    ///    support the ALTER TABLE ... ADD COLUMN functionality. Support for reading and writing format 2 was added
    ///    in SQLite version 3.1.3 on 2005-02-20.
    ///  - Format 3 adds the ability of extra columns added by ALTER TABLE ... ADD COLUMN to have non-NULL default
    ///    values. This capability was added in SQLite version 3.1.4 on 2005-03-11.
    ///  - Format 4 causes SQLite to respect the DESC keyword on index declarations. (The DESC keyword is ignored
    ///    in indexes for formats 1, 2, and 3.) Format 4 also adds two new boolean record type values
    ///    (serial types 8 and 9). Support for format 4 was added in SQLite 3.3.0 on 2006-01-10.
    ///
    /// New database files created by SQLite use format 4 by default. The legacy_file_format pragma can be used to
    /// cause SQLite to create new database files using format 1. The format version number can be made to default
    /// to 1 instead of 4 by setting SQLITE_DEFAULT_FILE_FORMAT=1 at compile-time.
    schema_format_version: u32,
    /// The suggested cache size in pages for the database file.
    ///
    /// The value is a suggestion only and SQLite is under no obligation to honor it. The absolute value of the integer
    /// is used as the suggested size. The suggested cache size can be set using the default_cache_size pragma.
    suggested_cache_size: i32,
    /// `largest_root_page` and `incremental_vaccum_mode` are used to manage the auto_vacuum and incremental_vacuum modes.
    ///
    /// If the `largest_root_page` is zero then pointer-map (ptrmap) pages are omitted from the database file and neither
    /// auto_vacuum nor incremental_vacuum are supported. Othwrwise, it is the page number of the largest root page in
    /// the database file, the database file will contain ptrmap pages, and the mode must be either auto_vacuum or incremental_vacuum.
    /// In this latter case, the integer at offset 64 is true for incremental_vacuum and false for auto_vacuum. If the integer at offset 52 is zero then the integer at offset 64 must also be zero.
    largest_root_page: u32,
    incremental_vaccum_mode: bool,
    /// The encoding used for all text strings stored in the database.
    text_encoding: TextEncoding,
    /// The user version which is set and queried by the user_version pragma. The user version is not used by SQLite.
    user_version: u32,
    /// The "Application ID" that can be set by the PRAGMA application_id command in order to identify the database
    /// as belonging to or associated with a particular application.
    ///
    /// The application ID is intended for database files used as an application file-format. The application ID can be
    /// used by utilities such as file(1) to determine the specific file type rather than just reporting "SQLite3 Database".
    ///
    /// A list of assigned application IDs can be seen by consulting the magic.txt file in the SQLite source repository.
    application_id: u32,
    /// The the value of the change counter when the version number was stored.
    ///
    /// Indicates which transaction the version number is valid for and is sometimes called the "version-valid-for number".
    version_valid_for: u32,
    /// Stores the SQLITE_VERSION_NUMBER value for the SQLite library that most recently modified the database file.
    sqlite_version: u32,
}

impl Parse<'_> for Header {
    fn parse(data: &[u8]) -> Result<Header> {
        let (data, _header) = bytes::tag(b"SQLite format 3\0")(data)?;
        let (data, page_size) = u16::parse(data)?;
        let (data, write_version) = u8::parse(data)?;
        let (data, read_version) = u8::parse(data)?;
        let (data, reserved_bytes_per_page) = u8::parse(data)?;
        let (data, _maximum_fraction) = bytes::tag(b"\x40")(data)?;
        let (data, _minimum_fraction) = bytes::tag(b"\x20")(data)?;
        let (data, _leaf_fraction) = bytes::tag(b"\x20")(data)?;
        let (data, change_counter) = u32::parse(data)?;
        let (data, page_count) = u32::parse(data)?;
        let (data, first_freelist_page) = u32::parse(data)?;
        let (data, total_freelist_pages) = u32::parse(data)?;
        let (data, schema_version) = u32::parse(data)?;
        let (data, schema_format_version) = u32::parse(data)?;
        let (data, suggested_cache_size) = i32::parse(data)?;
        let (data, largest_root_page) = u32::parse(data)?;
        let (data, text_encoding) = TextEncoding::parse(data)?;
        let (data, user_version) = u32::parse(data)?;
        let (data, incremental_vaccum_mode) = u32::parse(data)?;
        let (data, application_id) = u32::parse(data)?;
        let (data, _reserved) = bytes::tag([0u8; 20])(data)?;
        let (data, version_valid_for) = u32::parse(data)?;
        let (data, sqlite_version) = u32::parse(data)?;

        Ok((
            data,
            Header {
                page_size,
                write_version,
                read_version,
                reserved_bytes_per_page,
                change_counter,
                page_count,
                first_freelist_page,
                total_freelist_pages,
                schema_version,
                schema_format_version,
                suggested_cache_size,
                largest_root_page,
                text_encoding,
                user_version,
                incremental_vaccum_mode: (incremental_vaccum_mode != 0),
                application_id,
                version_valid_for,
                sqlite_version,
            },
        ))
    }
}

impl Header {
    pub fn page_size(&self) -> usize {
        match self.page_size {
            1 => 65536,
            _ => self.page_size as usize,
        }
    }

    pub fn usable_page_size(&self) -> usize {
        self.page_size() - self.reserved_bytes_per_page as usize
    }
}

#[derive(Debug)]
struct Page {
    data: Vec<u8>,
    offset_from_start: usize,
}

impl Page {
    fn data(&self) -> &[u8] {
        &self.data
    }

    fn offset_from_start(&self) -> usize {
        self.offset_from_start
    }
}

#[derive(Debug)]
struct TableLeafData<'a> {
    cells: &'a [u16],
    content_area: &'a [u8],
    offset_to_start_of_content: usize,
    usable_page_size: usize,
}

struct CellData<'a> {
    data_in_leaf: &'a [u8],
    total_data_length: usize,
    overflow_page: Option<u32>,
}

impl TableLeafData<'_> {
    fn read_cell(&self, index: usize) -> Result<CellData<'_>> {
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
            CellData {
                data_in_leaf: cell_data,
                total_data_length: length,
                overflow_page,
            },
        ))
    }

    fn cell_count(&self) -> usize {
        self.cells.len()
    }
}

impl<'a> ParseWithBlockOffset<'a> for TableLeafData<'a> {
    fn parse_in_block(
        data: &'a [u8],
        usable_page_size: usize,
        offset_from_block_start: usize,
    ) -> Result<'a, TableLeafData<'a>> {
        let input_data = data;
        let (data, first_freeblock) = u16::parse(data)?;
        let (data, number_of_cells) = u16::parse(data)?;
        let (data, offset_to_start_of_content) = u16::parse(data)?;
        let (data, fragmented_free_bytes_count) = u8::parse(data)?;
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

#[derive(Debug)]
enum ParsedBTreePage<'a> {
    TableLeaf(TableLeafData<'a>),
    TableInterior,
    IndexLeaf,
    IndexInterior,
}

impl<'a> ParseWithBlockOffset<'a> for ParsedBTreePage<'a> {
    fn parse_in_block(
        data: &'a [u8],
        page_size: usize,
        offset_from_block_start: usize,
    ) -> Result<Self> {
        let (data, type_value) = u8::parse(data)?;
        let ret = match type_value {
            2 => Ok((data, ParsedBTreePage::IndexInterior)),
            5 => Ok((data, ParsedBTreePage::TableInterior)),
            10 => Ok((data, ParsedBTreePage::IndexLeaf)),
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

#[derive(Debug)]
struct Table<'a> {
    database: &'a Database,
    root_page_data: Arc<Page>,
    root_page: ParsedBTreePage<'a>,
    parsed_pages_cache: HashMap<u32, ParsedBTreePage<'a>>,
}

impl<'a> Table<'a> {
    pub fn new(database: &'a Database, root_page_id: u32) -> Result<'a, Table<'a>> {
        let root_page_data = database.read_btree_page(root_page_id)?;
        let (_, root_page) = ParsedBTreePage::parse_in_block(
            unsafe { std::mem::transmute(root_page_data.data()) },
            database.header().usable_page_size(),
            root_page_data.offset_from_start(),
        )
        .map_err(|e| e.to_owned())?;

        Ok((
            &[],
            Table {
                database,
                root_page_data: root_page_data.clone(),
                root_page: root_page as ParsedBTreePage<'a>,
                parsed_pages_cache: HashMap::new(),
            },
        ))
    }

    pub fn iter<'b>(&'b self) -> TableIterator<'a, 'b> {
        TableIterator::new(self)
    }
}

#[derive(Debug)]
enum TableRowCell {
    Null,
    Integer(i64),
    Float(f64),
    Blob(Vec<u8>),
    String(String),
}

impl TableRowCell {
    fn parse<'a>(tag: i64, data: &'a [u8]) -> Result<'a, Self> {
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
struct TableRow {
    rowid: i64,
    cells: Vec<TableRowCell>,
}

impl TableRow {
    fn from_cell_data(data: &[u8]) -> Result<'_, TableRow> {
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
}

struct TableIterator<'a, 'b> {
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

    fn parse_row<'a>(&self, cell: &'a CellData) -> std::result::Result<TableRow, DbError<'a>> {
        if let Some(overflow_page) = cell.overflow_page {
            let mut data = self.table.database.read_overflow_data(overflow_page)?;
            data.extend_from_slice(cell.data_in_leaf);
            assert_eq!(data.len(), cell.total_data_length);
            data.rotate_right(cell.total_data_length - cell.data_in_leaf.len());

            TableRow::from_cell_data(&data)
                .map(|d| d.1)
                .map_err(|e| e.to_owned())
        } else {
            TableRow::from_cell_data(cell.data_in_leaf).map(|d| d.1)
        }
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

#[derive(Debug)]
enum SchemaEntry {
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

impl SchemaEntry {
    fn new(mut row: TableRow) -> SchemaEntry {
        assert_eq!(row.cells.len(), 5);

        let cell = row.cells.pop().unwrap();
        let sql = if let TableRowCell::String(sql) = cell {
            sql
        } else {
            panic!("Invalid column type {:?}", cell)
        };

        let cell = row.cells.pop().unwrap();
        let root_page = if let TableRowCell::Integer(root_page) = cell {
            root_page as u32
        } else {
            panic!("Invalid column type {:?}", cell)
        };

        let cell = row.cells.pop().unwrap();
        let table_name = if let TableRowCell::String(table_name) = cell {
            table_name
        } else {
            panic!("Invalid column type {:?}", cell)
        };

        let cell = row.cells.pop().unwrap();
        let name = if let TableRowCell::String(name) = cell {
            name
        } else {
            panic!("Invalid column type {:?}", cell)
        };

        let cell = row.cells.pop().unwrap();
        let type_text = if let TableRowCell::String(type_text) = cell {
            type_text
        } else {
            panic!("Invalid column type {:?}", cell)
        };

        match type_text.as_str() {
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
        }
    }
}

#[derive(Debug)]
struct Database {
    page_cache: Arc<RwLock<HashMap<u64, Arc<Page>>>>,
    file: Arc<RwLock<File>>,
    header: Header,
}

impl Database {
    pub fn open(path: &PathBuf) -> std::result::Result<Database, DbError<'static>> {
        let mut file = File::open(path)?;
        let mut header_buf = [0u8; 100];
        file.seek(std::io::SeekFrom::Start(0))?;
        file.read_exact(&mut header_buf)?;
        let (_, header) = Header::parse(&header_buf).map_err(|e| e.to_owned())?;

        let mut db = Database {
            page_cache: Arc::new(RwLock::new(HashMap::new())),
            file: Arc::new(RwLock::new(file)),
            header,
        };

        Ok(db)
    }

    pub fn read_schema(&self) -> std::result::Result<Vec<SchemaEntry>, DbError<'static>> {
        let schema_table = Table::new(self, 1).map_err(|e| e.to_owned())?.1;
        Ok(schema_table.iter().map(|r| SchemaEntry::new(r)).collect())
    }

    pub fn read_btree_page(
        &self,
        page_id: u32,
    ) -> std::result::Result<Arc<Page>, DbError<'static>> {
        let mut file = self.file.write()?;
        let mut page_cache = self.page_cache.write()?;

        let page_offset = match page_id {
            0 => Err(DbError::InvalidArgument("page_id", "0".into())),
            _ => Ok((page_id as u64 - 1) * self.header.page_size as u64),
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
            let page = Arc::new(Page {
                data: page_data,
                offset_from_start,
            });
            page_cache.insert(page_offset, page.clone());
            page
        };

        Ok(page)
    }

    pub fn read_overflow_data(
        &self,
        first_overflow_page_id: u32,
    ) -> std::result::Result<Vec<u8>, DbError<'static>> {
        todo!();
    }

    pub fn header(&self) -> &Header {
        &self.header
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

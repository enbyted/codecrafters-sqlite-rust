use anyhow::{bail, Context};
use nom::bytes::complete as bytes;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
enum DbError<'a> {
    #[error("parsing failed")]
    ParseError(nom::Err<nom::error::Error<&'a [u8]>>),
    #[error("parsing failed")]
    OwnedParseError(nom::Err<nom::error::Error<Vec<u8>>>),
    #[error("invalid enum value for {0} (got {1})")]
    InvalidEnum(&'static str, u32),
    #[error("invalid argument value for {0} (got {1})")]
    InvalidArgument(&'static str, String),
    #[error("filesystem i/o error")]
    IoError {
        #[from]
        source: std::io::Error,
    },
}

impl DbError<'_> {
    pub fn to_owned<'a>(self) -> DbError<'static> {
        match self {
            DbError::ParseError(nom::Err::Error(err)) => {
                DbError::OwnedParseError(nom::Err::Error(nom::error::Error::new(Vec::from(err.input), err.code)))
            },
            DbError::ParseError(nom::Err::Failure(err)) => {
                DbError::OwnedParseError(nom::Err::Failure(nom::error::Error::new(Vec::from(err.input), err.code)))
            },
            DbError::ParseError(nom::Err::Incomplete(needed)) => DbError::ParseError(nom::Err::Incomplete(needed)),
            DbError::OwnedParseError(err) => DbError::OwnedParseError(err),
            DbError::InvalidEnum(msg, value) => DbError::InvalidEnum(msg, value),
            DbError::InvalidArgument(msg, value) => DbError::InvalidArgument(msg, value),
            DbError::IoError { source } => DbError::IoError { source }
        }
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

trait Parse {
    fn parse(data: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

impl Parse for u32 {
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

impl Parse for i32 {
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

impl Parse for u16 {
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

impl Parse for u8 {
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

#[derive(Debug, Clone, Eq, PartialEq)]
enum TextEncoding {
    UTF8 = 1,
    UTF16le = 2,
    UTF16be = 3,
}

impl Parse for TextEncoding {
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

#[derive(Debug, Clone)]
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

impl Parse for Header {
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
}

struct Page(Vec<u8>);

struct Database {
    file: File,
    header: Header,
}

impl Database {
    pub fn open(path: &PathBuf) -> std::result::Result<Database, DbError<'static>> {
        let mut file = File::open(path)?;
        let mut header_buf = [0u8; 100];
        file.seek(std::io::SeekFrom::Start(0))?;
        file.read_exact(&mut header_buf)?;
        let (_, header) = Header::parse(&header_buf).map_err(|e| e.to_owned())?;
        Ok( Database { file, header })
    }

    pub fn read_page(&mut self, page_id: u32) -> std::result::Result<Page, DbError<'static>> {
        let page_offset = match page_id {
            0 => Err(DbError::InvalidArgument("page_id", "0".into())),
            _ => Ok((page_id as u64 - 1) * self.header.page_size as u64),
        }?;
        self.file.seek(std::io::SeekFrom::Start(page_offset))?;
        let mut page_data = Vec::new();
        page_data.resize(self.header.page_size(), 0u8);
        self.file.read_exact(&mut page_data)?;
        Ok(Page(page_data))
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
    match command.as_str() {
        ".dbinfo" => {
            let db = Database::open(&PathBuf::from(&args[1])).context("Reading database file")?;

            println!("database page size: {}", db.header().page_size());
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

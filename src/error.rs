use std::sync::PoisonError;

use peg::str::LineCol;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError<'a> {
    #[error("{source}{}", context.iter().cloned().map(|(name, v)| format!("\n - {name}: {v}")).collect::<String>())]
    WithContext {
        source: Box<DbError<'static>>,
        context: Vec<(&'static str, String)>,
    },
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

    #[error("expected column {0} but reached end of columns")]
    MissingColumn(&'static str),
    #[error("expected column of type {expected}, got {got}")]
    InvalidColumnType { expected: &'static str, got: String },

    #[error("requested table `{0}` was not found")]
    TableNotFound(String),
    #[error("requested column `{0}` was not found")]
    ColumnNotFound(String),

    #[error("received too many arguments, expected at most {1}, but got {0}")]
    TooManyArguments(usize, usize),
    #[error("received not enough arguments, expected at least {1}, but got {0}")]
    NotEnoughArguments(usize, usize),
    #[error("unknown function `{0}`")]
    UnknownFunction(String),

    #[error("SQL parse error")]
    SqlParseError {
        #[from]
        source: peg::error::ParseError<LineCol>,
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
            DbError::MissingColumn(column) => DbError::MissingColumn(column),
            DbError::InvalidColumnType { expected, got } => {
                DbError::InvalidColumnType { expected, got }
            }
            DbError::TableNotFound(table) => DbError::TableNotFound(table),
            DbError::ColumnNotFound(col) => DbError::ColumnNotFound(col),
            DbError::TooManyArguments(got, expected) => DbError::TooManyArguments(got, expected),
            DbError::NotEnoughArguments(got, expected) => {
                DbError::NotEnoughArguments(got, expected)
            }
            DbError::UnknownFunction(name) => DbError::UnknownFunction(name),
            DbError::SqlParseError { source } => DbError::SqlParseError { source },
            DbError::WithContext { source, context } => DbError::WithContext { source, context },
        }
    }

    pub fn add_context(self, name: &'static str, context_str: String) -> DbError<'static> {
        match self {
            DbError::WithContext {
                source,
                mut context,
            } => {
                context.push((name, context_str));
                DbError::WithContext { source, context }
            }
            other => DbError::WithContext {
                source: Box::new(other.to_owned()),
                context: [(name, context_str)].into(),
            },
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

pub trait Context {
    type Output;

    fn add_context(self, name: &'static str, context: String) -> Result<'static, Self::Output>;
}

pub type ParseResult<'a, O> = std::result::Result<(&'a [u8], O), DbError<'a>>;
pub type Result<'a, O> = std::result::Result<O, DbError<'a>>;

impl<O> Context for Result<'_, O> {
    type Output = O;

    fn add_context(self, name: &'static str, context: String) -> Result<'static, O> {
        self.map_err(|e| e.add_context(name, context))
    }
}

impl<O> Context for std::result::Result<O, peg::error::ParseError<LineCol>> {
    type Output = O;

    fn add_context(self, name: &'static str, context: String) -> Result<'static, O> {
        match self {
            Ok(o) => Ok(o),
            Err(err) => Err(DbError::from(err).add_context(name, context)),
        }
    }
}

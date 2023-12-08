use nom::bytes::complete as bytes;
use std::cell::RefCell;

use crate::error::{DbError, ParseResult};

pub(crate) trait Parse<'a> {
    fn parse(data: &'a [u8]) -> ParseResult<'a, Self>
    where
        Self: Sized;
}

pub(crate) trait ParseWithBlockOffset<'a> {
    fn parse_in_block(
        data: &'a [u8],
        usable_page_size: usize,
        offset_from_block_start: usize,
    ) -> ParseResult<'a, Self>
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
    ) -> ParseResult<Self>
    where
        Self: Sized,
    {
        T::parse(data)
    }
}

impl Parse<'_> for u32 {
    fn parse(data: &[u8]) -> ParseResult<Self> {
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
    fn parse(data: &[u8]) -> ParseResult<Self> {
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
    fn parse(data: &[u8]) -> ParseResult<Self> {
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
    fn parse(data: &[u8]) -> ParseResult<Self> {
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
    fn parse(data: &[u8]) -> ParseResult<Self> {
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

pub(crate) fn parse_varint(data: &[u8]) -> ParseResult<i64> {
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

pub(crate) fn parse_int(data: &[u8], byte_len: usize) -> ParseResult<'_, i64> {
    let (data, value_bytes) = bytes::take(byte_len)(data)?;
    // Sign extent
    let fill = if 0 != (value_bytes[0] & 0x80) {
        0xFF
    } else {
        0x00
    };
    let mut buffer = [fill; 8];

    buffer[8 - byte_len..].copy_from_slice(value_bytes);

    Ok((data, i64::from_be_bytes(buffer)))
}

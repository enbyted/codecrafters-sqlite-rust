#[derive(Debug)]
pub(crate) struct Page {
    data: Vec<u8>,
    offset_from_start: usize,
}

impl Page {
    pub(crate) fn new(data: Vec<u8>, offset_from_start: usize) -> Page {
        Page {
            data,
            offset_from_start,
        }
    }

    pub(crate) fn data(&self) -> &[u8] {
        &self.data
    }

    pub(crate) fn offset_from_start(&self) -> usize {
        self.offset_from_start
    }
}

pub(crate) mod btree;

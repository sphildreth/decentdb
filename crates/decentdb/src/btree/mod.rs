#![allow(dead_code)]
//! B+Tree storage primitives for table payloads and postings lists.

pub(crate) mod cursor;
pub(crate) mod page;
pub(crate) mod read;
pub(crate) mod table;
pub(crate) mod write;

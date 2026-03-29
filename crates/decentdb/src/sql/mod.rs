//! SQL parsing and normalization entry points.

pub(crate) mod ast;
pub(crate) mod normalize;
pub(crate) mod parser;
pub(crate) mod parser_tests;

#[cfg(test)]
mod ast_more_tests;

#[cfg(test)]
mod normalize_more_tests;

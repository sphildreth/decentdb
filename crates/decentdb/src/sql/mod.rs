//! SQL parsing and normalization entry points.

pub(crate) mod ast;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod normalize;
pub(crate) mod parser;
pub(crate) mod parser_tests;
#[cfg(any(all(target_arch = "wasm32", target_os = "unknown"), test))]
pub(crate) mod wasm_minimal;

#[cfg(test)]
mod ast_tests;

#[cfg(test)]
mod normalize_tests;

#[cfg(test)]
mod ast_more_tests;

#[cfg(test)]
mod normalize_more_tests;

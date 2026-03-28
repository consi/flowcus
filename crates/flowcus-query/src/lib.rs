// Parser crate: heavy pattern matching, string manipulation, numeric parsing.
// Clippy pedantic/nursery is too noisy for parser code.
#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
#![warn(clippy::correctness)]
#![deny(unsafe_code)]

pub mod ast;
pub mod parser;

pub use ast::Query;
pub use parser::parse;

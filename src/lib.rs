//! Rust bindings to the [mtbl](https://github.com/farsightsec/mtbl) library for
//! dealing with SSTables (immutable sorted map files).
//!
//! SSTables (String-String Tables) are basically constant on-disk maps from
//! [u8] to [u8], like those used by
//! [CDB](http://www.corpit.ru/mjt/tinycdb.html) (which also has [Rust
//! bindings](https://github.com/andrew-d/tinycdb-rs)), except using sorted maps
//! instead of hashmaps. SSTables are suitable for storing the output of
//! mapreduces or other batch results for easy lookup later.
//!
//! Version 0.1.0 of this library is a rather literal translation of the mtbl C
//! API. Later versions may change the API to be friendlier and more in the
//! Rust idioms.
//!
//! # Usage
//!
//! ## Creating a database
//!
//! ```
//! // Create a database, using a Sorter instead of a Writer so we can add
//! // keys in arbitrary (non-sorted) order.
//! {
//!   use mtbl::{Sorter,Write};
//!   let mut writer = mtbl::Sorter::create("data.mtbl");
//!   writer.add("key", "value");
//!   // Data is flushed to file when the writer/sorter is destroyed.
//! }
//! ```
//!
//! ## Reading from a database
//!
//! ```
//! use mtbl::{Read,Reader};
//! let reader = mtbl::Reader::open("data.mtbl");
//! // Get one element
//! let val: Option(Vec<u8>) = reader.get("key");
//! assert_eq!(val, Option("value".as_bytes()));
//! // Or iterate over all entries
//! for (key: Vec<u8>, value: Vec<u8>) in &reader {
//!     f(key, value);
//! }
//! ```
//!
//! # More details about MTBL
//!
//! Quoting from the MTBL documentation:
//!
//! > mtbl is not a database library. It does not provide an updateable
//! > key-value data store, but rather exposes primitives for creating,
//! > searching and merging SSTable files. Unlike databases which use the
//! > SSTable data structure internally as part of their data store, management
//! > of SSTable files -- creation, merging, deletion, combining of search
//! > results from multiple SSTables -- is left to the discretion of the mtbl
//! > library user.
//!
//! > mtbl SSTable files consist of a sequence of data blocks containing sorted
//! > key-value pairs, where keys and values are arbitrary byte arrays. Data
//! > blocks are optionally compressed using zlib or the Snappy library. The
//! > data blocks are followed by an index block, allowing for fast searches
//! > over the keyspace.
//!
//! > The basic mtbl interface is the writer, which receives a sequence of
//! > key-value pairs in sorted order with no duplicate keys, and writes them
//! > to data blocks in the SSTable output file. An index containing offsets to
//! > data blocks and the last key in each data block is buffered in memory
//! > until the writer object is closed, at which point the index is written to
//! > the end of the SSTable file. This allows SSTable files to be written in a
//! > single pass with sequential I/O operations only.
//!
//! > Once written, SSTable files can be searched using the mtbl reader
//! > interface. Searches can retrieve key-value pairs based on an exact key
//! > match, a key prefix match, or a key range. Results are retrieved using a
//! > simple iterator interface.
//!
//! > The mtbl library also provides two utility interfaces which facilitate a
//! > sort-and-merge workflow for bulk data loading. The sorter interface
//! > receives arbitrarily ordered key-value pairs and provides them in sorted
//! > order, buffering to disk as needed. The merger interface reads from
//! > multiple SSTables simultaneously and provides the key-value pairs from
//! > the combined inputs in sorted order. Since mtbl does not allow duplicate
//! > keys in an SSTable file, both the sorter and merger interfaces require a
//! > caller-provided merge function which will be called to merge multiple
//! > values for the same key. These interfaces also make use of sequential I/O
//! > operations only.
//!
//! # Why prefer MTBL over CDB or other constant databases?
//!
//! * Storing data in sorted order makes merging files easy.
//! * Compression is built-in (options: [zlib](http://www.zlib.net/) and
//!   [snappy](https://github.com/google/snappy)).
//! * The library code is a little more modern and uses mmapped files to have
//!   a properly immutable (and therefore thread-safe) representation -- it
//!   doesn't go mucking about with file pointers.

#![crate_name = "mtbl"]
#![crate_type = "lib"]
#![warn(missing_docs)]
#![warn(non_upper_case_globals)]
#![warn(unused_qualifications)]

extern crate libc;
extern crate mtbl_sys;

mod fileset;
mod merger;
mod reader;
mod sorter;
mod writer;

pub use fileset::Fileset;
pub use fileset::FilesetOptions;
pub use merger::MergeFn;
pub use merger::Merger;
pub use reader::ReaderOptions;
pub use reader::Read;
pub use reader::Reader;
pub use sorter::SorterOptions;
pub use sorter::Sorter;
pub use writer::WriterOptions;
pub use writer::CompressionType;
pub use writer::Write;
pub use writer::Writer;

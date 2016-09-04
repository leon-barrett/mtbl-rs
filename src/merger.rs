use libc::{c_void, malloc, size_t};
use std;
use std::slice;

use mtbl_sys;
use reader::{Iter, Read, Reader};

/// An MTBL merging function: given a key and two values for that key, create a
/// merged value for that key.
///
/// Note that, according to the [Rust
/// documentation](https://doc.rust-lang.org/book/ffi.html#ffi-and-panics), such
/// functions should not panic, because such functions are called from C code
/// via the FFI (Foreign Function Interface).
pub type MergeFn = Fn(&[u8], &[u8], &[u8]) -> Vec<u8>;

/// An MTBL reader that opens and reads from several MTBL files, merging their
/// contents.
///
/// An MTBL can have only one value for a key, so when the inputs have a key
/// collision (more than one source contains the same key), it uses a
/// [`MergeFn`](type.MergeFn.html) to combine them.
pub struct Merger {
    _sources: Vec<Box<Read>>,
    // NOTE(leon, 2015-12-13): I haven't figured out a better way to pass this
    // function to C than with nested boxes. Help would be appreciated.
    /// The function used to combine values for colliding keys.
    pub merge_fn: Box<Box<MergeFn>>,
    mtbl_merger: *mut mtbl_sys::mtbl_merger,
    mtbl_source: *const mtbl_sys::mtbl_source,
}

pub extern "C" fn _merge_cb_shim(clos: *mut c_void,
                                 key: *const u8,
                                 len_key: size_t,
                                 val0: *const u8,
                                 len_val0: size_t,
                                 val1: *const u8,
                                 len_val1: size_t,
                                 merged_val: *mut *mut u8,
                                 len_merged_val: *mut size_t) {
    unsafe {
        // NOTE(leon, 2015-12-13): I would like to simplify this so I don't have
        // to use quite as many layers of pointers, but I'm not sure how. Help
        // would be appreciated.
        let merge_fn: &mut Box<MergeFn> = &mut *(clos as *mut Box<MergeFn>);
        let merged = merge_fn(slice::from_raw_parts(key, len_key),
                              slice::from_raw_parts(val0, len_val0),
                              slice::from_raw_parts(val1, len_val1));
        // mtbl library expects malloc-allocated memory that it will own and
        // destroy.
        *merged_val = malloc(merged.len()) as *mut u8;
        *len_merged_val = merged.len();
        std::ptr::copy(merged.as_ptr(), *merged_val, merged.len());
    }
}

impl Merger {
    /// A default MTBL merging function that chooses the last (second) value for the colliding key.
    pub fn merge_choose_last_value(_key: &[u8], _val0: &[u8], val1: &[u8]) -> Vec<u8> {
        val1.to_vec()
    }

    /// A simple MTBL merging function that chooses the first value for the colliding key.
    pub fn merge_choose_first_value(_key: &[u8], val0: &[u8], _val1: &[u8]) -> Vec<u8> {
        val0.to_vec()
    }

    /// Create a merger from a collection of other sources. Note that you must provide a merge_fn
    /// to combine values for colliding keys.
    pub fn new<F>(sources: Vec<Reader>, merge_fn: F) -> Merger
        where F: Fn(&[u8], &[u8], &[u8]) -> Vec<u8> + 'static
    {
        let mut merge_fn: Box<Box<MergeFn>> = Box::new(Box::new(merge_fn));
        unsafe {
            let mut opts = mtbl_sys::mtbl_merger_options_init();
            mtbl_sys::mtbl_merger_options_set_merge_func(opts,
                                                         _merge_cb_shim,
                                                         // Wacky casting to get a void pointer for
                                                         // the C lib.
                                                         &mut (*merge_fn) as *mut _ as *mut c_void);
            let mtbl_merger = mtbl_sys::mtbl_merger_init(opts);
            let mut merger = Merger {
                _sources: Vec::new(),
                merge_fn: merge_fn,
                mtbl_merger: mtbl_merger,
                mtbl_source: mtbl_sys::mtbl_merger_source(mtbl_merger),
            };
            mtbl_sys::mtbl_merger_options_destroy(&mut opts);
            for source in sources {
                merger.add_source(source)
            }
            merger
        }
    }

    /// Add an additional source of data to be merged.
    pub fn add_source<T: 'static + Read>(self: &mut Self, source: T) {
        unsafe {
            mtbl_sys::mtbl_merger_add_source(self.mtbl_merger, *source.raw_mtbl_source());
        }
        self._sources.push(Box::new(source));
    }
}

impl Read for Merger {
    fn raw_mtbl_source(&self) -> &*const mtbl_sys::mtbl_source {
        &self.mtbl_source
    }
}

impl<'a> IntoIterator for &'a Merger {
    type Item = (Vec<u8>, Vec<u8>);
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

// Implement IntoIterator for Merger? I'm not sure how to cleanly do the needed lifetime stuff,
// though.

impl Drop for Merger {
    fn drop(&mut self) {
        unsafe {
            mtbl_sys::mtbl_merger_destroy(&mut self.mtbl_merger);
        }
    }
}

/// Merger is thread-safe.
unsafe impl Send for Merger {}

/// Merger is thread-safe.
unsafe impl Sync for Merger {}

#[cfg(test)]
mod tests {
    extern crate tempfile;
    use self::tempfile::NamedTempFile;

    use merger::Merger;
    use reader;
    use reader::Read as iRead;
    use writer;
    use writer::Write as iWrite;

    /// Set up readers with collision for "one".
    fn set_up_readers() -> Vec<reader::Reader> {
        let tempfile_writer1 = NamedTempFile::new().unwrap();
        let tempfile_reader1 = tempfile_writer1.reopen().unwrap();
        {
            let mut writer = writer::Writer::create_from_file(tempfile_writer1).unwrap();
            writer.add("one", "Hello").unwrap();
            writer.add("two", "world").unwrap();
        }
        let tempfile_writer2 = NamedTempFile::new().unwrap();
        let tempfile_reader2 = tempfile_writer2.reopen().unwrap();
        {
            let mut writer = writer::Writer::create_from_file(tempfile_writer2).unwrap();
            writer.add("one", "blue").unwrap();
            writer.add("three", "green").unwrap();
        }
        let reader1 = reader::Reader::open_from_file(&tempfile_reader1).unwrap();
        let reader2 = reader::Reader::open_from_file(&tempfile_reader2).unwrap();
        vec![reader1, reader2]
    }

    #[test]
    fn test_merger() {
        let merger = Merger::new(set_up_readers(),
                                 |_key, _val0, _val1| "wat".as_bytes().to_vec());
        assert_eq!(merger.get("a"), None);
        assert_eq!(merger.get("one").unwrap(), "wat".as_bytes());
        assert_eq!(merger.get("two").unwrap(), "world".as_bytes());
        assert_eq!(merger.get("three").unwrap(), "green".as_bytes());
        for (k, v) in &merger {
            println!("{} {}", k.len(), v.len());
        }
    }

    #[test]
    fn test_merge_choose_last_value() {
        let merger = Merger::new(set_up_readers(), Merger::merge_choose_last_value);
        assert_eq!(merger.get("a"), None);
        assert_eq!(merger.get("one").unwrap(), "blue".as_bytes());
        assert_eq!(merger.get("two").unwrap(), "world".as_bytes());
        assert_eq!(merger.get("three").unwrap(), "green".as_bytes());
    }

    #[test]
    fn test_merge_choose_first_value() {
        let merger = Merger::new(set_up_readers(), Merger::merge_choose_first_value);
        assert_eq!(merger.get("a"), None);
        assert_eq!(merger.get("one").unwrap(), "Hello".as_bytes());
        assert_eq!(merger.get("two").unwrap(), "world".as_bytes());
        assert_eq!(merger.get("three").unwrap(), "green".as_bytes());
    }

}

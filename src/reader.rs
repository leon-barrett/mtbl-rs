use libc::size_t;
use std::fs::File;
use std::io::Result as IOResult;
use std::io::{Error, ErrorKind};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::ptr;
use std::slice;

use mtbl_sys;

/// A trait for objects that can read keys from an MTBL file.
///
/// A Read is accessed like a sorted map, with each key mapping to one value.
/// Keys and values are both byte sequences, passed around as slices or vectors.
/// Because it's a sorted map, you can access not just via an exact key but also
/// by a key prefix or range.
pub trait Read {
    /// Get the internal mtbl_source pointer.
    fn raw_mtbl_source(&self) -> &*const mtbl_sys::mtbl_source;

    /// Get the value of a key, if it's present.
    fn get<T>(&self, key: T) -> Option<Vec<u8>>
        where Self: Sized,
              T: AsRef<[u8]>
    {
        let key = key.as_ref();
        unsafe {
            let mut iter = mtbl_sys::mtbl_source_get(*self.raw_mtbl_source(),
                                                     key.as_ptr(),
                                                     key.len());
            let mut keyptr: *const u8 = ptr::null();
            let mut keylen: size_t = 0;
            let mut valptr: *const u8 = ptr::null();
            let mut vallen: size_t = 0;
            let res = mtbl_sys::mtbl_iter_next(iter,
                                               &mut keyptr,
                                               &mut keylen,
                                               &mut valptr,
                                               &mut vallen);
            let retval = match res {
                mtbl_sys::MtblRes::mtbl_res_success => {
                    Some(slice::from_raw_parts(valptr, vallen).to_vec())
                }
                mtbl_sys::MtblRes::mtbl_res_failure => None,
            };
            mtbl_sys::mtbl_iter_destroy(&mut iter);
            retval
        }
    }

    /// Get an iterator over all keys and values.
    fn iter(&self) -> Iter {
        let source = self.raw_mtbl_source();
        Iter::new(unsafe { mtbl_sys::mtbl_source_iter(*source) }, source)
    }

    /// Get an iterator over all keys and values where the key starts with the given prefix.
    fn get_prefix<T>(&self, prefix: T) -> Iter
        where Self: Sized,
              T: AsRef<[u8]>
    {
        let prefix = prefix.as_ref();
        let source = self.raw_mtbl_source();
        Iter::new(unsafe {
                      mtbl_sys::mtbl_source_get_prefix(*source, prefix.as_ptr(), prefix.len())
                  },
                  source)
    }


    /// Get an iterator over all keys and values, where the keys are between key0 and key1
    /// (inclusive).
    fn get_range<T, U>(&self, key0: T, key1: U) -> Iter
        where Self: Sized,
              T: AsRef<[u8]>,
              U: AsRef<[u8]>
    {
        let key0 = key0.as_ref();
        let key1 = key1.as_ref();
        let source = self.raw_mtbl_source();
        Iter::new(unsafe {
                      mtbl_sys::mtbl_source_get_range(*source,
                                                      key0.as_ptr(),
                                                      key0.len(),
                                                      key1.as_ptr(),
                                                      key1.len())
                  },
                  source)
    }
}

impl<'a> IntoIterator for &'a Read {
    type Item = (Vec<u8>, Vec<u8>);
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

/// An iterator that steps through a section of an MTBL. This is a low-level
/// struct that interacts with the mtbl library directly.
pub struct Iter<'a> {
    mtbl_iter: *mut mtbl_sys::mtbl_iter,
    _source: &'a *const mtbl_sys::mtbl_source,
}

impl<'a> Iter<'a> {
    /// Create an iterator for an mtbl_source.
    pub fn new(mtbl_iter: *mut mtbl_sys::mtbl_iter,
               source: &'a *const mtbl_sys::mtbl_source)
               -> Iter<'a> {
        Iter {
            mtbl_iter: mtbl_iter,
            _source: source,
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    /// A key, value pair.
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let mut keyptr: *const u8 = ptr::null();
            let mut keylen: size_t = 0;
            let mut valptr: *const u8 = ptr::null();
            let mut vallen: size_t = 0;
            let res = mtbl_sys::mtbl_iter_next(self.mtbl_iter,
                                               &mut keyptr,
                                               &mut keylen,
                                               &mut valptr,
                                               &mut vallen);
            match res {
                mtbl_sys::MtblRes::mtbl_res_success => {
                    Some((slice::from_raw_parts(keyptr, keylen).to_vec(),
                          slice::from_raw_parts(valptr, vallen).to_vec()))
                }
                mtbl_sys::MtblRes::mtbl_res_failure => None,
            }
        }
    }
}

impl<'a> Drop for Iter<'a> {
    fn drop(&mut self) {
        unsafe {
            mtbl_sys::mtbl_iter_destroy(&mut self.mtbl_iter);
        }
    }
}

/// MTBL Reader opening options.
#[derive(Clone,Copy)]
pub struct ReaderOptions {
    // in mtbl v0.8.0
    // pub madvise_random: Option<bool>,
    /// Whether or not the CRC32C checksum on each data block should be verified
    /// or not. If verify_checksums is enabled, a checksum mismatch will cause a
    /// runtime error. The mtbl default is false.
    pub verify_checksums: Option<bool>,
}

impl ReaderOptions {
    /// Create a ReaderOptions containing only defaults.
    pub fn new() -> ReaderOptions {
        ReaderOptions { verify_checksums: None }
    }

    /// Create a new options with verify_checksums set.
    pub fn verify_checksums(self: &Self, verify_checksums: bool) -> ReaderOptions {
        ReaderOptions { verify_checksums: Some(verify_checksums), ..*self }
    }

    /// Open an MTBL reader with these options from a file described by the
    /// given path.
    pub fn open_from_path<T: AsRef<Path>>(self: &Self, path: T) -> IOResult<Reader> {
        File::open(path).and_then(|f| self.open_from_file(&f))
    }

    /// Open an MTBL reader with these options from a file object.
    pub fn open_from_file<T: 'static + AsRawFd>(self: &Self, file: &T) -> IOResult<Reader> {
        let fd = file.as_raw_fd();
        unsafe {
            let mut mtbl_options = mtbl_sys::mtbl_reader_options_init();
            if let Some(verify_checksums) = self.verify_checksums {
                mtbl_sys::mtbl_reader_options_set_verify_checksums(mtbl_options, verify_checksums);
            }
            let mtbl_reader = mtbl_sys::mtbl_reader_init_fd(fd, mtbl_options);
            mtbl_sys::mtbl_reader_options_destroy(&mut mtbl_options);
            if mtbl_reader.is_null() {
                Err(Error::new(ErrorKind::Other, "failed to open MTBL file"))
            } else {
                Ok(Reader {
                    options: *self,
                    mtbl_reader: mtbl_reader,
                    mtbl_source: mtbl_sys::mtbl_reader_source(mtbl_reader),
                })
            }
        }
    }
}

/// A reader for a single MTBL file.
///
/// Reader uses a memory-mapped file and is immutable and entirely thread-safe.
///
/// To create a Reader with options other than the default, use
/// [ReaderOptions](struct.ReaderOptions.html).
pub struct Reader {
    /// The options used to open this MTBL file.
    pub options: ReaderOptions,
    mtbl_reader: *mut mtbl_sys::mtbl_reader,
    mtbl_source: *const mtbl_sys::mtbl_source,
}

impl Reader {
    /// Open an MTBL reader from a file described by the given path.
    pub fn open_from_path<T: AsRef<Path>>(path: T) -> IOResult<Reader> {
        ReaderOptions::new().open_from_path(path)
    }

    /// Open an MTBL reader from a file object.
    pub fn open_from_file<T: 'static + AsRawFd>(file: &T) -> IOResult<Reader> {
        ReaderOptions::new().open_from_file(file)
    }
}

impl Read for Reader {
    fn raw_mtbl_source(&self) -> &*const mtbl_sys::mtbl_source {
        &self.mtbl_source
    }
}

impl<'a> IntoIterator for &'a Reader {
    type Item = (Vec<u8>, Vec<u8>);
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

// Implement IntoIterator for Reader? I'm not sure how to cleanly do the needed
// lifetime stuff, though.

impl Drop for Reader {
    fn drop(&mut self) {
        unsafe {
            mtbl_sys::mtbl_reader_destroy(&mut self.mtbl_reader);
        }
    }
}

/// Reader is thread-safe.
unsafe impl Send for Reader {}

/// Reader is thread-safe.
unsafe impl Sync for Reader {}

#[cfg(test)]
mod tests {
    extern crate tempfile;
    use self::tempfile::NamedTempFile;

    use std::sync::Arc;
    use std::thread;

    use reader::{ReaderOptions, Read, Reader};
    use writer::{Write, Writer};

    // Create a test MTBL file.
    fn create_mtbl(t: NamedTempFile) {
        let mut writer = Writer::create_from_file(t).unwrap();
        writer.add("one", "Hello").unwrap();
        writer.add("two", "world").unwrap();
    }

    #[test]
    fn test_lookup() {
        let tempfile_writer = NamedTempFile::new().unwrap();
        let tempfile_reader = tempfile_writer.reopen().unwrap();
        create_mtbl(tempfile_writer);
        let reader = Reader::open_from_file(&tempfile_reader).unwrap();
        assert_eq!(reader.get("one"), Some("Hello".as_bytes().to_vec()));
        assert_eq!(reader.get("two"), Some("world".as_bytes().to_vec()));
        assert_eq!(reader.get("three"), None);
    }

    #[test]
    fn test_iterator() {
        let tempfile_writer = NamedTempFile::new().unwrap();
        let tempfile_reader = tempfile_writer.reopen().unwrap();
        create_mtbl(tempfile_writer);
        let reader = Reader::open_from_file(&tempfile_reader).unwrap();
        {
            let mut it = reader.iter();
            assert_eq!(it.next(), Some(("one".as_bytes().to_vec(), "Hello".as_bytes().to_vec())));
            assert_eq!(it.next(), Some(("two".as_bytes().to_vec(), "world".as_bytes().to_vec())));
            assert_eq!(it.next(), None);
            for (k, v) in reader.iter() {
                assert_eq!(k.len(), 3);
                assert_eq!(v.len(), 5);
            }
        }
        {
            let mut it = reader.get_prefix("o");
            assert_eq!(it.next(), Some(("one".as_bytes().to_vec(), "Hello".as_bytes().to_vec())));
            assert_eq!(it.next(), None);
        }
        {
            let mut it = reader.get_range("to", "vo");
            assert_eq!(it.next(), Some(("two".as_bytes().to_vec(), "world".as_bytes().to_vec())));
            assert_eq!(it.next(), None);
        }
        {
            let mut it = reader.get_range("o", "two");
            assert_eq!(it.next(), Some(("one".as_bytes().to_vec(), "Hello".as_bytes().to_vec())));
            assert_eq!(it.next(), Some(("two".as_bytes().to_vec(), "world".as_bytes().to_vec())));
            assert_eq!(it.next(), None);
        }
        for (k, v) in &reader as &Read {
            println!("{} {}", k.len(), v.len());
        }
        for (k, v) in &reader {
            println!("{} {}", k.len(), v.len());
        }
    }

    #[test]
    fn test_reader_options() {
        let tempfile_writer = NamedTempFile::new().unwrap();
        let tempfile_reader = tempfile_writer.reopen().unwrap();
        create_mtbl(tempfile_writer);
        let reader = ReaderOptions::new()
                         .verify_checksums(true)
                         .open_from_file(&tempfile_reader)
                         .unwrap();
        assert_eq!(reader.options.verify_checksums, Some(true));
        let mut it = reader.iter();
        assert_eq!(it.next(), Some(("one".as_bytes().to_vec(), "Hello".as_bytes().to_vec())));
        assert_eq!(it.next(), Some(("two".as_bytes().to_vec(), "world".as_bytes().to_vec())));
        assert_eq!(it.next(), None);
    }

    #[test]
    fn test_parallel_readers() {
        let tempfile_writer = NamedTempFile::new().unwrap();
        let tempfile_reader = tempfile_writer.reopen().unwrap();
        create_mtbl(tempfile_writer);
        let reader = Arc::new(Reader::open_from_file(&tempfile_reader).unwrap());
        let mut threads = Vec::new();
        for _ in 0..100 {
            let r = reader.clone();
            threads.push(thread::spawn(move || r.get("one")));
        }
        for t in threads {
            assert_eq!(t.join().unwrap(), Some("Hello".as_bytes().to_vec()));
        }
    }
}

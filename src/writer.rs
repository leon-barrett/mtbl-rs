use std::fs::File;
use std::io::Result as IOResult;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::os::unix::io::AsRawFd;

use mtbl_sys;

pub use mtbl_sys::CompressionType;

/// A trait for objects that can write an MTBL file.
pub trait Write {
    /// Add a key/value pair to the MTBL file.
    fn add<T, U>(&mut self, key: T, value: U) -> Result<(), ()>
        where T: AsRef<[u8]>,
              U: AsRef<[u8]>;
}

/// Options for writing an MTBL file.
///
/// # Examples
///
/// ```
/// let writer = WriterOptions::new()
///                  .compression(CompressionType::MTBL_COMPRESSION_SNAPPY)
///                  .create_from_path("/tmp/f.mtbl");
/// ```
#[derive(Clone,Copy)]
pub struct WriterOptions {
    /// What compression type to use. Default is to use zlib.
    pub compression: Option<CompressionType>,
    /// What block size to use, in bytes. Default is 8 KiB.
    pub block_size: Option<usize>,
    /// How often, in keys, to restart intra-block key prefix compression. Default is every 16
    /// keys.
    pub block_restart_interval: Option<usize>,
}

impl WriterOptions {
    /// Create new options with only defaults.
    pub fn new() -> WriterOptions {
        WriterOptions {
            compression: None,
            block_size: None,
            block_restart_interval: None,
        }
    }

    /// Create a new options with compression type set.
    pub fn compression(self: &Self, compression: CompressionType) -> WriterOptions {
        WriterOptions { compression: Some(compression), ..*self }
    }

    /// Create a new options with block size set.
    pub fn block_size(self: &Self, block_size: usize) -> WriterOptions {
        WriterOptions { block_size: Some(block_size), ..*self }
    }

    /// Create a new options with block restart interval set.
    pub fn block_restart_interval(self: &Self, block_restart_interval: usize) -> WriterOptions {
        WriterOptions { block_restart_interval: Some(block_restart_interval), ..*self }
    }

    /// Create a new `Writer` using these options, at a given path.
    pub fn create_from_path<T: AsRef<Path>>(self: &Self, path: T) -> IOResult<Writer> {
        File::create(path).and_then(|f| self.create_from_file(f))
    }

    /// Create a new `Writer` using these options, with a given `File`.
    pub fn create_from_file<T: 'static + AsRawFd>(self: &Self, file: T) -> IOResult<Writer> {
        let fd = file.as_raw_fd();
        let fdbox = Box::new(file);
        unsafe {
            let mut mtbl_options = mtbl_sys::mtbl_writer_options_init();
            if let Some(compression) = self.compression {
                mtbl_sys::mtbl_writer_options_set_compression(mtbl_options, compression);
            }
            if let Some(block_size) = self.block_size {
                mtbl_sys::mtbl_writer_options_set_block_size(mtbl_options, block_size);
            }
            if let Some(block_restart_interval) = self.block_restart_interval {
                mtbl_sys::mtbl_writer_options_set_block_restart_interval(mtbl_options,
                                                                         block_restart_interval);
            }
            let mtbl_writer = mtbl_sys::mtbl_writer_init_fd(fd, mtbl_options);
            mtbl_sys::mtbl_writer_options_destroy(&mut mtbl_options);
            if mtbl_writer.is_null() {
                Err(Error::new(ErrorKind::Other, "failed to open MTBL file"))
            } else {
                Ok(Writer {
                    options: *self,
                    mtbl_writer: mtbl_writer,
                    _file: fdbox,
                })
            }
        }
    }
}

/// A struct to create an MTBL file from keys and values in sorted order.
///
/// Note that keys must be added in sorted order (by key), since they are
/// written to disk. To create an MTBL providing keys in non-sorted order, use a
/// [`Sorter`](struct.Sorter.html).
///
/// To create a Writer with non-default options, see
/// [WriterOptions](struct.WriterOptions.html).
///
/// # Examples
///
/// ```
/// let mut writer = Writer::create_from_path("/tmp/f.mtbl");
/// writer.add("a", dat_a);
/// writer.add("b", dat_b);
/// ```
pub struct Writer {
    /// The options used to create this MTBL file.
    pub options: WriterOptions,
    mtbl_writer: *mut mtbl_sys::mtbl_writer,
    _file: Box<AsRawFd>,
}

impl Writer {
    /// Create an empty MTBL file at the given path.
    pub fn create_from_path<T: AsRef<Path>>(path: T) -> IOResult<Writer> {
        WriterOptions::new().create_from_path(path)
    }

    /// Create an empty MTBL file from the given `File`.
    pub fn create_from_file<T: 'static + AsRawFd>(file: T) -> IOResult<Writer> {
        WriterOptions::new().create_from_file(file)
    }

    /// Get the underlying mtbl_writer pointer.
    pub fn as_raw_ptr(&mut self) -> *mut mtbl_sys::mtbl_writer {
        self.mtbl_writer
    }

    /// Add all elements from a sorted iterator.
    ///
    /// If the inputs are not all sorted (and after all the elements already
    /// added to the Writer), the result will be an Err, and that element and
    /// all further elements will not be written to the MTBL file.
    pub fn add_all_sorted<T, U, I>(&mut self, iterable: I) -> Result<(), ()>
        where T: AsRef<[u8]>,
              U: AsRef<[u8]>,
              I: IntoIterator<Item = (T, U)>
    {
        for (k, v) in iterable {
            try!(self.add(k.as_ref(), v.as_ref()));
        }
        Ok(())
    }
}

impl Write for Writer {
    /// Add a key-value pair to be written to the MTBL file.
    ///
    /// Keys must be provided in sorted order. If keys are not provided in
    /// sorted order, this will result in an Err.
    fn add<T, U>(&mut self, key: T, value: U) -> Result<(), ()>
        where T: AsRef<[u8]>,
              U: AsRef<[u8]>
    {
        let key = key.as_ref();
        let value = value.as_ref();
        unsafe {
            let res = mtbl_sys::mtbl_writer_add(self.mtbl_writer,
                                                key.as_ptr(),
                                                key.len(),
                                                value.as_ptr(),
                                                value.len());
            match res {
                mtbl_sys::MtblRes::mtbl_res_failure => Err(()),
                mtbl_sys::MtblRes::mtbl_res_success => Ok(()),
            }
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        unsafe {
            mtbl_sys::mtbl_writer_destroy(&mut self.mtbl_writer);
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate tempfile;
    use self::tempfile::NamedTempFile;

    use reader::{Read, Reader};
    use writer::{CompressionType, WriterOptions, Write, Writer};

    #[test]
    fn test_reader_writer_file() {
        let tempfile_writer = NamedTempFile::new().unwrap();
        let tempfile_reader = tempfile_writer.reopen().unwrap();
        {
            let mut writer = Writer::create_from_file(tempfile_writer).unwrap();
            writer.add("one", "Hello").unwrap();
            writer.add("two", "world").unwrap();
        }
        let reader = Reader::open_from_file(&tempfile_reader).unwrap();
        assert_eq!(reader.get("one").unwrap(), "Hello".as_bytes());
        assert_eq!(reader.get("two").unwrap(), "world".as_bytes());
    }

    #[test]
    fn test_reader_writer_path() {
        let tmpfile = NamedTempFile::new().unwrap();
        {
            let mut writer = Writer::create_from_path(tmpfile.path()).unwrap();
            writer.add("one", "Hello").unwrap();
            writer.add("two", "world").unwrap();
        }
        let reader = Reader::open_from_path(tmpfile.path()).unwrap();
        assert_eq!(reader.get("one").unwrap(), "Hello".as_bytes());
        assert_eq!(reader.get("two").unwrap(), "world".as_bytes());
    }

    #[test]
    fn test_options() {
        let opts = WriterOptions::new()
                       .compression(CompressionType::MTBL_COMPRESSION_SNAPPY)
                       .block_size(1000)
                       .block_restart_interval(500);
        assert_eq!(opts.compression,
                   Some(CompressionType::MTBL_COMPRESSION_SNAPPY));
        assert_eq!(opts.block_size, Some(1000));
        assert_eq!(opts.block_restart_interval, Some(500));
        let tmpfile = NamedTempFile::new().unwrap();
        {
            let mut writer = opts.create_from_path(tmpfile.path()).unwrap();
            writer.add("one", "Hello").unwrap();
            writer.add("two", "world").unwrap();
        }
        let reader = Reader::open_from_path(tmpfile.path()).unwrap();
        // TODO verify reader metadata--requires mtbl v0.8.0
        assert_eq!(reader.get("one").unwrap(), "Hello".as_bytes());
        assert_eq!(reader.get("two").unwrap(), "world".as_bytes());
    }

    #[test]
    #[should_panic]
    fn test_out_of_order_panic() {
        let tempfile_writer = NamedTempFile::new().unwrap();
        {
            let mut writer = Writer::create_from_file(tempfile_writer).unwrap();
            writer.add("two", "world").unwrap();
            writer.add("one", "Hello").unwrap();
        }
    }

    #[test]
    fn test_out_of_order_missing() {
        let tempfile_writer = NamedTempFile::new().unwrap();
        let tempfile_reader = tempfile_writer.reopen().unwrap();
        {
            let mut writer = Writer::create_from_file(tempfile_writer).unwrap();
            writer.add("two", "world").unwrap();
            assert_eq!(Err(()), writer.add("one", "Hello"));
        }
        let reader = Reader::open_from_file(&tempfile_reader).unwrap();
        assert_eq!(reader.get("one"), None);
        assert_eq!(reader.get("two").unwrap(), "world".as_bytes());
    }

    #[test]
    fn test_add_all_sorted() {
        let tempfile_writer = NamedTempFile::new().unwrap();
        let tempfile_reader = tempfile_writer.reopen().unwrap();
        {
            let mut writer = Writer::create_from_file(tempfile_writer).unwrap();
            writer.add_all_sorted((0..100).map(|i| (format!("{:08}", i), format!("entry {}", i))))
                  .unwrap();
        }
        let reader = Reader::open_from_file(&tempfile_reader).unwrap();
        for i in 0..100 {
            assert_eq!(format!("entry {}", i).as_bytes().to_vec(),
                       reader.get(format!("{:08}", i)).unwrap());
        }
    }

    #[test]
    #[should_panic]
    fn test_add_all_sorted_out_of_order() {
        let tempfile_writer = NamedTempFile::new().unwrap();
        {
            let mut writer = Writer::create_from_file(tempfile_writer).unwrap();
            writer.add_all_sorted((0..100).map(|i| (format!("{}", i), format!("entry {}", i))))
                  .unwrap();
        }
    }
}

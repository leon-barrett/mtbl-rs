use libc::c_void;
use std::ffi::CString;
use std::io::Result as IOResult;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

use merger;
use mtbl_sys;
use writer::{Write, Writer};

/// Options used to create a `Sorter`.
#[derive(Clone)]
pub struct SorterOptions {
    /// The temporary directory to be used for intermediate files. Default is "/var/tmp".
    pub temp_dir: Option<PathBuf>,
    /// The amount of RAM to use for storing intermediate files, in bytes. Default is 1 GiB.
    pub max_memory: Option<usize>,
}

impl SorterOptions {
    /// Create a new `SorterOptions` with defaults.
    pub fn new() -> SorterOptions {
        SorterOptions {
            temp_dir: None,
            max_memory: None,
        }
    }

    /// Create a new `SorterOptions` with temp_dir set.
    pub fn temp_dir<T: AsRef<Path>>(self: &Self, path: T) -> SorterOptions {
        SorterOptions { temp_dir: Some(path.as_ref().to_path_buf()), ..*self }
    }

    /// Create a new `SorterOptions` with max_memory set.
    pub fn max_memory(self: &Self, max_memory: usize) -> SorterOptions {
        SorterOptions { max_memory: Some(max_memory), ..self.clone() }
    }

    /// Create a new `Sorter` with these options.
    ///
    /// Once sorting is done, the resulting sequence will be written to the supplied `Writer`. Note
    /// that a `MergeFn` must be supplied to combine values for entries with colliding keys.
    pub fn create_from_writer<F>(self: &Self, writer: Writer, merge_fn: F) -> Sorter
        where F: Fn(&[u8], &[u8], &[u8]) -> Vec<u8> + 'static
    {
        let mut merge_fn: Box<Box<merger::MergeFn>> = Box::new(Box::new(merge_fn));
        unsafe {
            let mut opts = mtbl_sys::mtbl_sorter_options_init();
            mtbl_sys::mtbl_sorter_options_set_merge_func(opts,
                                                         merger::_merge_cb_shim,
                                                         // Wacky casting to get a void pointer for
                                                         // the C lib.
                                                         &mut (*merge_fn) as *mut _ as *mut c_void);
            if let Some(ref temp_dir) = self.temp_dir {
                let c_str = CString::new(temp_dir.to_str().unwrap()).unwrap();
                mtbl_sys::mtbl_sorter_options_set_temp_dir(opts, c_str.as_ptr());
            }
            if let Some(max_memory) = self.max_memory {
                mtbl_sys::mtbl_sorter_options_set_max_memory(opts, max_memory);
            }
            let mtbl_sorter = mtbl_sys::mtbl_sorter_init(opts);
            let sorter = Sorter {
                options: self.clone(),
                mtbl_sorter: mtbl_sorter,
                merge_fn: merge_fn,
                writer: writer,
            };
            mtbl_sys::mtbl_sorter_options_destroy(&mut opts);
            sorter
        }
    }

    /// Create a new `Sorter` with these options.
    ///
    /// Once sorting is done, the resulting sequence will be written to the supplied path. Note
    /// that a `MergeFn` must be supplied to combine values for entries with colliding keys.
    pub fn create_from_path<T, F>(self: &Self, path: T, merge_fn: F) -> IOResult<Sorter>
        where T: AsRef<Path>,
              F: Fn(&[u8], &[u8], &[u8]) -> Vec<u8> + 'static
    {
        Writer::create_from_path(path).map(|w| self.create_from_writer(w, merge_fn))
    }

    /// Create a new `Sorter` with these options.
    ///
    /// Once sorting is done, the resulting sequence will be written to the supplied path. Note
    /// that a `MergeFn` must be supplied to combine values for entries with colliding keys.
    pub fn create_from_file<T, F>(self: &Self, file: T, merge_fn: F) -> IOResult<Sorter>
        where T: 'static + AsRawFd,
              F: Fn(&[u8], &[u8], &[u8]) -> Vec<u8> + 'static
    {
        Writer::create_from_file(file).map(|w| self.create_from_writer(w, merge_fn))
    }
}

/// A tool to create an MTBL file out of keys in any order.
///
/// A Sorter will buffer entries in memory, periodically writing them to
/// (sorted) temporary files. When everything has been added, the temporary
/// files will be merged into a single MTBL file.
///
/// To create a Sorter with non-default options, see
/// [SorterOptions](struct.SorterOptions.html).
///
/// # Example
///
/// ```
/// let mut sorter = Sorter::create_from_path("/tmp/f.mtbl",
///                                           |k, v0, v1| "collision".as_bytes().to_vec());
/// sorter.add("b", dat_b);
/// sorter.add("a", dat_a);
/// sorter.add("a", other_dat_a);
/// sorter.add_all((0..100).map(|i| (format!("key {}", i), format!("entry {}", i))));
/// ```
pub struct Sorter {
    /// The options used to create this sorter.
    pub options: SorterOptions,
    mtbl_sorter: *mut mtbl_sys::mtbl_sorter,
    /// The function used to merge entries with colliding keys.
    pub merge_fn: Box<Box<merger::MergeFn>>,
    writer: Writer,
}

impl Sorter {
    /// Create a new `Sorter`.
    ///
    /// Once sorting is done, the resulting sequence of entries will be written
    /// to the supplied `Writer`. Note that a `MergeFn` must be supplied to
    /// combine values for entries with colliding keys.
    pub fn create_from_writer<F>(writer: Writer, merge_fn: F) -> Sorter
        where F: Fn(&[u8], &[u8], &[u8]) -> Vec<u8> + 'static
    {
        SorterOptions::new().create_from_writer(writer, merge_fn)
    }

    /// Create a new `Sorter`.
    ///
    /// Once sorting is done, the resulting sequence of entries will be written
    /// to the supplied path. Note that a `MergeFn` must be supplied to combine
    /// values for entries with colliding keys.
    pub fn create_from_path<T, F>(path: T, merge_fn: F) -> IOResult<Sorter>
        where T: AsRef<Path>,
              F: Fn(&[u8], &[u8], &[u8]) -> Vec<u8> + 'static
    {
        SorterOptions::new().create_from_path(path, merge_fn)
    }

    /// Create a new `Sorter`.
    ///
    /// Once sorting is done, the resulting sequence of entries will be written
    /// to the supplied path. Note that a `MergeFn` must be supplied to combine
    /// values for entries with colliding keys.
    pub fn create_from_file<T, F>(file: T, merge_fn: F) -> IOResult<Sorter>
        where T: 'static + AsRawFd,
              F: Fn(&[u8], &[u8], &[u8]) -> Vec<u8> + 'static
    {
        SorterOptions::new().create_from_file(file, merge_fn)
    }

    /// Add all elements from an iterator.
    ///
    /// This will result in an Error only if the output Writer receives items
    /// out of order, which can only happen if the output Writer had already had
    /// items added, not from the Sorter.
    pub fn add_all<T, U, I>(&mut self, iterable: I) -> Result<(), ()>
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

impl Write for Sorter {
    /// Add a key-value pair to be written to the MTBL file.
    fn add<T, U>(&mut self, key: T, value: U) -> Result<(), ()>
        where T: AsRef<[u8]>,
              U: AsRef<[u8]>
    {
        let key = key.as_ref();
        let value = value.as_ref();
        unsafe {
            let res = mtbl_sys::mtbl_sorter_add(self.mtbl_sorter,
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

impl Drop for Sorter {
    fn drop(&mut self) {
        unsafe {
            // TODO check retval? I'm not sure how to handle errors in
            // destructors.
            mtbl_sys::mtbl_sorter_write(self.mtbl_sorter, self.writer.as_raw_ptr());
            mtbl_sys::mtbl_sorter_destroy(&mut self.mtbl_sorter);
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate tempfile;
    use self::tempfile::NamedTempFile;

    use std::os::unix::io::AsRawFd;

    use reader;
    use reader::Read as iRead;
    use sorter::{SorterOptions, Sorter};
    use writer::{Write, Writer};

    fn check_sorter<T: 'static + AsRawFd>(mut sorter: Sorter, tempfile_reader: T) {
        for i in 0..1000 {
            sorter.add(format!("{}", i), format!("entry {}", i))
                  .unwrap();
        }
        sorter.add_all((1000..2000).map(|i| (format!("{}", i), format!("entry {}", i)))).unwrap();
        drop(sorter);
        let reader = reader::Reader::open_from_file(&tempfile_reader).unwrap();
        for i in 0..1000 {
            assert_eq!(format!("entry {}", i).as_bytes().to_vec(),
                       reader.get(format!("{}", i).as_bytes()).unwrap());
        }
        for (k, v) in reader.iter() {
            let mut target = "entry ".as_bytes().to_vec();
            target.extend(k);
            assert_eq!(target, v);
        }
    }

    #[test]
    fn test_sorter() {
        let tempfile_writer = NamedTempFile::new().unwrap();
        let tempfile_reader = tempfile_writer.reopen().unwrap();
        let writer = Writer::create_from_file(tempfile_writer).unwrap();
        let sorter = Sorter::create_from_writer(writer, |_key, _val0, _val1| {
            "collision".as_bytes().to_vec()
        });
        check_sorter(sorter, tempfile_reader);
    }

    #[test]
    fn test_create_from_path() {
        let tempfile = NamedTempFile::new().unwrap();
        let sorter = Sorter::create_from_path(tempfile.path(),
                                              |_key, _val0, _val1| "collision".as_bytes().to_vec())
                         .unwrap();
        check_sorter(sorter, tempfile);
    }

    #[test]
    fn test_create_from_file() {
        let tempfile_writer = NamedTempFile::new().unwrap();
        let tempfile_reader = tempfile_writer.reopen().unwrap();
        let sorter = Sorter::create_from_file(tempfile_writer,
                                              |_key, _val0, _val1| "collision".as_bytes().to_vec())
                         .unwrap();
        check_sorter(sorter, tempfile_reader);
    }

    #[test]
    fn test_sorter_options() {
        let tempfile_writer = NamedTempFile::new().unwrap();
        let tempfile_reader = tempfile_writer.reopen().unwrap();
        let writer = Writer::create_from_file(tempfile_writer).unwrap();
        let sorter = SorterOptions::new()
                         .max_memory(300)
                         .create_from_writer(writer, |_key, _val0, _val1| {
                             "collision"
                                 .as_bytes()
                                 .to_vec()
                         });
        assert_eq!(sorter.options.max_memory, Some(300));
        check_sorter(sorter, tempfile_reader);
    }
}

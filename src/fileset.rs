use libc::c_void;
use std::ffi::CString;
use std::path::Path;

use merger;
use mtbl_sys;
use reader::{Read, Iter};

/// Options for opening an MTBL fileset.
///
/// # Examples
///
/// ```
/// FilesetOptions::new().reload_interval_seconds(10).open_from_path("/tmp/data-fileset")
/// ```
#[derive(Clone,Copy)]
pub struct FilesetOptions {
    /// How often, in seconds, to reload the fileset description file to look for new file entries.
    /// The mtbl default is 60 seconds.
    pub reload_interval_seconds: Option<u32>,
}

impl FilesetOptions {
    /// Create a `FilesetOptions` with only defaults.
    pub fn new() -> FilesetOptions {
        FilesetOptions { reload_interval_seconds: None }
    }

    /// Create a modified `FilesetOptions` with reload_interval_seconds set.
    pub fn reload_interval_seconds(self: &Self, reload_interval_seconds: u32) -> FilesetOptions {
        FilesetOptions { reload_interval_seconds: Some(reload_interval_seconds), ..*self }
    }

    /// Open a `Fileset` with these options from the specified setfile. Note that you must include
    /// a `MergeFn` to combine colliding entries that have the same key.
    pub fn open_from_path<T: AsRef<Path>>(self: &Self,
                                          setfile: T,
                                          merge_fn: Box<merger::MergeFn>)
                                          -> Fileset {
        let mut merge_fn = Box::new(merge_fn);
        unsafe {
            let mut opts = mtbl_sys::mtbl_fileset_options_init();
            mtbl_sys::mtbl_fileset_options_set_merge_func(
                // Wacky casting to get a void pointer for the C lib.
                opts, merger::_merge_cb_shim, &mut (*merge_fn) as *mut _ as *mut c_void);
            if let Some(reload_interval_seconds) = self.reload_interval_seconds {
                mtbl_sys::mtbl_fileset_options_set_reload_interval(opts, reload_interval_seconds);
            }
            let c_path = CString::new(setfile.as_ref().to_str().unwrap().as_bytes()).unwrap();
            let mtbl_fileset = mtbl_sys::mtbl_fileset_init(c_path.as_ptr(), opts);
            let fileset = Fileset {
                options: *self,
                mtbl_fileset: mtbl_fileset,
                mtbl_source: mtbl_sys::mtbl_fileset_source(mtbl_fileset),
                _merge_fn: merge_fn,
            };
            mtbl_sys::mtbl_fileset_options_destroy(&mut opts);
            fileset
        }
    }
}

/// An MTBL reader that watches a "setfile" containing a list of MTBL files to
/// read from.
///
/// It acts like a [`Merger`](type.Merger.html) that watches that setfile for
/// updates to a list of MTBL files. Note that paths in the setfile are
/// *relative* paths from the directory of the setfile.
///
/// `Fileset`s are not thread-safe because reloading, which happens
/// automatically when reading, is not thread-safe--so only one thread can read
/// from a `Fileset`.
///
/// To create a Fileset with non-default options, see
/// [FilesetOptions](struct.FilesetOptions.html).
///
/// # Examples
///
/// ```
/// $ cp my-data.mtbl /tmp/my-data.mtbl
/// $ echo 'my-data.mtbl' >> /tmp/fs.mtbl-fileset
/// ...
/// let fileset = Fileset::open_from_path("/tmp/fs.mtbl-fileset", my_merge_fn);
/// ```
pub struct Fileset {
    /// The options used to open this `Fileset`.
    pub options: FilesetOptions,
    mtbl_fileset: *mut mtbl_sys::mtbl_fileset,
    mtbl_source: *const mtbl_sys::mtbl_source,
    _merge_fn: Box<Box<merger::MergeFn>>,
}

impl Fileset {
    /// Open a `Fileset` from a path. Note that you must include a `MergeFn` to
    /// combine colliding entries (entries that have the same key).
    pub fn open_from_path<T: AsRef<Path>>(setfile: T, merge_fn: Box<merger::MergeFn>) -> Fileset {
        FilesetOptions::new().open_from_path(setfile, merge_fn)
    }

    /// Reload the list of MTBL files (ignored if less than the configured reload
    /// interval has passed).
    pub fn reload(&mut self) {
        unsafe {
            mtbl_sys::mtbl_fileset_reload(self.mtbl_fileset);
        }
    }
}

impl Read for Fileset {
    fn raw_mtbl_source(&self) -> &*const mtbl_sys::mtbl_source {
        &self.mtbl_source
    }
}

impl<'a> IntoIterator for &'a Fileset {
    type Item = (Vec<u8>, Vec<u8>);
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

// TODO: Implement IntoIterator for Fileset? I'm not sure how to cleanly do the needed lifetime
// stuff, though.

impl Drop for Fileset {
    fn drop(&mut self) {
        unsafe {
            mtbl_sys::mtbl_fileset_destroy(&mut self.mtbl_fileset);
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate tempfile;
    use self::tempfile::NamedTempFile;
    use std::io::Write;

    use fileset::FilesetOptions;
    use reader::Read;
    use writer;
    use writer::Write as iWrite;

    #[test]
    fn test_fileset() {
        let f1 = NamedTempFile::new().unwrap();
        {
            let mut writer = writer::Writer::create_from_path(f1.path()).unwrap();
            writer.add("one", "Hello").unwrap();
            writer.add("two", "world").unwrap();
        }
        let f2 = NamedTempFile::new().unwrap();
        {
            let mut writer = writer::Writer::create_from_path(f2.path()).unwrap();
            writer.add("one", "blue").unwrap();
            writer.add("three", "green").unwrap();
        }
        let mut fileset_f = NamedTempFile::new().unwrap();
        write!(fileset_f,
               "{}\n",
               f1.path().file_name().unwrap().to_str().unwrap())
            .unwrap();
        write!(fileset_f,
               "{}\n",
               f2.path().file_name().unwrap().to_str().unwrap())
            .unwrap();
        fileset_f.sync_all().unwrap();
        let fileset = FilesetOptions::new()
                          .reload_interval_seconds(50)
                          .open_from_path(fileset_f.path(),
                                          Box::new(|_key, _val0, _val1| "wat".as_bytes().to_vec()));
        assert_eq!(fileset.get("a"), None);
        // "one" collides
        assert_eq!(fileset.get("one"), Some("wat".as_bytes().to_vec()));
        assert_eq!(fileset.get("two"), Some("world".as_bytes().to_vec()));
        assert_eq!(fileset.get("three"), Some("green".as_bytes().to_vec()));
        // Iter
        for (k, v) in &fileset {
            println!("{} {}", k.len(), v.len());
        }
    }
}

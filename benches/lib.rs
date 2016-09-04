extern crate mtbl;
extern crate rand;
extern crate tempfile;
extern crate test;

use mtbl::{Read, Write};
use rand::distributions::{IndependentSample, Range};
use test::Bencher;

#[bench]
fn bench_get(bench: &mut Bencher) {
    let tempfile_writer = tempfile::NamedTempFile::new().unwrap();
    let tempfile_reader = tempfile_writer.reopen().unwrap();
    let n_keys = 100000;
    {
        let mut writer = mtbl::Writer::create_from_file(tempfile_writer).unwrap();
        for i in 0..n_keys {
            writer.add(format!("{:09}", i),
                       format!("val{:09}", i)).unwrap();
        }
    }
    let reader = mtbl::Reader::open_from_file(&tempfile_reader).unwrap();
    let mut rng = rand::weak_rng();
    let range_gen = Range::new(0, n_keys);
    bench.iter(|| reader.get(format!("{:09}", range_gen.ind_sample(&mut rng))));
}

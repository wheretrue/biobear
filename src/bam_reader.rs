use noodles::{bam, bgzf, sam};
use pyo3::prelude::*;

#[pyclass]
struct BamReader {
    // reader: bam::IndexedReader<bgzf::Reader<BufferedReader>>,
    // header: sam::Header,
}

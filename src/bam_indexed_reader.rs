use noodles::{bam, bgzf, sam};
use pyo3::prelude::*;

#[pyclass]
struct BamIndexedReader {
    reader: bam::IndexedReader<bgzf::Reader<BufferedReader>>,
    header: sam::Header,
}

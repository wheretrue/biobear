use pyo3::prelude::*;

mod fasta_reader;
mod fastq_reader;

#[pymodule]
fn biobear(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<fasta_reader::FastaReader>()?;
    m.add_class::<fastq_reader::FastqReader>()?;
    Ok(())
}

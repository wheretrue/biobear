use pyo3::prelude::*;

mod bam_reader;
mod bcf_reader;
mod exon_reader;
mod vcf_reader;

#[pymodule]
fn biobear(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<exon_reader::ExonReader>()?;

    m.add_class::<bam_reader::BamIndexedReader>()?;
    m.add_class::<vcf_reader::VCFIndexedReader>()?;
    m.add_class::<bcf_reader::BCFIndexedReader>()?;

    Ok(())
}

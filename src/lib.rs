use pyo3::prelude::*;

mod bam_reader;
mod fasta_reader;
mod fastq_reader;
mod gff_reader;
mod vcf_reader;

#[pymodule]
fn biobear(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<fasta_reader::FastaReader>()?;
    m.add_class::<fastq_reader::FastqReader>()?;
    m.add_class::<gff_reader::GFFReader>()?;
    m.add_class::<bam_reader::BamReader>()?;
    m.add_class::<bam_reader::BamIndexedReader>()?;
    m.add_class::<vcf_reader::VCFReader>()?;
    m.add_class::<vcf_reader::VCFIndexedReader>()?;
    Ok(())
}

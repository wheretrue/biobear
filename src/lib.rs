use pyo3::prelude::*;

mod bam_reader;
mod batch;
mod fasta_reader;
mod fastq_reader;
mod gff_reader;
mod vcf_reader;

#[pymodule]
fn biobear(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<fasta_reader::FastaReader>()?;
    m.add_class::<fasta_reader::FastaGzippedReader>()?;
    m.add_function(wrap_pyfunction!(fasta_reader::fasta_reader_to_pyarrow, m)?)?;
    m.add_function(wrap_pyfunction!(
        fasta_reader::fasta_gzipped_reader_to_pyarrow,
        m
    )?)?;

    m.add_class::<fastq_reader::FastqReader>()?;
    m.add_class::<fastq_reader::FastqGzippedReader>()?;
    m.add_function(wrap_pyfunction!(fastq_reader::fastq_reader_to_pyarrow, m)?)?;
    m.add_function(wrap_pyfunction!(
        fastq_reader::fastq_gzipped_reader_to_pyarrow,
        m
    )?)?;

    m.add_class::<gff_reader::GFFReader>()?;
    m.add_function(wrap_pyfunction!(gff_reader::gff_reader_to_pyarrow, m)?)?;

    m.add_class::<bam_reader::BamReader>()?;
    m.add_class::<bam_reader::BamIndexedReader>()?;
    m.add_function(wrap_pyfunction!(bam_reader::bam_reader_to_pyarrow, m)?)?;
    m.add_function(wrap_pyfunction!(
        bam_reader::bam_indexed_reader_to_pyarrow,
        m
    )?)?;

    m.add_class::<vcf_reader::VCFReader>()?;
    m.add_class::<vcf_reader::VCFIndexedReader>()?;
    m.add_function(wrap_pyfunction!(vcf_reader::vcf_reader_to_pyarrow, m)?)?;
    m.add_function(wrap_pyfunction!(
        vcf_reader::vcf_indexed_reader_to_pyarrow,
        m
    )?)?;

    Ok(())
}

use std::error::Error;
use std::io::{self};
use std::str::FromStr;
use std::sync::Arc;

use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::pyarrow::PyArrowConvert;
use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::prelude::{SessionConfig, SessionContext};
use exon::context::ExonSessionExt;
use exon::datasources::ExonFileType;
use exon::ffi::create_dataset_stream_from_table_provider;
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use pyo3::prelude::*;
use tokio::runtime::Runtime;
use url::Url;

fn add_google_filesystem(ctx: &mut SessionContext, path: &str) -> Result<(), Box<dyn Error>> {
    let url = Url::parse(path)?;
    let host_str = url.host_str().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid URL: {path}"))
    })?;

    let bucket_address = Url::parse(format!("gs://{host_str}").as_str())?;

    let gcs = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(host_str)
        .build()?;

    ctx.runtime_env()
        .register_object_store(&bucket_address, Arc::new(gcs));

    Ok(())
}

fn add_s3_filesystem(ctx: &mut SessionContext, path: &str) -> Result<(), Box<dyn Error>> {
    let url = Url::parse(path)?;
    let host_str = url.host_str().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid URL: {path}"))
    })?;

    let bucket_address = Url::parse(format!("s3://{host_str}").as_str())?;

    let s3 = AmazonS3Builder::from_env()
        .with_bucket_name(host_str)
        .build()?;

    ctx.runtime_env()
        .register_object_store(&bucket_address, Arc::new(s3));

    Ok(())
}

#[pyclass(name = "_ExonReader")]
pub struct ExonReader {
    df: datafusion::dataframe::DataFrame,
    _runtime: Arc<Runtime>,
}

impl ExonReader {
    fn open(
        path: &str,
        file_type: ExonFileType,
        compression: Option<FileCompressionType>,
        batch_size: Option<usize>,
    ) -> io::Result<Self> {
        let rt = Arc::new(Runtime::new().unwrap());

        let mut config = SessionConfig::new();
        if let Some(batch_size) = batch_size {
            config = config.with_batch_size(batch_size);
        }

        let mut ctx = SessionContext::with_config(config);

        if path.starts_with("gs://") {
            add_google_filesystem(&mut ctx, path).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Error adding Google Cloud Storage: {e}"),
                )
            })?;
        }

        if path.starts_with("s3://") {
            add_s3_filesystem(&mut ctx, path).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Error adding Amazon S3: {e}"))
            })?;
        }

        let df = rt.block_on(async {
            match ctx.read_exon_table(path, file_type, compression).await {
                Ok(df) => Ok(df),
                Err(e) => Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Error reading GFF file: {e}"),
                )),
            }
        });

        match df {
            Ok(df) => Ok(Self { df, _runtime: rt }),
            Err(e) => Err(e),
        }
    }
}

#[pymethods]
impl ExonReader {
    #[new]
    fn new(
        path: &str,
        file_type: &str,
        compression: Option<&str>,
        batch_size: Option<usize>,
    ) -> PyResult<Self> {
        let exon_file_type = ExonFileType::from_str(file_type).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Error reading file type: {e:?}"
            ))
        })?;

        let file_compression_type =
            compression.map(
                |compression| match FileCompressionType::from_str(compression) {
                    Ok(compression_type) => Ok(compression_type),
                    Err(e) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Error reading compression type: {e:?}"
                    ))),
                },
            );

        let file_compression_type = file_compression_type.transpose()?;

        Self::open(path, exon_file_type, file_compression_type, batch_size).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Error opening file {path}: {e}"))
        })
    }

    fn to_pyarrow(&self) -> PyResult<PyObject> {
        let stream = Arc::new(FFI_ArrowArrayStream::empty());
        let stream_ptr = Arc::into_raw(stream) as *mut FFI_ArrowArrayStream;

        self._runtime.block_on(async {
            create_dataset_stream_from_table_provider(
                self.df.clone(),
                self._runtime.clone(),
                stream_ptr,
            )
            .await;
        });

        Python::with_gil(|py| unsafe {
            match ArrowArrayStreamReader::from_raw(stream_ptr) {
                Ok(stream_reader) => stream_reader.to_pyarrow(py),
                Err(err) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Error converting to pyarrow: {err}"
                ))),
            }
        })
    }
}

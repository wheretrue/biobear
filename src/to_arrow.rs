use std::sync::Arc;

use arrow::{
    ffi_stream::{export_reader_into_raw, ArrowArrayStreamReader, FFI_ArrowArrayStream},
    pyarrow::PyArrowConvert,
    record_batch::RecordBatchReader,
};
use pyo3::{PyErr, PyObject, PyResult, Python};

pub fn to_pyarrow<C: Clone + RecordBatchReader + 'static>(reader: C) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let stream = Arc::new(FFI_ArrowArrayStream::empty());
        let stream_ptr = Arc::into_raw(stream) as *mut FFI_ArrowArrayStream;

        unsafe {
            export_reader_into_raw(Box::new(reader), stream_ptr);

            match ArrowArrayStreamReader::from_raw(stream_ptr) {
                Ok(stream_reader) => stream_reader.to_pyarrow(py),
                Err(err) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Error converting to pyarrow: {}",
                    err
                ))),
            }
        }
    })
}

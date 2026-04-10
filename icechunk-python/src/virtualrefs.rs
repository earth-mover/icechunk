use std::sync::Arc;

use icechunk::{
    Store,
    format::{
        ChunkIndices, Path,
        manifest::{Checksum, VirtualChunkLocation, VirtualChunkRef},
    },
    store::SetVirtualRefsResult,
};
use itertools::Itertools as _;
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyList, PyTuple},
};

use crate::errors::PyIcechunkStoreError;

/// Build `Vec<(ChunkIndices, VirtualChunkRef)>` from columnar Python data.
///
/// Must be called while holding the GIL (borrows `&str` from the Python list).
pub(crate) fn build_vrefs_from_arrays(
    locations: &Bound<'_, PyList>,
    offsets: &[u64],
    lengths: &[u64],
    chunk_grid_shape: &[u32],
    arr_offset: Option<&[u32]>,
    checksum: Option<Checksum>,
) -> PyResult<Vec<(ChunkIndices, VirtualChunkRef)>> {
    let n = locations.len();

    if offsets.len() != n || lengths.len() != n {
        return Err(PyValueError::new_err(
            "locations, offsets, and lengths must have the same length",
        ));
    }

    let expected: usize = chunk_grid_shape.iter().map(|&x| x as usize).product();
    if expected != n {
        return Err(PyValueError::new_err(format!(
            "product of chunk_grid_shape ({expected}) != array length ({n})"
        )));
    }

    if let Some(offset) = arr_offset {
        if offset.len() != chunk_grid_shape.len() {
            return Err(PyValueError::new_err(format!(
                "arr_offset length ({}) != chunk_grid_shape length ({})",
                offset.len(),
                chunk_grid_shape.len()
            )));
        }
    }

    (0..n)
        .map(|i| {
            let loc_item = locations.get_item(i)?;
            let loc_str: &str = loc_item.extract()?;
            let location = VirtualChunkLocation::from_url(loc_str)
                .map_err(PyIcechunkStoreError::from)?;
            let indices = flat_to_nd_indices(i, chunk_grid_shape, arr_offset);
            let vref = VirtualChunkRef {
                location,
                offset: offsets[i],
                length: lengths[i],
                checksum: checksum.clone(),
            };
            Ok((indices, vref))
        })
        .collect()
}

/// Normalize path and call store.set_virtual_refs.
pub(crate) async fn do_set_virtual_refs(
    store: Arc<Store>,
    array_path: String,
    validate_containers: bool,
    vrefs: Vec<(ChunkIndices, VirtualChunkRef)>,
) -> PyResult<SetVirtualRefsResult> {
    let array_path =
        if !array_path.starts_with("/") { format!("/{array_path}") } else { array_path };
    let path = Path::try_from(array_path)
        .map_err(|e| PyValueError::new_err(format!("Invalid array path: {e}")))?;
    store
        .set_virtual_refs(&path, validate_containers, vrefs)
        .await
        .map_err(|e| PyIcechunkStoreError::from(e).into())
}

/// Convert SetVirtualRefsResult to the Python return type.
pub(crate) fn vrefs_result_to_py(
    res: SetVirtualRefsResult,
) -> PyResult<Option<Vec<Py<PyTuple>>>> {
    match res {
        SetVirtualRefsResult::Done => Ok(None),
        SetVirtualRefsResult::FailedRefs(vec) => Python::attach(|py| {
            let res = vec
                .into_iter()
                .map(|ci| PyTuple::new(py, ci.0).map(|tup| tup.unbind()))
                .try_collect()?;
            Ok(Some(res))
        }),
    }
}

/// Convert flat index to N-dimensional chunk indices (C-order/row-major).
fn flat_to_nd_indices(
    flat: usize,
    shape: &[u32],
    offset: Option<&[u32]>,
) -> ChunkIndices {
    let ndim = shape.len();
    let mut indices = vec![0u32; ndim];
    let mut remainder = flat;
    for dim in (0..ndim).rev() {
        indices[dim] = (remainder % shape[dim] as usize) as u32;
        remainder /= shape[dim] as usize;
    }
    if let Some(off) = offset {
        for (idx, o) in indices.iter_mut().zip(off.iter()) {
            *idx += o;
        }
    }
    ChunkIndices(indices)
}

//! Zarr array metadata types and pure-compute helpers.
//!
//! Both [`crate::store`] and [`crate::session`] need to read and rewrite Zarr
//! `chunk_grid` descriptions (regular and rectilinear). This module owns those
//! types so neither of those modules has to import from the other.

use bytes::Bytes;
use itertools::{izip, repeat_n};
use serde::{Deserialize, Serialize, de};
use thiserror::Error;

use crate::format::{
    ChunkIndices,
    snapshot::{ArrayShape, DimensionName},
};
use icechunk_types::{ICResultExt as _, error::ICError};

/// Per-axis chunk grid layout: either a single repeating size or an explicit list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AxisLayout {
    Regular { chunk_size: u64 },
    Rectilinear { sizes: Vec<u64> },
}

/// Precomputed per-axis lookup table used by [`ArrayMetadata::get_chunk_shapes`].
enum AxisChunkLookup {
    Regular { chunk_size: u32, num_chunks: u32, remainder: u32 },
    Rectilinear { sizes: Vec<u32> },
}

/// Textual form an axis used in the on-disk `chunk_shapes` JSON. Preserved so
/// that round-tripping rectilinear axes doesn't gratuitously change encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AxisForm {
    Flat,
    Rle,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ArrayMetadataError {
    #[error("error decoding Zarr chunk grid metadata: `{0}`")]
    BadChunkGrid(String),
    #[error(
        "invalid chunk coordinates {coords:?}: along axis {axis} with num_chunks={num_chunks}"
    )]
    InvalidIndex { axis: usize, coords: ChunkIndices, num_chunks: u32 },
    #[error("bad metadata")]
    BadMetadata(#[from] serde_json::Error),
}

pub type ArrayMetadataResult<T> = Result<T, ICError<ArrayMetadataError>>;

// The orphan rule blocks `From<ICError<ArrayMetadataError>>` for the foreign
// `ICError<StoreErrorKind>` / `ICError<SessionErrorKind>` aliases, so callers
// bridge via `.inject()` / `.capture()` (which use the `From` impls below for
// the bare kinds).
impl From<ArrayMetadataError> for crate::store::StoreErrorKind {
    fn from(err: ArrayMetadataError) -> Self {
        match err {
            ArrayMetadataError::InvalidIndex { axis, coords, num_chunks } => {
                crate::store::StoreErrorKind::InvalidIndex { axis, coords, num_chunks }
            }
            ArrayMetadataError::BadMetadata(e) => {
                crate::store::StoreErrorKind::BadMetadata(e)
            }
            ArrayMetadataError::BadChunkGrid(msg) => {
                crate::store::StoreErrorKind::BadChunkGridMetadata(msg)
            }
        }
    }
}

impl From<ArrayMetadataError> for crate::session::SessionErrorKind {
    fn from(err: ArrayMetadataError) -> Self {
        match err {
            ArrayMetadataError::BadMetadata(e) => {
                crate::session::SessionErrorKind::JsonSerializationError(e)
            }
            ArrayMetadataError::BadChunkGrid(msg) => {
                crate::session::SessionErrorKind::BadChunkGridMetadata(msg)
            }
            // SessionErrorKind::InvalidIndex carries a Path but no axis/num_chunks,
            // so the typed shape doesn't round-trip; preserve the message instead.
            other @ ArrayMetadataError::InvalidIndex { .. } => {
                crate::session::SessionErrorKind::BadChunkGridMetadata(other.to_string())
            }
        }
    }
}

fn bad_chunk_grid(msg: impl Into<String>) -> ICError<ArrayMetadataError> {
    ICError::capture(ArrayMetadataError::BadChunkGrid(msg.into()))
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ArrayMetadata {
    pub shape: Vec<u64>,

    #[serde(deserialize_with = "validate_array_node_type")]
    node_type: String,

    chunk_grid: ChunkGridSerializer,

    pub dimension_names: Option<Vec<Option<String>>>,
}

impl ArrayMetadata {
    pub(crate) fn dimension_names(&self) -> Option<Vec<DimensionName>> {
        self.dimension_names
            .as_ref()
            .map(|ds| ds.iter().map(|d| d.as_ref().map(|s| s.as_str()).into()).collect())
    }

    pub fn is_regular(&self) -> bool {
        self.chunk_grid.name == "regular"
    }

    pub fn num_chunks(&self) -> ArrayMetadataResult<Vec<u32>> {
        let layouts = self.parse_axis_layouts_with_form()?;
        Ok(layouts
            .iter()
            .zip(self.shape.iter())
            .map(|((layout, _), dim_length)| match layout {
                AxisLayout::Regular { chunk_size } => {
                    if *chunk_size == 0 {
                        0
                    } else {
                        (*dim_length).div_ceil(*chunk_size) as u32
                    }
                }
                AxisLayout::Rectilinear { sizes } => sizes.len() as u32,
            })
            .collect())
    }

    /// Look up chunk size along each axis for a given chunk coordinate
    pub fn get_chunk_shapes<'a>(
        &self,
        coords: impl Iterator<Item = &'a ChunkIndices> + 'a,
    ) -> ArrayMetadataResult<Box<dyn Iterator<Item = ArrayMetadataResult<Vec<u32>>> + 'a>>
    {
        let layouts = self.parse_axis_layouts_with_form()?;
        // Precompute per-axis state so the inner loop is a tight match.
        let per_axis: Vec<AxisChunkLookup> = layouts
            .into_iter()
            .zip(self.shape.iter())
            .map(|((layout, _), &dim_length)| match layout {
                AxisLayout::Regular { chunk_size } => {
                    let num_chunks = if chunk_size == 0 {
                        0
                    } else {
                        dim_length.div_ceil(chunk_size) as u32
                    };
                    let remainder = if chunk_size == 0 {
                        0
                    } else {
                        (dim_length % chunk_size) as u32
                    };
                    AxisChunkLookup::Regular {
                        chunk_size: chunk_size as u32,
                        num_chunks,
                        remainder,
                    }
                }
                AxisLayout::Rectilinear { sizes } => AxisChunkLookup::Rectilinear {
                    sizes: sizes.into_iter().map(|s| s as u32).collect(),
                },
            })
            .collect();

        let iter = coords.map(move |coord| {
            coord
                .0
                .iter()
                .enumerate()
                .zip(per_axis.iter())
                .map(|((axis, axcoord), lookup)| match lookup {
                    AxisChunkLookup::Regular { chunk_size, num_chunks, remainder } => {
                        if axcoord >= num_chunks {
                            Err(ArrayMetadataError::InvalidIndex {
                                axis,
                                coords: coord.clone(),
                                num_chunks: *num_chunks,
                            })
                            .capture()
                        } else if *remainder == 0 || *axcoord < num_chunks - 1 {
                            Ok(*chunk_size)
                        } else {
                            Ok(*remainder)
                        }
                    }
                    AxisChunkLookup::Rectilinear { sizes } => {
                        if (*axcoord as usize) >= sizes.len() {
                            Err(ArrayMetadataError::InvalidIndex {
                                axis,
                                coords: coord.clone(),
                                num_chunks: sizes.len() as u32,
                            })
                            .capture()
                        } else {
                            Ok(sizes[*axcoord as usize])
                        }
                    }
                })
                .collect::<ArrayMetadataResult<Vec<_>>>()
        });
        Ok(Box::new(iter))
    }

    /// Per-axis chunk layout extracted from the array's Zarr metadata.
    pub fn parse_axis_layouts_with_form(
        &self,
    ) -> ArrayMetadataResult<Vec<(AxisLayout, AxisForm)>> {
        let serde_json::Value::Object(kvs) = &self.chunk_grid.configuration else {
            return Err(bad_chunk_grid("Unsupported chunk grid"));
        };
        match self.chunk_grid.name.as_str() {
            "regular" => {
                let values = kvs
                    .get("chunk_shape")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| {
                        bad_chunk_grid(
                            "cannot parse `chunk_shape` for regular chunk grid",
                        )
                    })?;
                let layouts = values
                    .iter()
                    .map(|c| {
                        c.as_u64().map(|chunk_size| {
                            (AxisLayout::Regular { chunk_size }, AxisForm::Flat)
                        })
                    })
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| {
                        bad_chunk_grid(
                            "cannot parse `chunk_shape` for regular chunk grid",
                        )
                    })?;
                Ok(layouts)
            }
            "rectilinear" => {
                let values = kvs
                    .get("chunk_shapes")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| {
                        bad_chunk_grid(
                            "cannot parse `chunk_shapes` for rectilinear chunk grid",
                        )
                    })?;
                values
                    .iter()
                    .map(|v| {
                        let arr = v.as_array().ok_or_else(|| {
                            bad_chunk_grid("cannot parse `chunk_shapes` axis entry")
                        })?;
                        // Mixed forms are permitted (`[1, [2, 1], 3]`); classify
                        // as RLE if any element is an inner array.
                        let mut sizes: Vec<u64> = Vec::with_capacity(arr.len());
                        let mut has_rle = false;
                        for inner in arr {
                            if inner.is_number() {
                                let n = inner.as_u64().ok_or_else(|| bad_chunk_grid(
                                    "cannot parse chunk size",
                                ))?;
                                sizes.push(n);
                            } else if let Some(pair) = inner.as_array() {
                                has_rle = true;
                                let elem = pair.first().and_then(|v| v.as_u64()).ok_or_else(|| bad_chunk_grid(
                                    "cannot parse RLE chunk size",
                                ))?;
                                let count = pair.get(1).and_then(|v| v.as_u64()).ok_or_else(|| bad_chunk_grid(
                                    "cannot parse RLE chunk count",
                                ))? as usize;
                                sizes.extend(repeat_n(elem, count));
                            } else {
                                return Err(bad_chunk_grid(
                                    "cannot parse `chunk_shapes` for rectilinear chunk grid",
                                ));
                            }
                        }
                        let form = if has_rle { AxisForm::Rle } else { AxisForm::Flat };
                        Ok((AxisLayout::Rectilinear { sizes }, form))
                    })
                    .collect::<ArrayMetadataResult<Vec<_>>>()
            }
            _other => Err(bad_chunk_grid(format!(
                "Unsupported chunk grid {_other}. Only 'regular' and 'rectilinear' chunk grids are supported."
            ))),
        }
    }

    pub fn shape(&self) -> ArrayMetadataResult<ArrayShape> {
        let num_chunks = self.num_chunks()?;
        if self.shape.len() != num_chunks.len() {
            Err(bad_chunk_grid(format!(
                "Fewer dimensions on inferred number of chunks {} than shape {}",
                self.shape.len(),
                num_chunks.len()
            )))
        } else {
            ArrayShape::new(
                self.shape.iter().zip(num_chunks.iter()).map(|(a, b)| (*a, *b)),
            )
            .ok_or_else(|| bad_chunk_grid("invalid shape"))
        }
    }
}

fn validate_array_node_type<'de, D>(d: D) -> Result<String, D::Error>
where
    D: de::Deserializer<'de>,
{
    let value = String::deserialize(d)?;

    if value != "array" {
        return Err(de::Error::invalid_value(
            de::Unexpected::Str(value.as_str()),
            &"the word 'array'",
        ));
    }

    Ok(value)
}

#[derive(Debug, Deserialize)]
pub(crate) struct NodeMetadata {
    #[serde(deserialize_with = "validate_node_type")]
    pub(crate) node_type: String,
}

fn validate_node_type<'de, D>(d: D) -> Result<String, D::Error>
where
    D: de::Deserializer<'de>,
{
    let value = String::deserialize(d)?;

    if value != "array" && value != "group" {
        return Err(de::Error::invalid_value(
            de::Unexpected::Str(value.as_str()),
            &"'array' or 'group'",
        ));
    }

    Ok(value)
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ChunkGridSerializer {
    name: String,
    configuration: serde_json::Value,
}

#[cfg(test)]
/// Used as a convenience method in the tests
impl From<Vec<u64>> for ChunkGridSerializer {
    fn from(value: Vec<u64>) -> Self {
        let arr = serde_json::Value::Array(
            value
                .iter()
                .map(|v| serde_json::Value::Number(serde_json::value::Number::from(*v)))
                .collect(),
        );
        let kvs = serde_json::value::Map::from_iter(std::iter::once((
            "chunk_shape".to_string(),
            arr,
        )));
        Self {
            name: "regular".to_string(),
            configuration: serde_json::Value::Object(kvs),
        }
    }
}

/// Per-axis result of computing the post-shift chunk layout for an array.
#[derive(Debug, Clone)]
pub struct AxisPostShift {
    /// New per-chunk sizes for the axis, or `None` if the axis is unchanged.
    pub(crate) new_sizes: Option<Vec<u64>>,
    /// How to remap source chunk indices to destination chunk indices.
    pub(crate) remap: AxisRemap,
}

impl AxisPostShift {
    /// Number of chunks along the axis in the post-shift grid.
    pub(crate) fn new_num_chunks(&self) -> u32 {
        match &self.remap {
            AxisRemap::SimpleShift { num_chunks, .. } => *num_chunks,
            AxisRemap::Lookup { forward: _, backward } => backward.len() as u32,
        }
    }
}

#[derive(Debug, Clone)]
pub enum AxisRemap {
    /// Regular grid or zero-offset axis: `new = old + offset`, bounds-checked
    /// against the source `num_chunks`.
    SimpleShift { offset: i64, num_chunks: u32 },
    /// Rectilinear axis with a non-zero offset: explicit lookup tables.
    /// `forward[i]` gives the destination index for source chunk `i`;
    /// `backward[j]` gives the source index for destination chunk `j`.
    Lookup { forward: Vec<Option<u32>>, backward: Vec<Option<u32>> },
}

/// Compute the post-shift chunk layout per axis. See `Session::shift_array`
/// for the metadata-only rectilinear shift semantics.
pub fn compute_post_shift_layout(
    layouts: &[AxisLayout],
    offsets: &[i64],
    dim_lengths: &[u64],
) -> ArrayMetadataResult<Vec<AxisPostShift>> {
    if layouts.len() != offsets.len() || layouts.len() != dim_lengths.len() {
        return Err(bad_chunk_grid("rank mismatch when computing shifted layout"));
    }
    izip!(layouts, offsets, dim_lengths)
        .map(|(layout, &offset, &dim_length)| {
            compute_axis_post_shift(layout, offset, dim_length)
        })
        .collect()
}

fn compute_axis_post_shift(
    layout: &AxisLayout,
    offset: i64,
    dim_length: u64,
) -> ArrayMetadataResult<AxisPostShift> {
    match layout {
        AxisLayout::Regular { chunk_size } => {
            let num_chunks = if *chunk_size == 0 {
                0
            } else {
                dim_length.div_ceil(*chunk_size) as u32
            };
            Ok(AxisPostShift {
                new_sizes: None,
                remap: AxisRemap::SimpleShift { offset, num_chunks },
            })
        }
        AxisLayout::Rectilinear { sizes } => {
            let n = sizes.len() as u32;
            if offset == 0 {
                return Ok(AxisPostShift {
                    new_sizes: None,
                    remap: AxisRemap::SimpleShift { offset: 0, num_chunks: n },
                });
            }
            let k_abs = offset.unsigned_abs();
            if k_abs >= n as u64 {
                // Everything drops off the array: collapse to a single fill chunk.
                let new_sizes = vec![dim_length];
                let forward = vec![None; n as usize];
                let backward = vec![None];
                return Ok(AxisPostShift {
                    new_sizes: Some(new_sizes),
                    remap: AxisRemap::Lookup { forward, backward },
                });
            }
            let k = k_abs as usize;
            let (new_sizes, forward, backward) = if offset > 0 {
                // Surviving source range [0, n - k): survivors land at dest
                // indices [1, n - k + 1); dest 0 is the coalesced fill.
                let survivors = &sizes[..sizes.len() - k];
                let sum_survivors: u64 = survivors.iter().sum();
                let vacated = dim_length.checked_sub(sum_survivors).ok_or_else(|| {
                    bad_chunk_grid("surviving chunks exceed dim length")
                })?;
                let mut new_sizes = Vec::with_capacity(survivors.len() + 1);
                new_sizes.push(vacated);
                new_sizes.extend_from_slice(survivors);
                let forward: Vec<Option<u32>> =
                    (0..sizes.len())
                        .map(|i| {
                            if i + k < sizes.len() { Some((i + 1) as u32) } else { None }
                        })
                        .collect();
                let mut backward: Vec<Option<u32>> = vec![None; new_sizes.len()];
                for (src, dest) in forward.iter().enumerate() {
                    if let Some(d) = dest {
                        backward[*d as usize] = Some(src as u32);
                    }
                }
                (new_sizes, forward, backward)
            } else {
                // offset < 0: surviving source range [k, n); land at dest
                // indices [0, n - k); tail dest chunk is the coalesced fill.
                let survivors = &sizes[k..];
                let sum_survivors: u64 = survivors.iter().sum();
                let vacated = dim_length.checked_sub(sum_survivors).ok_or_else(|| {
                    bad_chunk_grid("surviving chunks exceed dim length")
                })?;
                let mut new_sizes = Vec::with_capacity(survivors.len() + 1);
                new_sizes.extend_from_slice(survivors);
                new_sizes.push(vacated);
                let forward: Vec<Option<u32>> = (0..sizes.len())
                    .map(|i| if i >= k { Some((i - k) as u32) } else { None })
                    .collect();
                let mut backward: Vec<Option<u32>> = vec![None; new_sizes.len()];
                for (src, dest) in forward.iter().enumerate() {
                    if let Some(d) = dest {
                        backward[*d as usize] = Some(src as u32);
                    }
                }
                (new_sizes, forward, backward)
            };
            debug_assert_eq!(new_sizes.iter().sum::<u64>(), dim_length);
            Ok(AxisPostShift {
                new_sizes: Some(new_sizes),
                remap: AxisRemap::Lookup { forward, backward },
            })
        }
    }
}

/// Per-axis remap precomputed for a single direction (forward or backward).
/// The `Shift` variant collapses regular and zero-offset rectilinear axes;
/// `Lookup` carries the explicit table for non-zero rectilinear shifts.
#[derive(Debug, Clone)]
pub enum AxisRemapResolved {
    Shift { offset: i64, bound: u32 },
    Lookup(Vec<Option<u32>>),
}

/// Flatten `per_axis` (with the `inverse` choice already applied) into a
/// per-axis table the chunk-remap loop can scan without further branching.
/// The Lookup tables already have the correct length for their input grid
/// (source for forward, destination for backward), so out-of-range coords
/// short-circuit naturally via `table.get(...)`.
pub fn resolve_remap(
    per_axis: &[AxisPostShift],
    inverse: bool,
) -> Vec<AxisRemapResolved> {
    per_axis
        .iter()
        .map(|axis| match &axis.remap {
            AxisRemap::SimpleShift { offset, num_chunks } => AxisRemapResolved::Shift {
                offset: if inverse { -*offset } else { *offset },
                bound: *num_chunks,
            },
            AxisRemap::Lookup { forward, backward } => {
                let table = if inverse { backward } else { forward };
                AxisRemapResolved::Lookup(table.clone())
            }
        })
        .collect()
}

/// Apply a [`resolve_remap`] table to a single coordinate. `None` means the
/// coord is out of range or maps to nothing (the reindex loop treats this as
/// "drop this chunk" / "stale source").
#[inline]
pub fn apply_remap(
    table: &[AxisRemapResolved],
    index: &ChunkIndices,
) -> Option<ChunkIndices> {
    let coords: Option<Vec<u32>> = index
        .0
        .iter()
        .zip(table.iter())
        .map(|(&idx, axis)| match axis {
            AxisRemapResolved::Shift { offset, bound } => {
                let new_idx = (idx as i64).checked_add(*offset)?;
                if new_idx < 0 || new_idx >= *bound as i64 {
                    None
                } else {
                    Some(new_idx as u32)
                }
            }
            AxisRemapResolved::Lookup(table) => {
                table.get(idx as usize).copied().flatten()
            }
        })
        .collect();
    coords.map(ChunkIndices)
}

/// Rewrite the `chunk_grid.configuration.chunk_shapes` JSON array so that
/// axes with new per-chunk sizes are replaced, while untouched axes remain
/// byte-identical. Callers must gate on `per_axis.iter().any(|a|
/// a.new_sizes.is_some())` — this function unconditionally parses and
/// re-serializes `user_data`.
pub fn rebuild_user_data(
    user_data: &Bytes,
    per_axis: &[AxisPostShift],
    forms: &[AxisForm],
) -> ArrayMetadataResult<Bytes> {
    let mut value: serde_json::Value = serde_json::from_slice(user_data).capture()?;
    let chunk_shapes = value
        .get_mut("chunk_grid")
        .and_then(|cg| cg.get_mut("configuration"))
        .and_then(|cfg| cfg.get_mut("chunk_shapes"))
        .and_then(|cs| cs.as_array_mut())
        .ok_or_else(|| {
            bad_chunk_grid(
                "missing chunk_grid.configuration.chunk_shapes for rectilinear array",
            )
        })?;
    if chunk_shapes.len() != per_axis.len() {
        return Err(bad_chunk_grid(format!(
            "chunk_shapes axis count {} != array rank {}",
            chunk_shapes.len(),
            per_axis.len()
        )));
    }
    for (ax, axis_shift) in per_axis.iter().enumerate() {
        if let Some(new_sizes) = &axis_shift.new_sizes {
            chunk_shapes[ax] = axis_sizes_to_json(new_sizes, forms[ax]);
        }
    }
    let bytes = serde_json::to_vec(&value).capture()?;
    Ok(Bytes::from(bytes))
}

fn axis_sizes_to_json(sizes: &[u64], form: AxisForm) -> serde_json::Value {
    let flat = || {
        serde_json::Value::Array(
            sizes.iter().map(|s| serde_json::Value::from(*s)).collect(),
        )
    };
    match form {
        AxisForm::Flat => flat(),
        AxisForm::Rle => {
            // Greedy run-length encode; fall back to flat if RLE would not
            // be a strict improvement (avoids gratuitous [s, 1] singletons).
            let mut runs: Vec<(u64, u64)> = Vec::new();
            for &s in sizes {
                match runs.last_mut() {
                    Some(last) if last.0 == s => last.1 += 1,
                    _ => runs.push((s, 1)),
                }
            }
            if runs.len() >= sizes.len() {
                flat()
            } else {
                serde_json::Value::Array(
                    runs.into_iter()
                        .map(|(s, c)| {
                            serde_json::Value::Array(vec![
                                serde_json::Value::from(s),
                                serde_json::Value::from(c),
                            ])
                        })
                        .collect(),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use icechunk_format::roundtrip_serialization_tests;
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;
    use proptest::{collection::vec, option};

    prop_compose! {
        fn array_metadata()
        (shape in vec(any::<u64>(), 1..4),
         node_type in Just("array".to_string()),
            chunk_grid in vec(any::<u64>(), 1..4),
            dimension_names in option::of(
            vec(
                option::of(any::<String>()),
                2..4))) -> ArrayMetadata {
            ArrayMetadata {
                shape,
                node_type,
                chunk_grid: chunk_grid.into(),
                dimension_names,
            }
        }
    }

    roundtrip_serialization_tests!(
        serialize_and_deserialize_array_metadata - array_metadata
    );

    #[icechunk_macros::test]
    fn test_metadata_serialization() {
        assert!(
            serde_json::from_str::<NodeMetadata>(
                r#"{"zarr_format":3, "node_type":"group"}"#
            )
            .is_ok()
        );
        assert!(
            serde_json::from_str::<NodeMetadata>(
                r#"{"zarr_format":3, "node_type":"array"}"#
            )
            .is_ok()
        );
        assert!(
            serde_json::from_str::<NodeMetadata>(
                r#"{"zarr_format":3, "node_type":"zarr"}"#
            )
            .is_err()
        );

        assert!(serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            )
            .is_ok());
        assert!(serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"group","shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            )
            .is_err());

        // deserialize with nan
        assert_eq!(
                serde_json::from_str::<ArrayMetadata>(
                    r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"NaN","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
                ).unwrap().dimension_names(),
                Some(vec!["x".into(), "y".into(), "t".into()])

            );
        assert_eq!(
                serde_json::from_str::<ArrayMetadata>(
                    r#"{"zarr_format":3,"node_type":"array","shape":[2,3,4],"data_type":"float16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,2,3]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"NaN","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
                ).unwrap().shape().unwrap(),
                ArrayShape::new(vec![(2,2), (3,2), (4,2) ]).unwrap()
            );
    }

    #[icechunk_macros::tokio_test]
    async fn test_get_chunk_shapes_regular() -> Result<(), Box<dyn std::error::Error>> {
        // all of these are valid ways of specifying chunk sizes for a dimension of size
        let chunk_grid =
            r#"{"name":"regular", "configuration": {"chunk_shape": [1, 2, 3, 4, 5, 6]}}"#;
        let zarr_meta = Bytes::from(format!(
            r#"{{"zarr_format":3,"node_type":"array","attributes":{{"foo":42}},"shape":[6,6,6,6,6,6],"data_type":"int32","chunk_grid":{chunk_grid},"chunk_key_encoding":{{"name":"default","configuration":{{"separator":"/"}}}},"fill_value":0,"codecs":[{{"name":"mycodec","configuration":{{"foo":42}}}}],"storage_transformers":[{{"name":"mytransformer","configuration":{{"bar":43}}}}],"dimension_names":["x","y","t"]}}"#
        ));
        let meta: ArrayMetadata = serde_json::from_slice(&zarr_meta)?;

        // these should work
        let actual = meta
            .get_chunk_shapes(
                [
                    ChunkIndices(vec![0, 0, 0, 0, 0, 0]),
                    ChunkIndices(vec![1, 1, 1, 0, 0, 0]),
                    ChunkIndices(vec![0, 1, 1, 1, 1, 0]),
                ]
                .iter(),
            )?
            .collect::<ArrayMetadataResult<Vec<_>>>()?;
        let expected =
            vec![vec![1, 2, 3, 4, 5, 6], vec![1, 2, 3, 4, 5, 6], vec![1, 2, 3, 2, 1, 6]];
        assert_eq!(expected, actual);

        // these should fail
        let actual = meta
            .get_chunk_shapes(
                [
                    ChunkIndices(vec![6, 0, 0, 0, 0, 0]),
                    ChunkIndices(vec![0, 3, 0, 0, 0, 0]),
                    ChunkIndices(vec![0, 0, 4, 0, 0, 0]),
                    ChunkIndices(vec![0, 0, 0, 3, 0, 0]),
                    ChunkIndices(vec![0, 0, 0, 0, 5, 0]),
                    ChunkIndices(vec![0, 1, 2, 2, 3, 1]),
                ]
                .iter(),
            )?
            .collect::<Vec<_>>();
        assert!(actual.iter().all(|x| x.is_err()));

        Ok(())
    }

    #[icechunk_macros::tokio_test]
    async fn test_get_chunk_shapes_rectilinear() -> Result<(), Box<dyn std::error::Error>>
    {
        // all of these are valid ways of specifying chunk sizes for a dimension of size
        let chunk_grid = r#"{
            "name":"rectilinear",
            "configuration": {
                "kind": "inline",
                "chunk_shapes": [
                        [2, 2, 2],
                        [[2, 3]],
                        [[1, 6]],
                        [1, [2, 1], 3],
                        [[1, 3], 3],
                        [6]
                ]
            }}"#;
        let zarr_meta = Bytes::from(format!(
            r#"{{"zarr_format":3,"node_type":"array","attributes":{{"foo":42}},"shape":[6,6,6,6,6,6],"data_type":"int32","chunk_grid":{chunk_grid},"chunk_key_encoding":{{"name":"default","configuration":{{"separator":"/"}}}},"fill_value":0,"codecs":[{{"name":"mycodec","configuration":{{"foo":42}}}}],"storage_transformers":[{{"name":"mytransformer","configuration":{{"bar":43}}}}],"dimension_names":["x","y","t"]}}"#
        ));
        let meta: ArrayMetadata = serde_json::from_slice(&zarr_meta)?;

        // these should work
        let actual = meta
            .get_chunk_shapes(
                [
                    ChunkIndices(vec![0, 0, 0, 0, 0, 0]),
                    ChunkIndices(vec![0, 1, 2, 2, 3, 0]),
                ]
                .iter(),
            )?
            .collect::<ArrayMetadataResult<Vec<_>>>()?;
        let expected = vec![vec![2, 2, 1, 1, 1, 6], vec![2, 2, 1, 3, 3, 6]];
        assert_eq!(expected, actual);

        // these should fail
        let actual = meta
            .get_chunk_shapes(
                [
                    ChunkIndices(vec![3, 0, 0, 0, 0, 0]),
                    ChunkIndices(vec![0, 3, 0, 0, 0, 0]),
                    ChunkIndices(vec![0, 0, 6, 0, 0, 0]),
                    ChunkIndices(vec![0, 0, 0, 3, 0, 0]),
                    ChunkIndices(vec![0, 0, 0, 0, 5, 0]),
                    ChunkIndices(vec![0, 1, 2, 2, 3, 1]),
                ]
                .iter(),
            )?
            .collect::<Vec<_>>();
        assert!(actual.iter().all(|x| x.is_err()));

        Ok(())
    }
}

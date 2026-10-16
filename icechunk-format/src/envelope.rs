//! The envelope every metadata file shares: a binary header followed by one
//! zstd frame holding the serialized body.

use std::io::Write as _;

use icechunk_types::ICResultExt as _;

use crate::{
    IcechunkResult,
    format_constants::{
        CompressionAlgorithmBin, FileTypeBin, ICECHUNK_CLIENT_NAME,
        ICECHUNK_FORMAT_MAGIC_BYTES, ICECHUNK_IMPL_NAME_LEN, SpecVersionBin,
    },
    serializers::{VERIFY_THRESHOLD_BYTES, verify_buffer},
};

pub fn binary_file_header(
    spec_version: SpecVersionBin,
    file_type: FileTypeBin,
    compression_algorithm: CompressionAlgorithmBin,
) -> Vec<u8> {
    // TODO: initialize capacity
    let mut buffer = Vec::with_capacity(1024);
    // magic numbers
    buffer.extend_from_slice(ICECHUNK_FORMAT_MAGIC_BYTES);
    // implementation name
    let implementation =
        format!("{:<width$}", *ICECHUNK_CLIENT_NAME, width = ICECHUNK_IMPL_NAME_LEN);
    buffer.extend_from_slice(&implementation.as_bytes()[..ICECHUNK_IMPL_NAME_LEN]);
    // spec version
    buffer.push(spec_version as u8);
    buffer.push(file_type as u8);
    // compression
    buffer.push(compression_algorithm as u8);
    buffer
}

/// Fail the write if `data` is large enough to be at risk and cannot be read back.
///
/// Serializers don't verify what they build, so without this a file that trips a
/// limit is written happily and only fails when someone reads it back.
fn verify_before_write(
    data: &[u8],
    file_type: FileTypeBin,
    threshold: usize,
) -> IcechunkResult<()> {
    if data.len() >= threshold {
        verify_buffer(file_type, data)?;
    }
    Ok(())
}

/// The header followed by `data` compressed into one zstd frame that declares
/// its decoded size.
pub fn compress_with_header(
    data: &[u8],
    spec_version: SpecVersionBin,
    file_type: FileTypeBin,
    compression_level: u8,
) -> IcechunkResult<Vec<u8>> {
    verify_before_write(data, file_type, VERIFY_THRESHOLD_BYTES)?;
    let mut buffer =
        binary_file_header(spec_version, file_type, CompressionAlgorithmBin::Zstd);
    let mut encoder =
        zstd::stream::write::Encoder::new(&mut buffer, i32::from(compression_level))
            .capture()?;
    // pledging the length records it in the frame header, so readers can size
    // the decoded buffer exactly; the frame is written straight into `buffer`
    encoder.set_pledged_src_size(Some(data.len() as u64)).capture()?;
    encoder.write_all(data).capture()?;
    encoder.finish().capture()?;
    Ok(buffer)
}

/// Decoded size a zstd frame declares in its header, if the writer recorded one.
/// Older writers do not record it.
pub fn frame_content_size(frame: &[u8]) -> Option<u64> {
    zstd::zstd_safe::get_frame_content_size(frame).ok().flatten()
}

/// Largest size we preallocate for: a frame declaring more than this falls
/// back to the growing decoder, and a caller's hint is capped at it, so
/// neither a corrupt header nor a wild estimate drives a huge allocation.
const MAX_PREALLOCATED_DECODE_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Buffer capacity to allocate up front for `bytes` decoded bytes, capped at
/// [`MAX_PREALLOCATED_DECODE_BYTES`]: both the size a frame declares and the
/// caller's estimate are untrusted.
fn prealloc_capacity(bytes: u64) -> usize {
    usize::try_from(bytes.min(MAX_PREALLOCATED_DECODE_BYTES)).unwrap_or(usize::MAX)
}

/// Decompress one zstd frame. With a declared content size the output is
/// allocated exactly; otherwise `size_hint` (an estimate from the caller)
/// sizes the buffer and the decoder grows it only if the estimate was low.
///
/// The zstd frame format makes a declared size exact, so a frame that decodes
/// to a different length is corrupt and is rejected.
pub fn decompress_frame(
    frame: &[u8],
    size_hint: Option<usize>,
) -> std::io::Result<Vec<u8>> {
    let declared = frame_content_size(frame);
    let out = match declared {
        Some(size) if size <= MAX_PREALLOCATED_DECODE_BYTES => {
            zstd::bulk::decompress(frame, prealloc_capacity(size))?
        }
        _ => {
            let mut out =
                Vec::with_capacity(prealloc_capacity(size_hint.unwrap_or(0) as u64));
            zstd::stream::copy_decode(frame, &mut out)?;
            // only trim real waste: a hint that was far too high
            if out.capacity() > out.len() + out.len() / 4 {
                out.shrink_to_fit();
            }
            out
        }
    };
    match declared {
        Some(size) if size != out.len() as u64 => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "zstd frame declares {size} decoded bytes but decoded to {}",
                out.len()
            ),
        )),
        _ => Ok(out),
    }
}

#[cfg(test)]
mod tests {
    use icechunk_macros::tokio_test;

    use super::*;
    use crate::{
        ChunkIndices, ManifestId, NodeId,
        manifest::{ChunkInfo, ChunkPayload, Manifest},
    };

    /// A buffer no serializer of ours would ever produce.
    fn unreadable_buffer(size: usize) -> Vec<u8> {
        vec![0u8; size]
    }

    #[icechunk_macros::test]
    fn write_verification_skips_buffers_below_the_threshold() {
        let data = unreadable_buffer(1024);
        verify_before_write(data.as_slice(), FileTypeBin::Manifest, data.len() + 1)
            .expect("small buffers must not be verified");
    }

    #[icechunk_macros::test]
    fn write_verification_rejects_buffers_that_cannot_be_read_back() {
        let data = unreadable_buffer(1024);
        verify_before_write(data.as_slice(), FileTypeBin::Manifest, data.len())
            .expect_err("a buffer we could not read back must not be written");
    }

    #[tokio_test]
    async fn write_verification_accepts_buffers_we_serialize()
    -> Result<(), Box<dyn std::error::Error>> {
        let manifest = Manifest::from_iter(
            &ManifestId::random(),
            vec![ChunkInfo {
                node: NodeId::random(),
                coord: ChunkIndices(vec![0]),
                payload: ChunkPayload::Inline("hello".into()),
            }],
            None,
        )
        .await?
        .ok_or("manifest is empty")?;
        // threshold 0: always verify
        verify_before_write(manifest.bytes(), FileTypeBin::Manifest, 0)?;
        Ok(())
    }

    #[icechunk_macros::test]
    fn compress_with_header_verifies_files_at_the_threshold()
    -> Result<(), Box<dyn std::error::Error>> {
        // below the threshold nothing is checked, even for a buffer we could not read back
        compress_with_header(
            unreadable_buffer(1024).as_slice(),
            SpecVersionBin::current(),
            FileTypeBin::Manifest,
            1,
        )?;

        compress_with_header(
            unreadable_buffer(VERIFY_THRESHOLD_BYTES).as_slice(),
            SpecVersionBin::current(),
            FileTypeBin::Manifest,
            1,
        )
        .map(|compressed| compressed.len())
        .expect_err("a file at the threshold must be verified before writing");
        Ok(())
    }

    #[icechunk_macros::test]
    fn decompress_frame_sizes_the_buffer_exactly_when_the_frame_says_so()
    -> Result<(), Box<dyn std::error::Error>> {
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        let with_size = zstd::bulk::compress(&data, 3)?;
        assert_eq!(frame_content_size(&with_size), Some(data.len() as u64));
        let out = decompress_frame(&with_size, None)?;
        assert_eq!(out, data);
        assert_eq!(out.capacity(), out.len(), "no slack when the size is declared");

        // a streamed frame has no declared size; a hint sizes the buffer instead
        let without_size = streamed_frame(&data)?;
        assert_eq!(frame_content_size(&without_size), None);
        let out = decompress_frame(&without_size, Some(data.len()))?;
        assert_eq!(out, data);
        assert!(out.capacity() <= data.len() + data.len() / 4, "hint prevented doubling");
        let out = decompress_frame(&without_size, None)?;
        assert_eq!(out, data);
        Ok(())
    }

    /// A frame that does not declare its decoded size: what every writer
    /// before the frame size was recorded produced.
    fn streamed_frame(data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut enc = zstd::stream::write::Encoder::new(Vec::new(), 3)?;
        enc.write_all(data)?;
        Ok(enc.finish()?)
    }

    /// Rebuild a streamed frame's header so it declares `declared` decoded
    /// bytes, keeping its compressed blocks: a frame no honest writer produces.
    /// Only frames from [`streamed_frame`] fit: no declared size, not single
    /// segment, no dictionary id.
    fn with_declared_size(frame: &[u8], declared: u64) -> Vec<u8> {
        let descriptor = frame[4];
        assert_eq!(descriptor >> 6, 0, "the frame already declares a size");
        assert_eq!(descriptor & 0x20, 0, "single segment frames carry no window byte");
        assert_eq!(descriptor & 0x03, 0, "a dictionary id would shift the size field");
        let mut out = Vec::with_capacity(frame.len() + 8);
        out.extend_from_slice(&frame[..4]);
        // top two bits of the descriptor: an 8 byte Frame_Content_Size field
        out.push(descriptor | 0xC0);
        out.push(frame[5]);
        out.extend_from_slice(&declared.to_le_bytes());
        out.extend_from_slice(&frame[6..]);
        out
    }

    #[icechunk_macros::test]
    fn a_frame_that_lies_about_its_decoded_size_is_an_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        let frame = streamed_frame(&data)?;
        assert_eq!(frame_content_size(&frame), None);
        let actual = data.len() as u64;
        // below the preallocation cap the buffer is sized from the header, above
        // it the decoder grows its own; both must reject the mismatch
        for declared in [
            actual + 1,
            actual - 1,
            actual * 10,
            MAX_PREALLOCATED_DECODE_BYTES + 1,
            u64::MAX / 2,
        ] {
            let forged = with_declared_size(&frame, declared);
            assert_eq!(frame_content_size(&forged), Some(declared));
            decompress_frame(&forged, None).err().ok_or(format!(
                "a frame declaring {declared} of {actual} bytes must not decode"
            ))?;
        }
        Ok(())
    }

    #[icechunk_macros::test]
    fn a_truncated_frame_is_an_error() -> Result<(), Box<dyn std::error::Error>> {
        let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        let declared = zstd::bulk::compress(&data, 3)?;
        assert!(decompress_frame(&declared[..declared.len() / 2], None).is_err());
        let streamed = streamed_frame(&data)?;
        assert!(decompress_frame(&streamed[..streamed.len() / 2], None).is_err());
        Ok(())
    }

    #[icechunk_macros::test]
    fn the_preallocation_is_clamped() {
        assert_eq!(prealloc_capacity(0), 0);
        assert_eq!(prealloc_capacity(1024), 1024);
        let cap = MAX_PREALLOCATED_DECODE_BYTES;
        assert_eq!(prealloc_capacity(cap), cap as usize);
        assert_eq!(prealloc_capacity(cap + 1), cap as usize);
        assert_eq!(prealloc_capacity(u64::MAX), cap as usize);
    }
}

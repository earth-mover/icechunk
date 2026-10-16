//! The repo info file: the binary header and one zstd frame holding the
//! [`RepoInfo`](crate::repo_info::RepoInfo) flatbuffer.

use chrono::{DateTime, Utc};
use icechunk_types::ICResultExt as _;

use crate::{
    ChunkId, IcechunkFormatErrorKind, IcechunkResult, REPO_INFO_FILE_PATH,
    envelope::{compress_with_header, decompress_frame},
    format_constants::{
        FileTypeBin, ICECHUNK_FILE_HEADER_LEN, SpecVersionBin, parse_file_header,
    },
    repo_info::RepoInfo,
    serializers::deserialize_repo_info,
};

/// The bytes of the repo info file for `info`.
pub fn encode(
    info: &RepoInfo,
    spec_version: SpecVersionBin,
    compression_level: u8,
) -> IcechunkResult<Vec<u8>> {
    compress_with_header(
        info.bytes(),
        spec_version,
        FileTypeBin::RepoInfo,
        compression_level,
    )
}

/// Decode and verify the bytes of a repo info file.
pub fn decode(bytes: &[u8]) -> IcechunkResult<RepoInfo> {
    let header = parse_file_header(bytes)?;
    if header.file_type != FileTypeBin::RepoInfo {
        return Err(IcechunkFormatErrorKind::InvalidFileType {
            expected: FileTypeBin::RepoInfo,
            got: header.file_type as u8,
        })
        .capture();
    }
    let body = decompress_frame(&bytes[ICECHUNK_FILE_HEADER_LEN..], None).capture()?;
    deserialize_repo_info(header.spec_version, body)
}

/// A new name, relative to `overwritten/`, for a backup of the repo info:
/// `repo.<milliseconds from now to 3000-01-01, 14 digits>.<random id>`.
pub fn new_backup_path() -> String {
    new_backup_path_of(REPO_INFO_FILE_PATH)
}

/// A name like [`new_backup_path`]'s for a backup of the file at
/// `source_path`, such as the configuration of a spec 1 repository.
pub fn new_backup_path_of(source_path: &str) -> String {
    backup_path_at(source_path, Utc::now(), &ChunkId::random())
}

fn backup_path_at(source_path: &str, now: DateTime<Utc>, id: &ChunkId) -> String {
    const LAST: u64 = 32_503_680_000_000;
    let time_index = LAST - now.timestamp_millis() as u64;
    format!("{source_path}.{time_index:014}.{id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CONFIG_FILE_PATH, IcechunkFormatErrorKind, SnapshotId,
        envelope::{binary_file_header, compress_with_header},
        format_constants::{
            CompressionAlgorithmBin, FileTypeBin, ICECHUNK_COMPRESSION_OFFSET,
            ICECHUNK_FILE_HEADER_LEN,
        },
        snapshot::SnapshotInfo,
    };

    fn a_repo_info() -> RepoInfo {
        RepoInfo::initial(
            SpecVersionBin::current(),
            SnapshotInfo {
                id: SnapshotId::random(),
                parent_id: None,
                flushed_at: DateTime::from_timestamp_micros(1_000_000).unwrap(),
                message: "initial".to_string(),
                metadata: Default::default(),
                pruned_ancestor_tx_logs: vec![],
            },
            100,
            None::<&()>,
            None,
            &[],
        )
    }

    #[icechunk_macros::test]
    fn encode_then_decode_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let info = a_repo_info();
        for level in [1, 3, 19] {
            let bytes = encode(&info, SpecVersionBin::current(), level)?;
            assert_eq!(decode(&bytes)?.bytes(), info.bytes());
            // decoding then encoding again reproduces the file
            assert_eq!(
                encode(&decode(&bytes)?, SpecVersionBin::current(), level)?,
                bytes
            );
        }
        Ok(())
    }

    #[icechunk_macros::test]
    fn encode_writes_the_repo_info_envelope() -> Result<(), Box<dyn std::error::Error>> {
        let info = a_repo_info();
        let bytes = encode(&info, SpecVersionBin::current(), 3)?;
        assert_eq!(
            &bytes[..ICECHUNK_FILE_HEADER_LEN],
            binary_file_header(
                SpecVersionBin::current(),
                FileTypeBin::RepoInfo,
                CompressionAlgorithmBin::Zstd
            )
            .as_slice()
        );
        assert_eq!(
            bytes,
            compress_with_header(
                info.bytes(),
                SpecVersionBin::current(),
                FileTypeBin::RepoInfo,
                3
            )?
        );
        Ok(())
    }

    #[icechunk_macros::test]
    fn decode_rejects_other_file_types() -> Result<(), Box<dyn std::error::Error>> {
        let info = a_repo_info();
        let bytes = compress_with_header(
            info.bytes(),
            SpecVersionBin::current(),
            FileTypeBin::Snapshot,
            3,
        )?;
        let err =
            decode(&bytes).err().ok_or("a snapshot file is not a repo info file")?;
        assert!(
            matches!(
                err.kind,
                IcechunkFormatErrorKind::InvalidFileType {
                    expected: FileTypeBin::RepoInfo,
                    got: 1
                }
            ),
            "{err}"
        );
        Ok(())
    }

    #[icechunk_macros::test]
    fn decode_rejects_uncompressed_bodies() -> Result<(), Box<dyn std::error::Error>> {
        let info = a_repo_info();
        let mut bytes = binary_file_header(
            SpecVersionBin::current(),
            FileTypeBin::RepoInfo,
            CompressionAlgorithmBin::Zstd,
        );
        bytes[ICECHUNK_COMPRESSION_OFFSET] = 0;
        bytes.extend_from_slice(info.bytes());
        let err = decode(&bytes).err().ok_or("compression 0 is not decodable")?;
        assert!(
            matches!(err.kind, IcechunkFormatErrorKind::InvalidCompressionAlgorithm),
            "{err}"
        );
        Ok(())
    }

    #[icechunk_macros::test]
    fn decode_rejects_malformed_files() -> Result<(), Box<dyn std::error::Error>> {
        let bytes = encode(&a_repo_info(), SpecVersionBin::current(), 3)?;
        // a short header, a truncated frame, a frame that isn't zstd, a body
        // that isn't a repo info
        let not_a_repo_info = compress_with_header(
            b"not a flatbuffer",
            SpecVersionBin::current(),
            FileTypeBin::RepoInfo,
            3,
        )?;
        let mut not_zstd = bytes[..ICECHUNK_FILE_HEADER_LEN].to_vec();
        not_zstd.extend_from_slice(b"garbage");
        for malformed in [
            &bytes[..ICECHUNK_FILE_HEADER_LEN - 1],
            &bytes[..bytes.len() - 4],
            not_zstd.as_slice(),
            not_a_repo_info.as_slice(),
            &[],
        ] {
            decode(malformed).err().ok_or("malformed bytes must not decode")?;
        }
        Ok(())
    }

    #[icechunk_macros::test]
    fn backup_paths_follow_the_spec() -> Result<(), Box<dyn std::error::Error>> {
        let id = ChunkId::try_from("S0CHS5WSF158RN937BP0")?;
        let now =
            DateTime::from_timestamp_millis(32_503_680_000_000 - 30_729_294_865_234)
                .ok_or("bad time")?;
        assert_eq!(
            backup_path_at(REPO_INFO_FILE_PATH, now, &id),
            "repo.30729294865234.S0CHS5WSF158RN937BP0"
        );
        // zero-padded to 14 digits
        let late =
            DateTime::from_timestamp_millis(32_503_680_000_000 - 42).ok_or("bad time")?;
        assert_eq!(
            backup_path_at(CONFIG_FILE_PATH, late, &id),
            "config.yaml.00000000000042.S0CHS5WSF158RN937BP0"
        );
        Ok(())
    }

    #[icechunk_macros::test]
    fn new_backup_paths_are_fresh_and_timed_now() -> Result<(), Box<dyn std::error::Error>>
    {
        const LAST: i64 = 32_503_680_000_000;
        let before = Utc::now().timestamp_millis();
        let paths =
            [new_backup_path(), new_backup_path(), new_backup_path_of(CONFIG_FILE_PATH)];
        let after = Utc::now().timestamp_millis();
        assert_ne!(paths[0], paths[1]);
        for (path, source) in
            paths.iter().zip([REPO_INFO_FILE_PATH, REPO_INFO_FILE_PATH, CONFIG_FILE_PATH])
        {
            let rest = path
                .strip_prefix(source)
                .and_then(|rest| rest.strip_prefix('.'))
                .ok_or(format!("{path} doesn't name {source}"))?;
            let (index, id) = rest.split_once('.').ok_or(format!("{path} has no id"))?;
            assert_eq!(index.len(), 14, "{path}");
            assert!(
                (LAST - after..=LAST - before).contains(&index.parse::<i64>()?),
                "{path}"
            );
            assert_eq!(id.len(), 20, "{path}");
            assert_eq!(ChunkId::try_from(id)?.to_string(), id, "{path}");
        }
        Ok(())
    }
}

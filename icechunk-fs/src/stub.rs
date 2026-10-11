//! The `repo` stub: a bare V2 file header of type `FilesystemStub`, so that
//! every other reader of `repo` fails with an error instead of reading a
//! stale or missing file.

use icechunk_format::format_constants::{
    CompressionAlgorithmBin, FileTypeBin, ICECHUNK_CLIENT_NAME, ICECHUNK_FILE_HEADER_LEN,
    ICECHUNK_FORMAT_MAGIC_BYTES, ICECHUNK_IMPL_NAME_LEN, SpecVersionBin,
    parse_file_header,
};

/// magic | client name padded | V2 | `FilesystemStub` | no compression
pub(crate) fn stub_bytes() -> Vec<u8> {
    let name = format!("{:<ICECHUNK_IMPL_NAME_LEN$}", *ICECHUNK_CLIENT_NAME);
    let mut bytes = Vec::with_capacity(ICECHUNK_FILE_HEADER_LEN);
    bytes.extend_from_slice(ICECHUNK_FORMAT_MAGIC_BYTES);
    bytes.extend_from_slice(&name.as_bytes()[..ICECHUNK_IMPL_NAME_LEN]);
    bytes.push(SpecVersionBin::V2 as u8);
    bytes.push(FileTypeBin::FilesystemStub as u8);
    bytes.push(CompressionAlgorithmBin::None as u8);
    bytes
}

/// Exactly one V2 file header, of type `FilesystemStub`.
pub(crate) fn is_stub(bytes: &[u8]) -> bool {
    bytes.len() == ICECHUNK_FILE_HEADER_LEN
        && parse_file_header(bytes).is_ok_and(|header| {
            header.spec_version == SpecVersionBin::V2
                && header.file_type == FileTypeBin::FilesystemStub
        })
}

#[cfg(test)]
mod tests {
    use icechunk_format::format_constants::{
        ICECHUNK_FILE_TYPE_OFFSET, ICECHUNK_SPEC_VERSION_OFFSET,
    };

    use super::*;

    #[test]
    fn stub_is_a_bare_v2_filesystem_stub_header() {
        let bytes = stub_bytes();
        assert_eq!(bytes.len(), ICECHUNK_FILE_HEADER_LEN);
        assert!(is_stub(&bytes));
        let header = parse_file_header(&bytes).unwrap();
        assert_eq!(header.file_type, FileTypeBin::FilesystemStub);
        assert_eq!(header.spec_version, SpecVersionBin::V2);
        assert_eq!(header.impl_name, *ICECHUNK_CLIENT_NAME);
    }

    #[test]
    fn other_bytes_are_not_a_stub() {
        let stub = stub_bytes();
        let mut repo_info = stub.clone();
        repo_info[ICECHUNK_FILE_TYPE_OFFSET] = FileTypeBin::RepoInfo as u8;
        let mut v1 = stub.clone();
        v1[ICECHUNK_SPEC_VERSION_OFFSET] = SpecVersionBin::V1 as u8;
        assert_eq!(
            parse_file_header(&v1).unwrap().file_type,
            FileTypeBin::FilesystemStub
        );
        let mut longer = stub.clone();
        longer.push(0);
        for bytes in [
            repo_info,
            v1,
            longer,
            stub[..stub.len() - 1].to_vec(),
            Vec::new(),
            b"not an icechunk file at all, no".to_vec(),
        ] {
            assert!(!is_stub(&bytes), "{bytes:?}");
        }
    }
}

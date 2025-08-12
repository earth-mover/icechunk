//! Path specification parsing for ZEP 8 icechunk URLs.
//!
//! Parses path specifications like:
//! - `@branch.main/data/array` -> branch='main', path='data/array'
//! - `@tag.v1.0` -> tag='v1.0', path=''
//! - `@abc123def456/nested` -> snapshot_id='abc123def456', path='nested'
//! - `data/array` -> branch='main' (default), path='data/array'

use std::fmt;

/// Type of reference in an icechunk path specification.
#[derive(Debug, Clone, PartialEq)]
pub enum ReferenceType {
    Branch,
    Tag,
    Snapshot,
}

impl fmt::Display for ReferenceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReferenceType::Branch => write!(f, "branch"),
            ReferenceType::Tag => write!(f, "tag"),
            ReferenceType::Snapshot => write!(f, "snapshot"),
        }
    }
}

/// Parsed icechunk path specification from ZEP 8 URL syntax.
#[derive(Debug, Clone)]
pub struct IcechunkPathSpec {
    pub reference_type: ReferenceType,
    pub reference_value: String,
    pub path: String,
}

impl IcechunkPathSpec {
    /// Parse a ZEP 8 URL path specification into structured components.
    ///
    /// # Arguments
    /// * `segment_path` - Path specification using ZEP 8 format
    ///
    /// # Examples
    /// ```
    /// use icechunk::zep8::{IcechunkPathSpec, ReferenceType};
    ///
    /// let spec = IcechunkPathSpec::parse("@branch.main/data/temp").unwrap();
    /// assert_eq!(spec.reference_type, ReferenceType::Branch);
    /// assert_eq!(spec.reference_value, "main");
    /// assert_eq!(spec.path, "data/temp");
    ///
    /// let spec = IcechunkPathSpec::parse("@tag.v1.0").unwrap();
    /// assert_eq!(spec.reference_type, ReferenceType::Tag);
    /// assert_eq!(spec.reference_value, "v1.0");
    /// assert_eq!(spec.path, "");
    ///
    /// let spec = IcechunkPathSpec::parse("data/array").unwrap();
    /// assert_eq!(spec.reference_type, ReferenceType::Branch);
    /// assert_eq!(spec.reference_value, "main");
    /// assert_eq!(spec.path, "data/array");
    /// ```
    pub fn parse(segment_path: &str) -> Result<Self, ParseError> {
        if segment_path.is_empty() {
            // Empty path -> default to main branch
            return Ok(IcechunkPathSpec {
                reference_type: ReferenceType::Branch,
                reference_value: "main".to_string(),
                path: "".to_string(),
            });
        }

        if !segment_path.starts_with('@') {
            // No @ prefix -> treat entire string as path, default to main branch
            return Ok(IcechunkPathSpec {
                reference_type: ReferenceType::Branch,
                reference_value: "main".to_string(),
                path: segment_path.to_string(),
            });
        }

        // Remove @ prefix and split on first /
        let ref_spec = &segment_path[1..];
        let (ref_part, path_part) = if let Some(slash_pos) = ref_spec.find('/') {
            (&ref_spec[..slash_pos], &ref_spec[slash_pos + 1..])
        } else {
            (ref_spec, "")
        };

        // Parse reference specification
        if let Some(branch_name) = ref_part.strip_prefix("branch.") {
            if branch_name.is_empty() {
                return Err(ParseError::EmptyBranchName);
            }
            Ok(IcechunkPathSpec {
                reference_type: ReferenceType::Branch,
                reference_value: branch_name.to_string(),
                path: path_part.to_string(),
            })
        } else if let Some(tag_name) = ref_part.strip_prefix("tag.") {
            if tag_name.is_empty() {
                return Err(ParseError::EmptyTagName);
            }
            Ok(IcechunkPathSpec {
                reference_type: ReferenceType::Tag,
                reference_value: tag_name.to_string(),
                path: path_part.to_string(),
            })
        } else {
            // Assume it's a snapshot ID (no prefix, must be a valid ID format)
            if !is_valid_snapshot_id(ref_part) {
                return Err(ParseError::InvalidReference(format!(
                    "Invalid reference specification: '{ref_part}'. Expected @branch.name, @tag.name, or @SNAPSHOT_ID format"
                )));
            }
            Ok(IcechunkPathSpec {
                reference_type: ReferenceType::Snapshot,
                reference_value: ref_part.to_string(),
                path: path_part.to_string(),
            })
        }
    }

    /// Get the reference type as a string.
    pub fn reference_type_str(&self) -> &str {
        match self.reference_type {
            ReferenceType::Branch => "branch",
            ReferenceType::Tag => "tag",
            ReferenceType::Snapshot => "snapshot",
        }
    }
}

impl fmt::Display for IcechunkPathSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IcechunkPathSpec({}: '{}', path: '{}')",
            self.reference_type, self.reference_value, self.path
        )
    }
}

/// Error type for path specification parsing.
#[derive(Debug, Clone)]
pub enum ParseError {
    EmptyBranchName,
    EmptyTagName,
    InvalidReference(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EmptyBranchName => {
                write!(f, "Branch name cannot be empty in @branch.name format")
            }
            ParseError::EmptyTagName => {
                write!(f, "Tag name cannot be empty in @tag.name format")
            }
            ParseError::InvalidReference(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Check if a string looks like a valid snapshot ID.
fn is_valid_snapshot_id(s: &str) -> bool {
    // Snapshot IDs should be at least 12 characters of hexadecimal
    s.len() >= 12 && s.chars().all(|c| c.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_path() {
        let spec = IcechunkPathSpec::parse("").expect("Failed to parse valid path spec");
        assert_eq!(spec.reference_type, ReferenceType::Branch);
        assert_eq!(spec.reference_value, "main");
        assert_eq!(spec.path, "");
    }

    #[test]
    fn test_parse_path_only() {
        let spec = IcechunkPathSpec::parse("data/array")
            .expect("Failed to parse valid path spec");
        assert_eq!(spec.reference_type, ReferenceType::Branch);
        assert_eq!(spec.reference_value, "main");
        assert_eq!(spec.path, "data/array");
    }

    #[test]
    fn test_parse_branch_with_path() {
        let spec = IcechunkPathSpec::parse("@branch.main/data/temp")
            .expect("Failed to parse valid path spec");
        assert_eq!(spec.reference_type, ReferenceType::Branch);
        assert_eq!(spec.reference_value, "main");
        assert_eq!(spec.path, "data/temp");
    }

    #[test]
    fn test_parse_branch_without_path() {
        let spec = IcechunkPathSpec::parse("@branch.dev")
            .expect("Failed to parse valid path spec");
        assert_eq!(spec.reference_type, ReferenceType::Branch);
        assert_eq!(spec.reference_value, "dev");
        assert_eq!(spec.path, "");
    }

    #[test]
    fn test_parse_tag_with_path() {
        let spec = IcechunkPathSpec::parse("@tag.v1.0/data")
            .expect("Failed to parse valid path spec");
        assert_eq!(spec.reference_type, ReferenceType::Tag);
        assert_eq!(spec.reference_value, "v1.0");
        assert_eq!(spec.path, "data");
    }

    #[test]
    fn test_parse_tag_without_path() {
        let spec = IcechunkPathSpec::parse("@tag.release.2024")
            .expect("Failed to parse valid path spec");
        assert_eq!(spec.reference_type, ReferenceType::Tag);
        assert_eq!(spec.reference_value, "release.2024");
        assert_eq!(spec.path, "");
    }

    #[test]
    fn test_parse_snapshot_with_path() {
        let spec = IcechunkPathSpec::parse("@abc123def456789/nested/path")
            .expect("Failed to parse valid path spec");
        assert_eq!(spec.reference_type, ReferenceType::Snapshot);
        assert_eq!(spec.reference_value, "abc123def456789");
        assert_eq!(spec.path, "nested/path");
    }

    #[test]
    fn test_parse_snapshot_without_path() {
        let spec = IcechunkPathSpec::parse("@123456789abcdef")
            .expect("Failed to parse valid path spec");
        assert_eq!(spec.reference_type, ReferenceType::Snapshot);
        assert_eq!(spec.reference_value, "123456789abcdef");
        assert_eq!(spec.path, "");
    }

    #[test]
    fn test_parse_empty_branch_error() {
        let result = IcechunkPathSpec::parse("@branch.");
        assert!(matches!(result, Err(ParseError::EmptyBranchName)));
    }

    #[test]
    fn test_parse_empty_tag_error() {
        let result = IcechunkPathSpec::parse("@tag.");
        assert!(matches!(result, Err(ParseError::EmptyTagName)));
    }

    #[test]
    fn test_parse_invalid_snapshot_error() {
        let result = IcechunkPathSpec::parse("@invalid_snapshot");
        assert!(matches!(result, Err(ParseError::InvalidReference(_))));
    }

    #[test]
    fn test_is_valid_snapshot_id() {
        assert!(is_valid_snapshot_id("abc123def456"));
        assert!(is_valid_snapshot_id("123456789abcdef0123456789"));
        assert!(is_valid_snapshot_id("ABCDEF123456"));

        assert!(!is_valid_snapshot_id("abc123")); // Too short
        assert!(!is_valid_snapshot_id("abc123xyz456")); // Invalid character
        assert!(!is_valid_snapshot_id("")); // Empty
    }

    #[test]
    fn test_display_formatting() {
        let spec = IcechunkPathSpec::parse("@branch.main/data")
            .expect("Failed to parse valid path spec");
        let display = format!("{spec}");
        assert!(display.contains("branch"));
        assert!(display.contains("main"));
        assert!(display.contains("data"));
    }
}

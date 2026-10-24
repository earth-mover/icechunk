//! Writer features: format features an implementation must implement to modify
//! a repository without damaging it. A repo info lists the ones it uses.

use std::{collections::BTreeSet, fmt};

use icechunk_types::ICResultExt as _;
use itertools::Itertools as _;
use serde::Serialize;

use crate::{IcechunkFormatErrorKind, IcechunkResult, layout_v1};

/// An Icechunk library version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IcechunkVersion {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

impl fmt::Display for IcechunkVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// The first Icechunk version that implements writer features. The status gate
/// tells users not to change the status with an older one.
pub const WRITER_FEATURES_SINCE: IcechunkVersion =
    IcechunkVersion { major: 2, minor: 4, patch: 0 };

/// A writer feature this implementation can write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WriterFeature {
    FilesystemLayoutV1,
}

/// Every writer feature this implementation recognizes, with the version that
/// first implemented it.
pub const KNOWN: &[(WriterFeature, IcechunkVersion)] = &[(
    WriterFeature::FilesystemLayoutV1,
    IcechunkVersion { major: 2, minor: 4, patch: 0 },
)];

impl WriterFeature {
    pub const fn name(self) -> &'static str {
        match self {
            WriterFeature::FilesystemLayoutV1 => layout_v1::FEATURE,
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        KNOWN.iter().map(|(feature, _)| *feature).find(|feature| feature.name() == name)
    }

    pub fn first_implemented_in(self) -> Option<IcechunkVersion> {
        KNOWN.iter().find(|(feature, _)| *feature == self).map(|(_, version)| *version)
    }
}

impl fmt::Display for WriterFeature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A writer feature name as a repo info lists it: any string, recognized or not.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct FeatureName(String);

impl FeatureName {
    /// A name exactly as read, without checking it.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// A name that follows the grammar of names defined by the specification:
    /// lowercase ASCII letters, digits and `-`, ending in `-v<n>`. Names with
    /// `.` are reserved and refused.
    pub fn parse(name: &str) -> IcechunkResult<Self> {
        if follows_spec_grammar(name) {
            Ok(Self(name.to_string()))
        } else {
            Err(IcechunkFormatErrorKind::InvalidWriterFeatureName {
                name: name.to_string(),
            })
            .capture()
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The feature this name stands for, if this implementation recognizes it.
    pub fn recognized(&self) -> Option<WriterFeature> {
        WriterFeature::from_name(&self.0)
    }
}

impl From<WriterFeature> for FeatureName {
    fn from(value: WriterFeature) -> Self {
        Self(value.name().to_string())
    }
}

impl fmt::Display for FeatureName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn follows_spec_grammar(name: &str) -> bool {
    let Some((prefix, n)) = name.rsplit_once("-v") else {
        return false;
    };
    !prefix.is_empty()
        && prefix
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !n.is_empty()
        && n.bytes().all(|b| b.is_ascii_digit())
}

/// The oldest Icechunk that implements a set of writer features.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MinVersion {
    Icechunk(IcechunkVersion),
    /// Some feature isn't recognized by this implementation.
    Newer,
}

impl fmt::Display for MinVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MinVersion::Icechunk(version) => write!(f, "Icechunk {version}"),
            MinVersion::Newer => f.write_str("a newer Icechunk"),
        }
    }
}

/// The highest "first implemented in" version among `features`, `None` when
/// there are none.
pub fn min_version<'a>(
    features: impl IntoIterator<Item = &'a FeatureName>,
) -> Option<MinVersion> {
    features
        .into_iter()
        .map(|name| {
            name.recognized()
                .and_then(WriterFeature::first_implemented_in)
                .map_or(MinVersion::Newer, MinVersion::Icechunk)
        })
        .max()
}

/// The listed features that are not in `implemented`, sorted and without
/// duplicates. A name this implementation doesn't recognize is never
/// implemented.
pub fn missing_features<'a>(
    listed: impl IntoIterator<Item = &'a FeatureName>,
    implemented: impl IntoIterator<Item = WriterFeature>,
) -> BTreeSet<FeatureName> {
    let implemented: BTreeSet<_> = implemented.into_iter().collect();
    listed
        .into_iter()
        .filter(|name| !name.recognized().is_some_and(|f| implemented.contains(&f)))
        .cloned()
        .collect()
}

/// The `limited_availability_reason` of the status gate: one line without
/// double quotes naming the features and the oldest Icechunk that implements
/// them all, followed by the repository's own reason, if any.
pub fn gate_reason(features: &[FeatureName], underlying_reason: Option<&str>) -> String {
    let names = features.iter().map(FeatureName::as_str).join(", ");
    let required = match min_version(features) {
        Some(MinVersion::Icechunk(version)) => format!("Icechunk {version} or later"),
        Some(MinVersion::Newer) | None => MinVersion::Newer.to_string(),
    };
    let mut reason = format!(
        "This repository uses writer features ({names}) that require {required} to modify it; do not change its status with Icechunk older than {WRITER_FEATURES_SINCE}."
    );
    if let Some(underlying) = underlying_reason {
        reason.push(' ');
        reason.push_str(underlying);
    }
    reason
        .chars()
        .map(|c| match c {
            '"' => '\'',
            '\n' | '\r' => ' ',
            c => c,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(names: &[&str]) -> Vec<FeatureName> {
        names.iter().map(|n| FeatureName::new(*n)).collect()
    }

    #[test]
    fn every_writer_feature_is_known_by_name() {
        for (feature, _) in KNOWN {
            assert_eq!(WriterFeature::from_name(feature.name()), Some(*feature));
            assert!(FeatureName::parse(feature.name()).is_ok());
        }
        assert_eq!(
            WriterFeature::FilesystemLayoutV1.first_implemented_in(),
            Some(IcechunkVersion { major: 2, minor: 4, patch: 0 })
        );
        assert_eq!(WriterFeature::FilesystemLayoutV1.name(), "filesystem-layout-v1");
    }

    #[test]
    fn parse_follows_the_spec_grammar() {
        for good in ["filesystem-layout-v1", "test-feature-v1", "a-v10", "x2-y-v3"] {
            assert_eq!(FeatureName::parse(good).unwrap().as_str(), good);
        }
        for bad in [
            "",
            "-v1",
            "feature",
            "feature-v",
            "feature-vx",
            "Feature-v1",
            "feature_x-v1",
            "feature-v1 ",
            "org.example.feature-v1",
            "feature.v1",
            "feature-v1.2",
        ] {
            assert!(
                matches!(
                    FeatureName::parse(bad),
                    Err(e) if matches!(e.kind, IcechunkFormatErrorKind::InvalidWriterFeatureName { .. })
                ),
                "{bad:?} must be refused"
            );
        }
    }

    #[test]
    fn missing_features_for_unknown_dotted_and_known_names() {
        let listed = names(&[
            "test-feature-v1",
            "org.example.thing-v1",
            "filesystem-layout-v1",
            "test-feature-v1",
            "Odd Name",
        ]);
        assert_eq!(
            missing_features(&listed, [WriterFeature::FilesystemLayoutV1]),
            names(&["Odd Name", "org.example.thing-v1", "test-feature-v1"])
                .into_iter()
                .collect()
        );
        assert_eq!(
            missing_features(&listed, []),
            names(&[
                "Odd Name",
                "filesystem-layout-v1",
                "org.example.thing-v1",
                "test-feature-v1"
            ])
            .into_iter()
            .collect()
        );
        assert!(
            missing_features(
                &names(&["filesystem-layout-v1"]),
                [WriterFeature::FilesystemLayoutV1]
            )
            .is_empty()
        );
        assert!(missing_features(&[], []).is_empty());
    }

    #[test]
    fn min_version_is_the_highest_and_unknown_needs_a_newer_icechunk() {
        assert_eq!(min_version(&[]), None);
        assert_eq!(
            min_version(&names(&["filesystem-layout-v1"])),
            Some(MinVersion::Icechunk(IcechunkVersion { major: 2, minor: 4, patch: 0 }))
        );
        let with_unknown = names(&["filesystem-layout-v1", "test-feature-v1"]);
        assert_eq!(min_version(&with_unknown), Some(MinVersion::Newer));
        assert_eq!(MinVersion::Newer.to_string(), "a newer Icechunk");
        assert_eq!(
            min_version(&names(&["filesystem-layout-v1"])).unwrap().to_string(),
            "Icechunk 2.4.0"
        );
    }

    #[test]
    fn gate_reason_for_one_and_several_features() {
        let one = gate_reason(&names(&["filesystem-layout-v1"]), None);
        assert_eq!(
            one,
            "This repository uses writer features (filesystem-layout-v1) that require Icechunk 2.4.0 or later to modify it; do not change its status with Icechunk older than 2.4.0."
        );

        let several = gate_reason(
            &names(&["filesystem-layout-v1", "test-feature-v1"]),
            Some("maintenance"),
        );
        assert_eq!(
            several,
            "This repository uses writer features (filesystem-layout-v1, test-feature-v1) that require a newer Icechunk to modify it; do not change its status with Icechunk older than 2.4.0. maintenance"
        );
    }

    #[test]
    fn gate_reason_is_one_line_without_double_quotes() {
        let reason = gate_reason(
            &names(&["odd\"name-v1"]),
            Some("moved to \"archive\"\nsee ticket\r\n42"),
        );
        assert!(!reason.contains('"'));
        assert!(!reason.contains('\n'));
        assert!(!reason.contains('\r'));
        assert!(reason.ends_with("moved to 'archive' see ticket  42"));
    }
}

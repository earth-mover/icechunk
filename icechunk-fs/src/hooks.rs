//! Test seams in the `repo.d` protocol: [`ProtocolHooks`].
//!
//! Its correctness depends on how concurrent writers interleave and on how
//! name operations fail, and neither can be produced reliably from outside.
//! The default methods do the real operation and never sabotage. Production
//! uses [`NoHooks`].

use std::{fmt, fs, io, path::Path};

/// The protocol steps at which [`ProtocolHooks::at`] is called.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
    not(feature = "fault-injection"),
    expect(unreachable_pub, reason = "exported with the fault-injection feature")
)]
pub enum Point {
    /// Our `stage.<id>` is written.
    Staged,
    /// Our content is linked to `<N+1>.<h>`.
    Linked,
    /// The next step is the commit rename of the marker.
    BeforeRename,
    /// Our marker rename succeeded.
    Renamed,
    /// Committed; the next step deletes the base's version file.
    BeforeTidyUp,
    /// A step of a creation.
    Creation(CreationStep),
    /// A step of a conversion.
    Conversion(ConversionStep),
}

/// The steps of a creation at which [`ProtocolHooks::at`] is called.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
    not(feature = "fault-injection"),
    expect(unreachable_pub, reason = "exported with the fault-injection feature")
)]
pub enum CreationStep {
    /// Our `repo.d.tmp.<id>` holds our first version and its marker; the
    /// next step renames it to `repo.d`.
    Built,
    /// Our directory rename succeeded: the repository exists.
    DirRenamed,
}

/// The conversions whose steps [`ProtocolHooks::at`] is called at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
    not(feature = "fault-injection"),
    expect(unreachable_pub, reason = "exported with the fault-injection feature")
)]
pub enum ConversionStep {
    /// A step of the conversion from the object store layout.
    FromObjectStore(FromObjectStoreStep),
    /// A step of the conversion to the object store layout.
    ToObjectStore(ToObjectStoreStep),
}

/// The steps of the conversion from the object store layout at which
/// [`ProtocolHooks::at`] is called.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
    not(feature = "fault-injection"),
    expect(unreachable_pub, reason = "exported with the fault-injection feature")
)]
pub enum FromObjectStoreStep {
    /// Our backup of `$ROOT/repo` is written; the next step builds our
    /// `repo.d.tmp.<id>`.
    Backup,
    /// Our `repo.d.tmp.<id>` holds the first version and its marker; the next
    /// step renames it to `repo.d`.
    Build,
    /// Our directory rename succeeded; the next step unlinks `$ROOT/repo`.
    RenameDir,
    /// `$ROOT/repo` is unlinked; the next step deletes every `repo.d.tmp.*`.
    RepoUnlinked,
}

/// The steps of the conversion to the object store layout at which
/// [`ProtocolHooks::at`] is called.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
    not(feature = "fault-injection"),
    expect(unreachable_pub, reason = "exported with the fault-injection feature")
)]
pub enum ToObjectStoreStep {
    /// The current version is read and lists the feature; the next step
    /// checks that `$ROOT/repo` doesn't exist, then makes the change. Reached
    /// again after every conflict.
    BeforeChange,
    /// Our change landed: the current version doesn't list the feature; the
    /// next step writes it to `$ROOT/repo`.
    Change,
    /// `$ROOT/repo` holds the version without the feature; the next step
    /// renames `repo.d` aside.
    RepoWritten,
    /// `repo.d` is renamed to a `repo.d.tmp.<id>`; the next step deletes
    /// every `repo.d.tmp.*`.
    RenameDir,
    /// Every `repo.d.tmp.*` is deleted.
    DeleteDir,
}

/// The ways an implementation could break the protocol. The stress tool's
/// self-test turns each one on and checks that its oracle notices.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
    not(feature = "fault-injection"),
    expect(unreachable_pub, reason = "exported with the fault-injection feature")
)]
pub enum Sabotage {
    /// Commit without publishing the bytes.
    SkipLink,
    /// Rename from the listing's current marker, not the base.
    RenameFromListedMarker,
    /// Acknowledge on `ENOENT` from the rename without listing.
    TrustRenameEnoent,
    /// Acknowledge whenever `after_failed_rename`'s listing doesn't prove the
    /// outcome, without the lost-reply check.
    AckUnproven,
    /// Acknowledge an attempt whose listing shows a marker past `N+1` without
    /// the lost-reply check: a change that didn't land is acknowledged.
    AckSupersededWithoutProof,
    /// Tidy-up also deletes the version it just committed.
    TidyUpDeletesCurrent,
    /// Tidy-up and sweep also delete version files above the marker.
    DeleteAboveMarker,
    /// Unlink `<N>.<h>` before the rename.
    UnlinkBaseBeforeRename,
    /// Rename first, link the content after.
    LinkAfterRename,
    /// On a lost race, delete the winner's `<N+1>.<x>` instead of ours.
    DeleteWinnerFile,
    /// A rerun of the conversion from the object store layout that finds
    /// `repo.d` next to `$ROOT/repo` unlinks `$ROOT/repo` without the
    /// provenance check: an object-store-layout write made after the
    /// conversion's directory rename is lost.
    FromObjectStoreWithoutProvenance,
    /// The conversion to the object store layout writes the version its
    /// listing showed, converted, without making its change: under a stale
    /// listing a change acknowledged after that version is lost.
    ToObjectStoreWithoutChange,
    /// The conversion to the object store layout renames `repo.d` aside and
    /// deletes it before writing `$ROOT/repo`: a crash after the deletion
    /// loses the repository.
    DeleteDirBeforeRepoWrite,
}

/// How a commit resolved a name operation whose outcome was in doubt, for
/// [`ProtocolHooks::observed`]. Only lost replies and concurrent writers
/// produce them, so a stress run can tell whether it reached these branches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
    not(feature = "fault-injection"),
    expect(unreachable_pub, reason = "exported with the fault-injection feature")
)]
pub enum Observed {
    /// The content `link` reported `EEXIST`: the name holds our bytes,
    /// typically our own link retried after a lost reply.
    LinkExisted,
    /// The marker rename reported `ENOENT` and the listing shows our marker:
    /// the rename landed and its reply was lost. Acknowledged.
    RenameLanded,
    /// The marker rename reported `ENOENT` and the listing shows another
    /// writer's `N+1`. A conflict.
    RenameLostRace,
    /// The marker rename reported `ENOENT` and the listing shows a marker past
    /// `N+1`: the lost-reply check decides.
    RenameSuperseded,
    /// The marker rename reported `ENOENT` and the listing doesn't settle the
    /// outcome: the lost-reply check decides.
    RenameUnproven,
}

/// The operation hooks are used only for the protocol's name operations: the
/// content link, the marker rename, deletions of version files, a creation's
/// directory rename, the directory renames of both conversions, and the link
/// and unlinks of `$ROOT/repo` by the conversions.
#[cfg_attr(
    not(feature = "fault-injection"),
    expect(unreachable_pub, reason = "exported with the fault-injection feature")
)]
pub trait ProtocolHooks: Send + Sync + fmt::Debug {
    /// A test can block a writer here, run other writers, then resume it,
    /// which checks interleavings deterministically, without sleeps.
    fn at(&self, _point: Point) {}

    /// A test can link, then report `EEXIST`, as an NFS client does when it
    /// retries a `link` whose first reply was lost.
    fn link(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::hard_link(from, to)
    }

    /// A test can rename, then report `ENOENT`, as after a lost reply.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    /// A test can fail it, or delete and report `ENOENT`.
    fn unlink(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    /// Only the stress tool's self-test turns a sabotage on.
    fn sabotaged(&self, _sabotage: Sabotage) -> bool {
        false
    }

    /// Called once for each doubtful outcome a commit resolves.
    fn observed(&self, _what: Observed) {}
}

/// The hooks used in production: every method does the real operation.
#[derive(Debug)]
pub(crate) struct NoHooks;

impl ProtocolHooks for NoHooks {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_hooks_never_sabotage() {
        for sabotage in [
            Sabotage::SkipLink,
            Sabotage::RenameFromListedMarker,
            Sabotage::TrustRenameEnoent,
            Sabotage::AckUnproven,
            Sabotage::TidyUpDeletesCurrent,
            Sabotage::DeleteAboveMarker,
            Sabotage::UnlinkBaseBeforeRename,
            Sabotage::LinkAfterRename,
            Sabotage::DeleteWinnerFile,
            Sabotage::FromObjectStoreWithoutProvenance,
            Sabotage::AckSupersededWithoutProof,
            Sabotage::ToObjectStoreWithoutChange,
            Sabotage::DeleteDirBeforeRepoWrite,
        ] {
            assert!(!NoHooks.sabotaged(sabotage), "{sabotage:?}");
        }
        for point in [
            Point::Staged,
            Point::Linked,
            Point::BeforeRename,
            Point::Renamed,
            Point::BeforeTidyUp,
            Point::Creation(CreationStep::Built),
            Point::Creation(CreationStep::DirRenamed),
            Point::Conversion(ConversionStep::FromObjectStore(
                FromObjectStoreStep::Backup,
            )),
            Point::Conversion(ConversionStep::FromObjectStore(
                FromObjectStoreStep::Build,
            )),
            Point::Conversion(ConversionStep::FromObjectStore(
                FromObjectStoreStep::RenameDir,
            )),
            Point::Conversion(ConversionStep::FromObjectStore(
                FromObjectStoreStep::RepoUnlinked,
            )),
            Point::Conversion(ConversionStep::ToObjectStore(
                ToObjectStoreStep::BeforeChange,
            )),
            Point::Conversion(ConversionStep::ToObjectStore(ToObjectStoreStep::Change)),
            Point::Conversion(ConversionStep::ToObjectStore(
                ToObjectStoreStep::RepoWritten,
            )),
            Point::Conversion(ConversionStep::ToObjectStore(
                ToObjectStoreStep::RenameDir,
            )),
            Point::Conversion(ConversionStep::ToObjectStore(
                ToObjectStoreStep::DeleteDir,
            )),
        ] {
            NoHooks.at(point);
        }
    }

    #[test]
    fn no_hooks_do_the_real_operations() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        let c = dir.path().join("c");
        fs::write(&a, b"x").unwrap();
        NoHooks.link(&a, &b).unwrap();
        assert_eq!(
            NoHooks.link(&a, &b).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        NoHooks.rename(&b, &c).unwrap();
        NoHooks.unlink(&a).unwrap();
        assert!(!a.exists() && !b.exists());
        assert_eq!(fs::read(&c).unwrap(), b"x");
    }
}

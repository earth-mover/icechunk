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
    /// outcome.
    AckUnproven,
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
    /// `N+1`: a conflict, whether or not our rename landed.
    RenameSuperseded,
    /// The marker rename reported `ENOENT` and the base is frozen.
    RenameFrozen,
    /// The marker rename reported `ENOENT` and the listing doesn't settle the
    /// outcome. A conflict.
    RenameUnproven,
}

/// The operation hooks are used only for the protocol's name operations: the
/// content link, the marker rename and deletions of version files.
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
        ] {
            assert!(!NoHooks.sabotaged(sabotage), "{sabotage:?}");
        }
        for point in [
            Point::Staged,
            Point::Linked,
            Point::BeforeRename,
            Point::Renamed,
            Point::BeforeTidyUp,
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

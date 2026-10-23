//! What to do after a run: the text printed after the summary.

use std::{path::Path, sync::LazyLock};

use crate::report::Outcome;

pub const SLACK_INVITE_URL: &str = "https://join.slack.com/t/earthmover-community/shared_invite/zt-2cwje92ir-xU3CfdG8BI~4CJOJy~sceQ";
pub const ISSUES_URL: &str = "https://github.com/earth-mover/icechunk/issues";

/// What to do next after a run with `outcome` in the run root `root`, as the
/// user gave it.
pub fn next_steps(outcome: Outcome, root: &Path) -> String {
    steps(outcome, root, "fs-stress verify")
}

/// [`next_steps`] after `qualify` in its root `root`, as the user gave it.
pub fn qualify_next_steps(outcome: Outcome, root: &Path) -> String {
    steps(outcome, root, "fs-stress qualify verify")
}

/// `verify` is the command that verifies `root`.
fn steps(outcome: Outcome, root: &Path, verify: &str) -> String {
    let root = shell_quoted(&root.display().to_string());
    let send = SEND_THE_FILE.as_str();
    let ask = |why: &str| {
        format!(
            "What to do next: {why}\n\
             \x20 1. Run `fs-stress bundle --root {root}`. It writes a .tar.gz file in \
             the current directory. The file contains this run's host names and \
             mount details.\n\
             \x20 2. {send}\n"
        )
    };
    match outcome {
        Outcome::Failed => {
            ask("the run found problems. Some of them need our review before they mean \
             that this filesystem is unsafe for filesystem_storage, so please send us \
             the results.")
        }
        Outcome::Error => {
            ask("the test itself hit an error, so this result says nothing about the \
             filesystem. Please send us the results.")
        }
        Outcome::Provisional => format!(
            "What to do next: some hosts hadn't finished. Run `{verify} --root {root}` \
             again once every host finished.\n\
             If a host stopped for good (it crashed or was killed), please send us the \
             results anyway: run `fs-stress bundle --root {root}`. {send}\n"
        ),
        Outcome::Passed => format!(
            "To share this result with us, run `fs-stress bundle --root {root}`.\n\
             {send}\n"
        ),
    }
}

static SEND_THE_FILE: LazyLock<String> = LazyLock::new(|| {
    format!(
        "Send the file in the #icechunk channel of the Earthmover community Slack \
         ({SLACK_INVITE_URL}), or in a direct message to an Icechunk developer \
         there. Or open an issue at {ISSUES_URL} without attaching the \
         file, and we'll contact you with a private way to send it."
    )
});

/// `path` as one shell word: in single quotes unless it only has characters
/// a shell leaves alone.
fn shell_quoted(path: &str) -> String {
    let plain = |c: char| c.is_ascii_alphanumeric() || "_./:@%+=,-".contains(c);
    if !path.is_empty() && path.chars().all(plain) {
        path.to_owned()
    } else {
        format!("'{}'", path.replace('\'', r"'\''"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT: &str = "/mnt/shared/stress-1";

    fn steps(outcome: Outcome) -> String {
        next_steps(outcome, Path::new(ROOT))
    }

    #[test]
    fn failures_and_errors_ask_for_the_bundle_and_say_where_to_send_it() {
        for outcome in [Outcome::Failed, Outcome::Error] {
            let text = steps(outcome);
            for expected in [
                &format!("fs-stress bundle --root {ROOT}"),
                "host names and mount details",
                "#icechunk",
                "direct message to an Icechunk developer",
                SLACK_INVITE_URL,
                ISSUES_URL,
                "without attaching the file",
            ] {
                assert!(
                    text.contains(expected),
                    "{outcome:?}: {expected} missing from {text}"
                );
            }
            assert!(!text.contains("fs-stress verify"), "{text}");
        }
        assert!(steps(Outcome::Failed).contains("need our review"));
        assert!(steps(Outcome::Error).contains("says nothing about the filesystem"));
    }

    #[test]
    fn a_provisional_result_asks_to_verify_again() {
        let text = steps(Outcome::Provisional);
        assert!(text.contains(&format!("fs-stress verify --root {ROOT}")), "{text}");
        assert!(text.contains("once every host finished"), "{text}");
        for expected in [
            "If a host stopped for good",
            &format!("run `fs-stress bundle --root {ROOT}`"),
            SEND_THE_FILE.as_str(),
        ] {
            assert!(text.contains(expected), "{expected} missing from {text}");
        }
    }

    #[test]
    fn a_pass_says_how_to_share_it_privately() {
        let text = steps(Outcome::Passed);
        assert_eq!(text.lines().count(), 2, "{text}");
        for expected in [
            &format!("fs-stress bundle --root {ROOT}"),
            "#icechunk",
            "direct message to an Icechunk developer",
            SLACK_INVITE_URL,
            ISSUES_URL,
            "without attaching the file",
        ] {
            assert!(text.contains(expected), "{expected} missing from {text}");
        }
    }

    #[test]
    fn after_qualify_verify_again_with_qualify() {
        let text = qualify_next_steps(Outcome::Provisional, Path::new(ROOT));
        assert!(
            text.contains(&format!("Run `fs-stress qualify verify --root {ROOT}` again")),
            "{text}"
        );
        for outcome in [Outcome::Passed, Outcome::Failed, Outcome::Error] {
            assert_eq!(qualify_next_steps(outcome, Path::new(ROOT)), steps(outcome));
        }
    }

    #[test]
    fn the_root_is_shown_as_given() {
        let text = next_steps(Outcome::Failed, Path::new("stress-1/../stress-2"));
        assert!(text.contains("fs-stress bundle --root stress-1/../stress-2"), "{text}");
    }

    #[test]
    fn a_root_a_shell_would_split_is_quoted() {
        let spaced = next_steps(Outcome::Failed, Path::new("/mnt/my runs/r1"));
        let expected = "fs-stress bundle --root '/mnt/my runs/r1'`";
        assert!(spaced.contains(expected), "{spaced}");
        let quoted = next_steps(Outcome::Provisional, Path::new("/mnt/bob's/r1"));
        let expected = r"fs-stress verify --root '/mnt/bob'\''s/r1'`";
        assert!(quoted.contains(expected), "{quoted}");
    }
}

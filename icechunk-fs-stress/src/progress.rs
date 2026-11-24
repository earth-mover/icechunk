//! Where a command's progress goes: all of it on stderr for the plain
//! commands; for `qualify`, the detail in a file under the run root and only
//! short lines, with the time left, on stderr.

use std::{
    fmt,
    fs::{self, OpenOptions},
    io::Write as _,
    path::PathBuf,
    time::Duration,
};

use chrono::{SecondsFormat, Utc};

/// How often `qualify` prints a short line with the time left.
pub const BRIEF_EVERY: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Progress {
    /// The detail on stderr, no short lines.
    Stderr,
    /// The detail appended to `detail`, short lines about `run` on stderr,
    /// each after `[host] ` if there is a `host`. The runs after it take
    /// `later`.
    Brief { detail: PathBuf, run: RunLabel, later: Duration, host: Option<String> },
}

impl Progress {
    /// Creates the detail file's directory; a failure only warns, as a
    /// failed append does.
    pub fn brief(
        detail: PathBuf,
        run: RunLabel,
        later: Duration,
        host: Option<String>,
    ) -> Self {
        if let Some(dir) = detail.parent()
            && let Err(err) = fs::create_dir_all(dir)
        {
            tracing::warn!("creating {}: {err}", dir.display());
        }
        Self::Brief { detail, run, later, host }
    }

    /// A line for us: on stderr, or appended to the detail file with the time.
    /// A failed append only warns: progress is no reason to stop a run.
    pub fn detail(&self, line: &str) {
        match self {
            Self::Stderr => eprintln!("{line}"),
            Self::Brief { detail, .. } => {
                let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
                let appended = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(detail)
                    .and_then(|mut file| writeln!(file, "{now} {line}"));
                if let Err(err) = appended {
                    tracing::warn!("appending to {}: {err}", detail.display());
                }
            }
        }
    }

    /// A short line for the person running `qualify`, on stderr; nothing for
    /// the plain commands.
    pub fn brief_line(&self, line: impl FnOnce(&RunLabel) -> String) {
        if let Self::Brief { run, host, .. } = self {
            eprintln!("{}", host_line(host.as_deref(), &line(run)));
        }
    }

    /// The short line with the time left of a run of `duration`, `elapsed`
    /// into it.
    pub fn time_left(&self, elapsed: Duration, duration: Duration) {
        if let Self::Brief { run, later, host, .. } = self {
            let line = time_left_line(run, *later, elapsed, duration);
            eprintln!("{}", host_line(host.as_deref(), &line));
        }
    }
}

/// `line` after `[host] `, so that the lines of several hosts in one terminal
/// tell apart; `line` alone without a `host`.
pub fn host_line(host: Option<&str>, line: &str) -> String {
    match host {
        Some(host) => format!("[{host}] {line}"),
        None => line.to_owned(),
    }
}

/// Which of the qualification's runs this is: `Run 1 of 2`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunLabel {
    /// From 1.
    pub number: usize,
    pub of: usize,
}

/// `Run 1 of 2: 2m 30s of 5m done, about 13 min left`, the time left
/// counting the runs after it, which take `later`.
fn time_left_line(
    run: &RunLabel,
    later: Duration,
    elapsed: Duration,
    duration: Duration,
) -> String {
    let elapsed = Duration::from_secs(elapsed.min(duration).as_secs());
    let left = duration - elapsed + later;
    let left = match left.as_secs().div_ceil(60) {
        0 | 1 => "less than a minute left".to_owned(),
        minutes => format!("about {minutes} min left"),
    };
    format!("{run}: {} of {} done, {left}", clock(elapsed), clock(duration))
}

impl fmt::Display for RunLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Run {} of {}", self.number, self.of)
    }
}

/// `45s`, `5m` or `2m 30s`, to the second.
pub fn clock(d: Duration) -> String {
    let (minutes, seconds) = (d.as_secs() / 60, d.as_secs() % 60);
    match (minutes, seconds) {
        (0, seconds) => format!("{seconds}s"),
        (minutes, 0) => format!("{minutes}m"),
        (minutes, seconds) => format!("{minutes}m {seconds}s"),
    }
}

/// `about 15 minutes`, rounded up.
pub fn about_minutes(d: Duration) -> String {
    match d.as_secs().div_ceil(60) {
        0 | 1 => "about a minute".to_owned(),
        minutes => format!("about {minutes} minutes"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIRST: RunLabel = RunLabel { number: 1, of: 2 };
    const LAST: RunLabel = RunLabel { number: 2, of: 2 };

    #[test]
    fn the_time_left_counts_the_later_runs_and_rounds_up() {
        let secs = Duration::from_secs;
        let (five, ten) = (secs(300), secs(600));
        let first = |elapsed| time_left_line(&FIRST, ten, elapsed, five);
        let last = |elapsed| time_left_line(&LAST, Duration::ZERO, elapsed, ten);
        assert_eq!(first(secs(150)), "Run 1 of 2: 2m 30s of 5m done, about 13 min left");
        assert_eq!(
            first(Duration::from_millis(10_900)),
            "Run 1 of 2: 10s of 5m done, about 15 min left"
        );
        assert_eq!(last(secs(530)), "Run 2 of 2: 8m 50s of 10m done, about 2 min left");
        assert_eq!(
            last(secs(590)),
            "Run 2 of 2: 9m 50s of 10m done, less than a minute left"
        );
        assert_eq!(
            last(secs(700)),
            "Run 2 of 2: 10m of 10m done, less than a minute left"
        );
    }

    #[test]
    fn durations_read_as_a_clock_and_in_minutes() {
        let secs = Duration::from_secs;
        assert_eq!(clock(secs(45)), "45s");
        assert_eq!(clock(secs(300)), "5m");
        assert_eq!(clock(secs(150)), "2m 30s");
        assert_eq!(about_minutes(secs(900)), "about 15 minutes");
        assert_eq!(about_minutes(secs(80)), "about 2 minutes");
        assert_eq!(about_minutes(secs(30)), "about a minute");
    }

    #[test]
    fn a_host_prefixes_its_lines() {
        assert_eq!(
            host_line(Some("client2"), "Run 1 of 2 started."),
            "[client2] Run 1 of 2 started."
        );
        assert_eq!(host_line(None, "Run 1 of 2 started."), "Run 1 of 2 started.");
    }

    #[test]
    fn brief_progress_appends_the_detail_to_its_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("progress").join("a.txt");
        let progress = Progress::brief(path.clone(), FIRST, Duration::ZERO, None);
        progress.detail("one");
        progress.detail("two");
        let text = fs::read_to_string(&path).unwrap();
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 2, "{text}");
        assert!(lines[0].ends_with("Z one"), "{text}");
        assert!(lines[1].ends_with("Z two"), "{text}");
    }
}

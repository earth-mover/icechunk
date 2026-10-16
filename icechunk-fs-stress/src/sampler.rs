//! The mirror sampler: a thread in each `workers` supervisor that reads
//! `$ROOT/repo` at a fixed interval and logs what it found whenever that
//! changes, to `logs/<tag>.sampler.log`. It observes the mirror
//! independently of the publications' own records: a write of `$ROOT/repo`
//! that bypasses the hooks, or a stale or torn read on a network filesystem,
//! shows up as a sample that is no version that was current.

use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use anyhow::Context as _;

use crate::{
    layout::RunRoot,
    log::{LogWriter, Record, Sample, now_ms},
    storage_mode::{REPO_KEY, payload_etag},
};

/// What one read of the mirror at `path` finds.
pub fn sample(path: &Path) -> Sample {
    match fs::read(path) {
        Ok(bytes) => Sample::Hash(payload_etag(&bytes)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Sample::Missing,
        Err(err) => Sample::Error(err.to_string()),
    }
}

/// A running sampler; [`Sampler::stop`] ends it.
#[derive(Debug)]
pub struct Sampler {
    stop: Arc<AtomicBool>,
    thread: JoinHandle<anyhow::Result<()>>,
}

impl Sampler {
    /// Starts sampling the run's mirror every `every`, logging to `tag`'s
    /// sampler log, which must not exist.
    pub fn start(root: &RunRoot, tag: &str, every: Duration) -> anyhow::Result<Self> {
        let path = root.sampler_log(tag);
        let log = LogWriter::create(&path)
            .with_context(|| format!("creating {}", path.display()))?;
        let mirror = root.storage().join(REPO_KEY);
        Ok(Self::start_with(mirror, log, every))
    }

    fn start_with(mirror: PathBuf, log: LogWriter, every: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let thread = thread::spawn({
            let stop = Arc::clone(&stop);
            move || run(&mirror, &log, every, &stop)
        });
        Self { stop, thread }
    }

    /// Stops the sampler and waits for it; `Err` if it couldn't log.
    pub fn stop(self) -> anyhow::Result<()> {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.join().map_err(|_| anyhow::anyhow!("the mirror sampler panicked"))?
    }
}

fn run(
    mirror: &Path,
    log: &LogWriter,
    every: Duration,
    stop: &AtomicBool,
) -> anyhow::Result<()> {
    let mut last = None;
    while !stop.load(Ordering::Relaxed) {
        let sample = sample(mirror);
        if last.as_ref() != Some(&sample) {
            log.append(&Record::MirrorSample { t_ms: now_ms(), sample: sample.clone() })?;
            last = Some(sample);
        }
        thread::sleep(every);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::log::read_log;

    #[test]
    fn a_sample_is_the_hash_missing_or_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let mirror = dir.path().join(REPO_KEY);
        assert_eq!(sample(&mirror), Sample::Missing);
        fs::write(&mirror, b"v1").unwrap();
        assert_eq!(sample(&mirror), Sample::Hash(payload_etag(b"v1")));
        let not_a_file = dir.path().join("dir");
        fs::create_dir(&not_a_file).unwrap();
        assert!(matches!(sample(&not_a_file), Sample::Error(_)));
    }

    /// Polls until the log holds `n` samples.
    fn samples(log: &Path, n: usize) -> Vec<Sample> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let samples: Vec<Sample> = read_log(log)
                .unwrap()
                .into_iter()
                .map(|record| match record {
                    Record::MirrorSample { sample, .. } => sample,
                    other => panic!("unexpected {other:?}"),
                })
                .collect();
            if samples.len() >= n || Instant::now() >= deadline {
                return samples;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn the_sampler_logs_each_change_once() {
        let dir = tempfile::tempdir().unwrap();
        let mirror = dir.path().join(REPO_KEY);
        let log_path = dir.path().join("h.sampler.log");
        fs::write(&mirror, b"v1").unwrap();
        let sampler = Sampler::start_with(
            mirror.clone(),
            LogWriter::create(&log_path).unwrap(),
            Duration::from_millis(1),
        );
        assert_eq!(samples(&log_path, 1), [Sample::Hash(payload_etag(b"v1"))]);
        // Many reads of the same bytes, one record.
        thread::sleep(Duration::from_millis(50));
        let staged = dir.path().join("staged");
        fs::write(&staged, b"v2").unwrap();
        fs::rename(&staged, &mirror).unwrap();
        assert_eq!(samples(&log_path, 2).len(), 2);
        fs::remove_file(&mirror).unwrap();
        let expected = [
            Sample::Hash(payload_etag(b"v1")),
            Sample::Hash(payload_etag(b"v2")),
            Sample::Missing,
        ];
        assert_eq!(samples(&log_path, 3), expected);
        sampler.stop().unwrap();
        assert_eq!(samples(&log_path, 3), expected);
    }
}

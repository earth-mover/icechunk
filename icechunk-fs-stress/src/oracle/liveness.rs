//! Whether the run made progress and exercised its faults.

use std::{collections::BTreeMap, iter};

use crate::{
    config::{PointName, RunConfig},
    log::{FaultAction, Record},
    oracle::{History, Verdict, Violation, Warning},
    supervisor::GC_SLOT,
    toggler::TOGGLER_SLOT,
};

/// The [`Violation::FaultFloor`] kind of external kills.
pub const EXTERNAL_KILL_FLOOR: &str = "external_kill";

const MAX_START_SKEW_MS: i64 = 2000;
/// A host is starved when its longest gap between acks is this many times
/// the system-wide one.
const STARVED_FACTOR: i64 = 4;

/// Checks for stalls and starved hosts, that enough faults fired, and warns
/// about start skew, stragglers and failed mirror samplers. Acks of the GC
/// worker and the status toggler don't count as progress.
pub fn check_liveness(
    history: &History,
    config: &RunConfig,
    network_fs: bool,
    verdict: &mut Verdict,
) {
    check_fault_floor(history, config, verdict);
    check_progress(history, config, network_fs, verdict);
    verdict.warnings.extend(history.done.iter().filter(|done| done.stragglers > 0).map(
        |done| Warning::Stragglers { tag: done.tag.clone(), count: done.stragglers },
    ));
    verdict.warnings.extend(history.done.iter().filter_map(|done| {
        let error = done.sampler_error.clone()?;
        Some(Warning::SamplerFailed { tag: done.tag.clone(), error })
    }));
}

/// At least one operation of the hosts' workers landed: a run whose workers
/// ran but changed nothing, as when they worked on another storage root than
/// the one checked, is a harness failure, not a pass.
pub fn check_landed(history: &History, verdict: &mut Verdict) {
    let workers: u32 = history.done.iter().map(|done| done.workers).sum();
    if workers == 0 {
        return;
    }
    let landed = history.worker_records().any(|(name, record)| {
        history.done.iter().any(|done| done.tag == name.tag)
            && matches!(record, Record::Outcome { result, .. } if result.landed().is_some())
    });
    if !landed {
        verdict.violations.push(Violation::Harness {
            detail: format!(
                "no operation of the {workers} workers landed: check that they and verify use \
                 the same storage root"
            ),
        });
    }
}

fn check_fault_floor(history: &History, config: &RunConfig, verdict: &mut Verdict) {
    let (mut kills, mut pauses, mut sabotage, mut external_kills) = (0, 0, 0, 0);
    let mut at_point: BTreeMap<PointName, u64> = BTreeMap::new();
    for record in history.logs.values().flatten() {
        match record {
            // The conversion scenario's converters, not the run's faults.
            Record::Fault {
                point,
                action: FaultAction::Kill | FaultAction::Pause,
                ..
            } if point.is_conversion_step() => {}
            Record::Fault { action: FaultAction::Kill, point, .. } => {
                kills += 1;
                *at_point.entry(*point).or_default() += 1;
            }
            Record::Fault { action: FaultAction::Pause, point, .. } => {
                pauses += 1;
                *at_point.entry(*point).or_default() += 1;
            }
            Record::Fault { action: FaultAction::Sabotage, .. } => sabotage += 1,
            Record::ExternalKill { .. } => external_kills += 1,
            _ => {}
        }
    }
    let per_point = |point| format!("per_point.{}", point_name(point));
    let stats = &mut verdict.stats;
    stats.faults.extend(
        [("in_window_kill", kills), ("pause", pauses), ("sabotage", sabotage)]
            .map(|(kind, n)| (kind.to_owned(), n)),
    );
    stats.faults.extend(at_point.iter().map(|(&point, &n)| (per_point(point), n)));
    stats.external_kills = external_kills;

    let (faults, floor) = (&config.faults, &config.fault_floor);
    let mut floors = Vec::new();
    if faults.kill_probability > 0.0 {
        floors.push(("in_window_kill".to_owned(), kills, floor.in_window_kill));
    }
    if faults.pause_probability > 0.0 {
        floors.push(("pause".to_owned(), pauses, floor.pause));
    }
    if config.external_kill_every.is_some() {
        floors.push((
            EXTERNAL_KILL_FLOOR.to_owned(),
            external_kills,
            floor.external_kill,
        ));
    }
    if faults.kill_probability > 0.0 || faults.pause_probability > 0.0 {
        floors.extend(faults.points.iter().map(|point| {
            let fired = at_point.get(point).copied().unwrap_or(0);
            (per_point(*point), fired, floor.per_point)
        }));
    }
    verdict.violations.extend(
        floors.into_iter().filter(|(_, fired, required)| fired < required).map(
            |(kind, fired, required)| Violation::FaultFloor { kind, fired, required },
        ),
    );
}

/// Stalls, start skew and starved hosts, within the window every host was
/// running: from the last go seen to the first stop.
fn check_progress(
    history: &History,
    config: &RunConfig,
    network_fs: bool,
    verdict: &mut Verdict,
) {
    let go = history.done.iter().map(|done| done.go_seen_at_ms);
    let first_stop = history.done.iter().map(|done| done.stopped_at_ms).min();
    let (Some(first_go), Some(start), Some(end)) =
        (go.clone().min(), go.max(), first_stop)
    else {
        verdict.violations.push(Violation::Harness {
            detail: "no done/ records, so progress can't be checked".to_owned(),
        });
        return;
    };
    let skew = start - first_go;
    if skew > MAX_START_SKEW_MS {
        verdict.warnings.push(Warning::StartSkew { ms: skew });
    }
    if start > end {
        verdict.violations.push(Violation::Harness {
            detail: format!("a host stopped at {end}, before the last go at {start}"),
        });
        return;
    }

    let threshold = i64::try_from(config.stall_threshold.as_millis()).unwrap_or(i64::MAX);
    let mut acks: BTreeMap<&str, Vec<i64>> = BTreeMap::new();
    for (name, record) in history.worker_records() {
        if let Record::Outcome { t_ms, result, .. } = record
            && result.landed().is_some()
            && ![GC_SLOT, TOGGLER_SLOT].contains(&name.slot.as_str())
        {
            acks.entry(&name.tag).or_default().push(*t_ms);
        }
    }

    let system = gaps(acks.values().flatten().copied(), start, end);
    for &(from_ms, to_ms) in &system {
        if to_ms - from_ms > threshold {
            if network_fs {
                verdict.warnings.push(Warning::Stall { from_ms, to_ms });
            } else {
                verdict.violations.push(Violation::Stall { from_ms, to_ms });
            }
        }
    }
    let longest = |gaps: &[(i64, i64)]| gaps.iter().map(|(from, to)| to - from).max();
    let system_longest = longest(&system).unwrap_or(0);
    for done in &history.done {
        let host_acks = acks.get(done.tag.as_str()).into_iter().flatten().copied();
        let host_longest = longest(&gaps(host_acks, start, end)).unwrap_or(0);
        if host_longest > STARVED_FACTOR.saturating_mul(system_longest)
            && host_longest > threshold
        {
            verdict.warnings.push(Warning::Starved {
                tag: done.tag.clone(),
                longest_gap_ms: host_longest,
            });
        }
    }
}

/// The stretches of `[start, end]` between consecutive acks, `start` and
/// `end` included.
fn gaps(acks: impl Iterator<Item = i64>, start: i64, end: i64) -> Vec<(i64, i64)> {
    let mut times: Vec<i64> = acks.filter(|t_ms| (start..=end).contains(t_ms)).collect();
    times.sort_unstable();
    let times: Vec<i64> = iter::once(start).chain(times).chain(iter::once(end)).collect();
    times.iter().copied().zip(times.iter().copied().skip(1)).collect()
}

fn point_name(point: PointName) -> &'static str {
    match point {
        PointName::Staged => "staged",
        PointName::Linked => "linked",
        PointName::BeforeRename => "before_rename",
        PointName::Renamed => "renamed",
        PointName::BeforeTidyUp => "before_tidy_up",
        PointName::BeforePublish => "before_publish",
        PointName::MirrorStaged => "mirror_staged",
        PointName::BeforeMirrorRename => "before_mirror_rename",
        PointName::MirrorRenamed => "mirror_renamed",
        PointName::FromObjectStoreBackup => "from_object_store_backup",
        PointName::FromObjectStoreBuild => "from_object_store_build",
        PointName::FromObjectStoreRenameDir => "from_object_store_rename_dir",
        PointName::FromObjectStorePublish => "from_object_store_publish",
        PointName::ToObjectStoreForkCheck => "to_object_store_fork_check",
        PointName::ToObjectStoreChange => "to_object_store_change",
        PointName::ToObjectStorePublish => "to_object_store_publish",
        PointName::ToObjectStoreRenameDir => "to_object_store_rename_dir",
        PointName::ToObjectStoreDeleteDir => "to_object_store_delete_dir",
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use uuid::Uuid;

    use super::*;
    use crate::{
        config::{Mode, PointName},
        env_info::HostDone,
        log::{FaultAction, LogName, OpResult, Record},
        oracle::{Violation, Warning},
        supervisor::GC_SLOT,
        toggler::TOGGLER_SLOT,
    };

    /// No faults configured, stalls above 1 s; three fault points.
    fn quiet_config() -> RunConfig {
        let mut config = RunConfig::defaults(Mode::Storage, 1);
        config.faults.points =
            vec![PointName::Linked, PointName::BeforeRename, PointName::BeforeTidyUp];
        config.faults.kill_probability = 0.0;
        config.faults.pause_probability = 0.0;
        config.external_kill_every = None;
        config.stall_threshold = Duration::from_secs(1);
        config
    }

    fn faulty_config() -> RunConfig {
        let mut config = quiet_config();
        config.faults.kill_probability = 0.01;
        config.faults.pause_probability = 0.01;
        config.external_kill_every = Some(Duration::from_secs(3));
        config
    }

    fn done(tag: &str, go_seen_at_ms: i64, stopped_at_ms: i64) -> HostDone {
        HostDone {
            tag: tag.to_owned(),
            go_seen_at_ms,
            stopped_at_ms,
            workers: 1,
            incarnations_spawned: 1,
            external_kills: 0,
            stragglers: 0,
            sampler_error: None,
        }
    }

    fn worker(tag: &str, slot: &str) -> LogName {
        LogName { tag: tag.to_owned(), slot: slot.to_owned(), incarnation: Some(0) }
    }

    fn supervisor(tag: &str) -> LogName {
        LogName { tag: tag.to_owned(), slot: "supervisor".to_owned(), incarnation: None }
    }

    fn acks(times: impl IntoIterator<Item = i64>) -> Vec<Record> {
        times
            .into_iter()
            .map(|t_ms| Record::Outcome {
                t_ms,
                id: Uuid::new_v4(),
                result: OpResult::Ack { snapshot: None },
            })
            .collect()
    }

    fn faults(action: FaultAction, points: &[PointName]) -> Vec<Record> {
        points.iter().map(|&point| Record::Fault { t_ms: 0, action, point }).collect()
    }

    fn external_kills(n: u32) -> Vec<Record> {
        (0..n)
            .map(|incarnation| Record::ExternalKill {
                t_ms: 0,
                slot: "0".to_owned(),
                incarnation,
            })
            .collect()
    }

    /// One host acking every 100 ms over 0..=1000.
    fn busy(extra: Vec<(LogName, Vec<Record>)>) -> History {
        let mut logs =
            BTreeMap::from([(worker("h", "0"), acks((0..=10).map(|i| i * 100)))]);
        for (name, records) in extra {
            logs.entry(name).or_default().extend(records);
        }
        History { logs, hosts: vec![], done: vec![done("h", 0, 1000)] }
    }

    fn check(history: &History, config: &RunConfig, network_fs: bool) -> Verdict {
        let mut verdict = Verdict::default();
        check_liveness(history, config, network_fs, &mut verdict);
        verdict
    }

    #[test]
    fn busy_run_passes() {
        let verdict = check(&busy(vec![]), &quiet_config(), false);
        assert_eq!(verdict.violations, []);
        assert_eq!(verdict.warnings, []);
    }

    #[test]
    fn stall_fails_locally_and_warns_on_a_network_fs() {
        let history = History {
            logs: BTreeMap::from([(
                worker("h", "0"),
                acks([500, 1000, 1500, 2000, 2500, 3000, 5000, 5500, 6000, 6500]),
            )]),
            hosts: vec![],
            done: vec![done("h", 0, 7000)],
        };
        let local = check(&history, &quiet_config(), false);
        assert_eq!(local.violations, [Violation::Stall { from_ms: 3000, to_ms: 5000 }]);
        assert_eq!(local.warnings, []);

        let network = check(&history, &quiet_config(), true);
        assert_eq!(network.violations, []);
        assert_eq!(network.warnings, [Warning::Stall { from_ms: 3000, to_ms: 5000 }]);
    }

    #[test]
    fn stall_includes_the_ends_of_the_window() {
        let history = History {
            logs: BTreeMap::from([(worker("h", "0"), acks([1500, 2000]))]),
            hosts: vec![],
            done: vec![done("h", 0, 4000)],
        };
        assert_eq!(
            check(&history, &quiet_config(), false).violations,
            [
                Violation::Stall { from_ms: 0, to_ms: 1500 },
                Violation::Stall { from_ms: 2000, to_ms: 4000 }
            ]
        );
    }

    #[test]
    fn stall_window_is_the_overlap_of_all_hosts() {
        // Acks outside the window, and a GC or status toggler ack inside
        // it, don't count.
        let history = History {
            logs: BTreeMap::from([
                (worker("a", "0"), acks([0, 100, 4900, 5000])),
                (worker("a", GC_SLOT), acks([2500])),
                (worker("a", TOGGLER_SLOT), acks([3000])),
            ]),
            hosts: vec![],
            done: vec![done("a", 0, 5000), done("b", 1000, 4000)],
        };
        assert_eq!(
            check(&history, &quiet_config(), false).violations,
            [Violation::Stall { from_ms: 1000, to_ms: 4000 }]
        );
    }

    #[test]
    fn fault_floor_shortfall_per_kind() {
        use FaultAction::{Kill, Pause, Sabotage};
        use PointName::{BeforeRename, BeforeTidyUp, Linked};
        let enough = || {
            vec![
                (worker("h", "0"), faults(Kill, &[Linked, BeforeRename, BeforeTidyUp])),
                (worker("h", "1"), faults(Pause, &[Linked, BeforeRename, BeforeTidyUp])),
                (supervisor("h"), external_kills(10)),
                (worker("h", "2"), faults(Sabotage, &[BeforeRename, BeforeRename])),
            ]
        };
        let verdict = check(&busy(enough()), &faulty_config(), false);
        assert_eq!(verdict.violations, []);
        assert_eq!(verdict.stats.external_kills, 10);
        assert_eq!(verdict.stats.faults["in_window_kill"], 3);
        assert_eq!(verdict.stats.faults["pause"], 3);
        assert_eq!(verdict.stats.faults["sabotage"], 2);

        let short = [
            ("in_window_kill", worker("h", "0"), faults(Kill, &[Linked, BeforeRename])),
            ("pause", worker("h", "1"), faults(Pause, &[Linked, BeforeTidyUp])),
            ("external_kill", supervisor("h"), external_kills(9)),
        ];
        for (kind, name, records) in short {
            let mut logs = enough();
            for (log, contents) in &mut logs {
                if *log == name {
                    *contents = records.clone();
                }
            }
            let required = if kind == "external_kill" { 10 } else { 3 };
            let fired = required - 1;
            let verdict = check(&busy(logs), &faulty_config(), false);
            assert_eq!(
                verdict.violations,
                [Violation::FaultFloor { kind: kind.to_owned(), fired, required }],
                "{kind}"
            );
        }

        // The conversion scenario's kills of its converters don't count.
        let mut logs = enough();
        logs[0].1 = faults(Kill, &[Linked, BeforeRename]);
        logs.push((
            worker("scenario", "to-object-store"),
            faults(Kill, &[PointName::ToObjectStoreChange]),
        ));
        let verdict = check(&busy(logs), &faulty_config(), false);
        assert_eq!(
            verdict.violations,
            [Violation::FaultFloor {
                kind: "in_window_kill".to_owned(),
                fired: 2,
                required: 3
            }]
        );
    }

    #[test]
    fn fault_floor_shortfall_per_point() {
        use FaultAction::{Kill, Pause};
        use PointName::{BeforeRename, Linked};
        let logs = vec![
            (worker("h", "0"), faults(Kill, &[Linked, Linked, BeforeRename])),
            (worker("h", "1"), faults(Pause, &[Linked, Linked, BeforeRename])),
            (supervisor("h"), external_kills(10)),
        ];
        let verdict = check(&busy(logs), &faulty_config(), false);
        assert_eq!(
            verdict.violations,
            [Violation::FaultFloor {
                kind: "per_point.before_tidy_up".to_owned(),
                fired: 0,
                required: 1
            }]
        );
    }

    #[test]
    fn fault_floor_ignores_disabled_kinds() {
        use FaultAction::Pause;
        use PointName::{BeforeRename, BeforeTidyUp, Linked};
        let verdict = check(&busy(vec![]), &quiet_config(), false);
        assert_eq!(verdict.violations, []);

        // Pauses alone still need every point.
        let mut config = quiet_config();
        config.faults.pause_probability = 0.01;
        let logs = vec![(
            worker("h", "0"),
            faults(Pause, &[Linked, BeforeRename, BeforeTidyUp]),
        )];
        let verdict = check(&busy(logs), &config, false);
        assert_eq!(verdict.violations, []);
        let logs = vec![(
            worker("h", "0"),
            faults(Pause, &[Linked, BeforeRename, BeforeRename]),
        )];
        let verdict = check(&busy(logs), &config, false);
        assert_eq!(
            verdict.violations,
            [Violation::FaultFloor {
                kind: "per_point.before_tidy_up".to_owned(),
                fired: 0,
                required: 1
            }]
        );
    }

    #[test]
    fn start_skew_warns_above_two_seconds() {
        for (skew, warned) in [(2000, false), (2001, true)] {
            let history = History {
                logs: BTreeMap::from([
                    (worker("a", "0"), acks((0..=60).map(|i| i * 100))),
                    (worker("b", "0"), acks((0..=60).map(|i| i * 100))),
                ]),
                hosts: vec![],
                done: vec![done("a", 0, 6000), done("b", skew, 6000)],
            };
            let verdict = check(&history, &quiet_config(), false);
            assert_eq!(verdict.violations, []);
            let expected =
                if warned { vec![Warning::StartSkew { ms: skew }] } else { vec![] };
            assert_eq!(verdict.warnings, expected, "{skew}");
        }
    }

    #[test]
    fn starved_host_warns() {
        let history = History {
            logs: BTreeMap::from([
                (worker("a", "0"), acks((0..=200).map(|i| i * 100))),
                (worker("b", "0"), acks([100, 19_900])),
            ]),
            hosts: vec![],
            done: vec![done("a", 0, 20_000), done("b", 0, 20_000)],
        };
        let verdict = check(&history, &quiet_config(), false);
        assert_eq!(verdict.violations, []);
        assert_eq!(
            verdict.warnings,
            [Warning::Starved { tag: "b".to_owned(), longest_gap_ms: 19_800 }]
        );

        // Below the stall threshold a gap is never starvation.
        let mut config = quiet_config();
        config.stall_threshold = Duration::from_secs(20);
        assert_eq!(check(&history, &config, false).warnings, []);
    }

    #[test]
    fn stragglers_warn_and_conflicts_do_not() {
        let conflicts = (0..300)
            .map(|_| Record::Outcome {
                t_ms: 0,
                id: Uuid::new_v4(),
                result: OpResult::Conflict,
            })
            .collect();
        let mut history = busy(vec![(worker("h", "1"), conflicts)]);
        history.done[0].stragglers = 2;
        let verdict = check(&history, &quiet_config(), false);
        assert_eq!(
            verdict.warnings,
            [Warning::Stragglers { tag: "h".to_owned(), count: 2 }]
        );
    }

    #[test]
    fn a_failed_sampler_is_a_warning() {
        let mut history = busy(vec![]);
        history.done[0].sampler_error = Some("No space left on device".to_owned());
        let verdict = check(&history, &quiet_config(), false);
        assert_eq!(verdict.violations, []);
        assert_eq!(
            verdict.warnings,
            [Warning::SamplerFailed {
                tag: "h".to_owned(),
                error: "No space left on device".to_owned()
            }]
        );
    }

    #[test]
    fn missing_done_records_are_a_harness_error() {
        let mut history = busy(vec![]);
        history.done.clear();
        assert!(matches!(
            &check(&history, &quiet_config(), false).violations[..],
            [Violation::Harness { .. }]
        ));
    }
}

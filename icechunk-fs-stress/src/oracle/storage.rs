//! Storage mode: rebuilds the chain of payloads `repo` went through from the
//! backups, and checks every logged intent against it.

use std::{
    collections::{HashMap, HashSet},
    fs, io,
    path::Path,
};

use anyhow::Context as _;
use uuid::Uuid;

use crate::{
    log::{Op, OpResult},
    oracle::{History, Verdict, Violation, Warning, intents},
    storage_mode::{BACKUP_DIR, Payload, parse_payload, payload_etag},
};

/// What the oracle keeps of a payload: everything but the padding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    pub id: Uuid,
    pub parent: Option<String>,
    pub host: String,
    pub slot: String,
    pub incarnation: u32,
    pub seq: u64,
}

impl From<Payload> for Link {
    fn from(payload: Payload) -> Self {
        let Payload { id, parent, host, slot, incarnation, seq, padding: _ } = payload;
        Self { id, parent, host, slot, incarnation, seq }
    }
}

/// The backups, without their bytes, so that memory scales with the number
/// of distinct backed-up versions rather than with the backups' size.
#[derive(Debug, Default)]
pub struct Backups {
    /// The parsable backups by etag. Many attempts back up the same base.
    pub links: HashMap<String, Link>,
    /// A `BrokenHistory` detail naming each unparsable backup file.
    pub unparsable: Vec<String>,
}

impl Backups {
    fn add(&mut self, path: &Path, bytes: &[u8]) {
        let etag = payload_etag(bytes);
        if self.links.contains_key(&etag) {
            return;
        }
        match parse_payload(bytes) {
            Ok(payload) => {
                self.links.insert(etag, payload.into());
            }
            Err(err) => {
                self.unparsable.push(format!("backup {}: {err:#}", path.display()));
            }
        }
    }
}

/// Every backup under `storage_root`, read one file at a time, skipping the
/// staging files of interrupted copies.
pub fn read_backups(storage_root: &Path) -> anyhow::Result<Backups> {
    let dir = storage_root.join(BACKUP_DIR);
    let context = || format!("listing {}", dir.display());
    let mut backups = Backups::default();
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        // No attempt got as far as a backup.
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(backups),
        Err(err) => return Err(err).with_context(context),
    };
    for entry in entries {
        let path = entry.with_context(context)?.path();
        if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().contains(".staging."))
        {
            continue;
        }
        let bytes =
            fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        backups.add(&path, &bytes);
    }
    Ok(backups)
}

/// Rebuilds the chain from `final_repo` back to the initial payload and checks
/// every intent in `history` against it. `None` if `repo` couldn't be read,
/// which the caller reports.
pub fn check_chain(
    final_repo: Option<&[u8]>,
    backups: &Backups,
    history: &History,
    network_fs: bool,
    verdict: &mut Verdict,
) {
    verdict.violations.extend(
        backups
            .unparsable
            .iter()
            .map(|detail| Violation::BrokenHistory { detail: detail.clone() }),
    );
    let head = final_repo.and_then(|bytes| {
        parse_payload(bytes)
            .map_err(|err| {
                verdict.violations.push(Violation::BrokenHistory {
                    detail: format!("final repo: {err:#}"),
                });
            })
            .ok()
            .map(|head| (payload_etag(bytes), Link::from(head)))
    });
    let chain = head
        .as_ref()
        .map(|(etag, head)| walk(etag.clone(), head, &backups.links, verdict))
        .unwrap_or_default();
    let index: HashMap<&str, usize> = chain
        .links
        .iter()
        .enumerate()
        .map(|(position, (etag, _))| (etag.as_str(), position))
        .collect();
    let predecessor =
        |position: usize| chain.links.get(position + 1).map(|(etag, _)| etag);

    let mut claimed = HashSet::new();
    for (id, (op, result, _)) in intents(history, verdict) {
        let Op::Cas { base, new } = op else {
            verdict.violations.push(Violation::Harness {
                detail: format!("storage-mode intent {id} is {op:?}"),
            });
            continue;
        };
        verdict.stats.intents += 1;
        let landed = index.get(new.as_str()).copied();
        match result {
            Some(OpResult::Ack { .. }) => {
                verdict.stats.acks += 1;
                let detail = match landed.map(predecessor) {
                    Some(Some(before)) if *before == base => None,
                    Some(Some(before)) => Some(format!(
                        "{new} landed after {before}, not after its base {base}"
                    )),
                    // Beyond the break, or right before it.
                    None | Some(None) if !chain.complete => None,
                    Some(None) => Some(format!("{new} is the initial payload")),
                    None => Some(format!("{new} is not on the chain")),
                };
                if let Some(detail) = detail {
                    verdict.violations.push(Violation::LostUpdate { id, detail });
                }
            }
            Some(OpResult::Conflict) => {
                verdict.stats.conflicts += 1;
                if landed.is_some() {
                    if network_fs {
                        verdict.warnings.push(Warning::LostReplyConflict { id });
                    } else {
                        verdict
                            .violations
                            .push(Violation::FalseConflict { id, landed_twice: false });
                    }
                }
            }
            Some(OpResult::Error { .. }) => {
                verdict.stats.errors += 1;
                if landed.is_some() {
                    verdict.warnings.push(Warning::AmbiguousErrorLanded { id });
                }
            }
            None => verdict.stats.unresolved += 1,
        }
        claimed.insert(new);
    }

    let initial = chain.complete.then(|| chain.links.len() - 1);
    for (position, (etag, payload)) in chain.links.iter().enumerate() {
        if Some(position) != initial && !claimed.contains(etag) {
            verdict.violations.push(Violation::Phantom {
                what: format!(
                    "chain element {etag} (id {}, written by {}.{}.{} as seq {})",
                    payload.id,
                    payload.host,
                    payload.slot,
                    payload.incarnation,
                    payload.seq
                ),
            });
        }
    }
    verdict.stats.chain_len = chain.links.len() as u64;
}

/// The payloads `repo` went through, the final one first.
#[derive(Debug, Default)]
struct Chain<'a> {
    links: Vec<(String, &'a Link)>,
    /// Whether the chain reaches the initial payload, which is then last.
    complete: bool,
}

/// Follows `parent` from `head` through `backups`.
fn walk<'a>(
    head_etag: String,
    head: &'a Link,
    backups: &'a HashMap<String, Link>,
    verdict: &mut Verdict,
) -> Chain<'a> {
    let mut broken =
        |detail| verdict.violations.push(Violation::BrokenHistory { detail });
    let mut seen = HashSet::from([head_etag.clone()]);
    let mut links = Vec::new();
    let (mut etag, mut payload) = (head_etag, head);
    let complete = loop {
        let next = match &payload.parent {
            None => {
                let initial = payload.id == Uuid::nil();
                if !initial {
                    broken(format!(
                        "the chain ends at {etag} (id {}), not at the initial payload",
                        payload.id
                    ));
                }
                break initial;
            }
            Some(parent) if !seen.insert(parent.clone()) => {
                broken(format!("cycle: {etag} has the earlier {parent} as its parent"));
                break false;
            }
            Some(parent) => match backups.get(parent) {
                Some(next) => (parent.clone(), next),
                None => {
                    broken(format!("no backup of {parent}, the parent of {etag}"));
                    break false;
                }
            },
        };
        links.push((etag, payload));
        (etag, payload) = next;
    };
    links.push((etag, payload));
    Chain { links, complete }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use uuid::Uuid;

    use super::*;
    use crate::{
        log::{LogName, Op, OpResult, Record},
        oracle::{Violation, Warning},
        storage_mode::payload_etag,
    };

    fn payload(id: Uuid, parent: Option<String>) -> Payload {
        Payload {
            id,
            parent,
            host: "h".to_owned(),
            slot: "0".to_owned(),
            incarnation: 0,
            seq: 0,
            padding: "xx".to_owned(),
        }
    }

    fn link(id: Uuid, parent: Option<String>) -> Link {
        Link::from(payload(id, parent))
    }

    fn initial() -> Vec<u8> {
        serde_json::to_vec(&payload(Uuid::nil(), None)).unwrap()
    }

    fn child(id: Uuid, parent: &[u8]) -> Vec<u8> {
        serde_json::to_vec(&payload(id, Some(payload_etag(parent)))).unwrap()
    }

    fn intent(id: Uuid, base: &[u8], new: &[u8]) -> Record {
        let op = Op::Cas { base: payload_etag(base), new: payload_etag(new) };
        Record::Intent { t_ms: 0, id, op }
    }

    fn outcome(id: Uuid, result: OpResult) -> Record {
        Record::Outcome { t_ms: 0, id, result }
    }

    fn ack(id: Uuid) -> Record {
        outcome(id, OpResult::Ack { snapshot: None })
    }

    fn check(
        final_repo: &[u8],
        backups: &[&[u8]],
        records: Vec<Record>,
        network_fs: bool,
    ) -> Verdict {
        let name =
            LogName { tag: "h".to_owned(), slot: "0".to_owned(), incarnation: Some(0) };
        let history =
            History { logs: BTreeMap::from([(name, records)]), ..History::default() };
        let mut indexed = Backups::default();
        for (n, bytes) in backups.iter().enumerate() {
            indexed.add(Path::new(&format!("backup-{n}")), bytes);
        }
        let backups = indexed;
        let mut verdict = Verdict::default();
        check_chain(Some(final_repo), &backups, &history, network_fs, &mut verdict);
        verdict
    }

    fn ids<const N: usize>() -> [Uuid; N] {
        std::array::from_fn(|_| Uuid::new_v4())
    }

    #[test]
    fn clean_three_update_chain_passes() {
        let [a, b, c, d] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let p2 = child(b, &p1);
        let p3 = child(c, &p2);
        let lost = child(d, &p2);
        let records = vec![
            intent(a, &p0, &p1),
            ack(a),
            intent(b, &p1, &p2),
            intent(d, &p2, &lost),
            ack(b),
            intent(c, &p2, &p3),
            outcome(d, OpResult::Conflict),
            ack(c),
        ];
        // Several attempts back up the same base.
        let verdict = check(&p3, &[&p0, &p1, &p2, &p1, &p2], records, false);
        assert_eq!(verdict.violations, []);
        assert_eq!(verdict.warnings, []);
        let stats = &verdict.stats;
        assert_eq!(
            (stats.intents, stats.acks, stats.conflicts, stats.errors, stats.unresolved),
            (4, 3, 1, 0, 0)
        );
        assert_eq!(stats.chain_len, 4);
    }

    #[test]
    fn fork_is_a_lost_update() {
        let [a, b] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let p2 = child(b, &p0);
        let records = vec![intent(a, &p0, &p1), intent(b, &p0, &p2), ack(a), ack(b)];
        let verdict = check(&p2, &[&p0, &p0], records, false);
        assert!(
            matches!(&verdict.violations[..], [Violation::LostUpdate { id, .. }] if *id == a),
            "{verdict:?}"
        );
    }

    #[test]
    fn ack_missing_from_the_chain_is_a_lost_update() {
        let [a, x] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let px = child(x, &p1);
        let records = vec![intent(a, &p0, &p1), ack(a), intent(x, &p1, &px), ack(x)];
        let verdict = check(&p1, &[&p0, &p1], records, true);
        assert!(
            matches!(&verdict.violations[..], [Violation::LostUpdate { id, .. }] if *id == x),
            "{verdict:?}"
        );
    }

    #[test]
    fn ack_after_another_predecessor_is_a_lost_update() {
        let [a, b] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let p2 = child(b, &p1);
        let records = vec![intent(a, &p0, &p1), ack(a), intent(b, &p0, &p2), ack(b)];
        let verdict = check(&p2, &[&p0, &p1], records, false);
        assert!(
            matches!(&verdict.violations[..], [Violation::LostUpdate { id, .. }] if *id == b),
            "{verdict:?}"
        );
    }

    #[test]
    fn chain_element_without_intent_is_a_phantom() {
        let [a, b] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let p2 = child(b, &p1);
        let verdict = check(&p2, &[&p0, &p1], vec![intent(b, &p1, &p2), ack(b)], false);
        assert!(
            matches!(&verdict.violations[..], [Violation::Phantom { what }]
                if what.contains(&payload_etag(&p1))),
            "{verdict:?}"
        );
    }

    #[test]
    fn conflict_that_landed_fails_only_on_a_local_fs() {
        let [a] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let records = || vec![intent(a, &p0, &p1), outcome(a, OpResult::Conflict)];

        let local = check(&p1, &[&p0], records(), false);
        assert_eq!(
            local.violations,
            [Violation::FalseConflict { id: a, landed_twice: false }]
        );
        assert_eq!(local.warnings, []);

        let network = check(&p1, &[&p0], records(), true);
        assert_eq!(network.violations, []);
        assert_eq!(network.warnings, [Warning::LostReplyConflict { id: a }]);
    }

    #[test]
    fn error_that_landed_is_a_warning() {
        let [a] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        for network_fs in [false, true] {
            let error = OpResult::Error { message: "timed out".to_owned() };
            let records = vec![intent(a, &p0, &p1), outcome(a, error)];
            let verdict = check(&p1, &[&p0], records, network_fs);
            assert_eq!(verdict.violations, []);
            assert_eq!(verdict.warnings, [Warning::AmbiguousErrorLanded { id: a }]);
            assert_eq!(verdict.stats.errors, 1);
        }
    }

    #[test]
    fn missing_backup_breaks_history() {
        let [a, b] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let p2 = child(b, &p1);
        let records = vec![intent(a, &p0, &p1), ack(a), intent(b, &p1, &p2), ack(b)];
        let verdict = check(&p2, &[&p0], records, false);
        // `a`'s ack is beyond the break, so it can't be checked.
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                if detail.contains(&payload_etag(&p1))),
            "{verdict:?}"
        );
    }

    #[test]
    fn chain_must_end_at_the_initial_payload() {
        let [a] = ids();
        let root = serde_json::to_vec(&payload(a, None)).unwrap();
        let verdict =
            check(&root, &[], vec![intent(a, &initial(), &root), ack(a)], false);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { .. }]),
            "{verdict:?}"
        );
    }

    #[test]
    fn cycle_breaks_history() {
        // Real etags can't form a cycle, so the walk gets made-up ones.
        let [a, b] = ids();
        let backups = HashMap::from([("e2".to_owned(), link(b, Some("e1".to_owned())))]);
        let mut verdict = Verdict::default();
        let head = link(a, Some("e2".to_owned()));
        let chain = walk("e1".to_owned(), &head, &backups, &mut verdict);
        assert!(!chain.complete);
        assert_eq!(chain.links.len(), 2);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                if detail.contains("e1")),
            "{verdict:?}"
        );

        let backups = HashMap::new();
        let mut verdict = Verdict::default();
        let head = link(a, Some("e1".to_owned()));
        let chain = walk("e1".to_owned(), &head, &backups, &mut verdict);
        assert!(!chain.complete);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { .. }]),
            "{verdict:?}"
        );
    }

    #[test]
    fn unparsable_payloads_break_history() {
        let [a] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let records = || vec![intent(a, &p0, &p1), ack(a)];
        let verdict = check(&p1, &[&p0, b"{\"id\":"], records(), false);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                if detail.contains("backup-1")),
            "{verdict:?}"
        );

        let verdict = check(b"garbage", &[&p0], records(), false);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { .. }]),
            "{verdict:?}"
        );
    }

    #[test]
    fn unresolved_intents_are_fine_landed_or_not() {
        let [a, x] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        let px = child(x, &p1);
        let verdict =
            check(&p1, &[&p0], vec![intent(a, &p0, &p1), intent(x, &p1, &px)], false);
        assert_eq!(verdict.violations, []);
        assert_eq!(verdict.warnings, []);
        assert_eq!(verdict.stats.unresolved, 2);
    }

    #[test]
    fn non_storage_op_is_a_harness_error() {
        let p0 = initial();
        let records = vec![Record::Intent { t_ms: 0, id: Uuid::new_v4(), op: Op::Gc }];
        let verdict = check(&p0, &[], records, false);
        assert!(
            matches!(&verdict.violations[..], [Violation::Harness { .. }]),
            "{verdict:?}"
        );
    }

    #[test]
    fn read_backups_keeps_only_links_and_skips_staging_files() {
        let dir = tempfile::tempdir().unwrap();
        let empty: Backups = read_backups(dir.path()).unwrap();
        assert!(empty.links.is_empty() && empty.unparsable.is_empty());

        let overwritten = dir.path().join("overwritten");
        fs::create_dir_all(&overwritten).unwrap();
        let [a, b, c] = ids();
        let p0 = initial();
        let p1 = child(a, &p0);
        fs::write(overwritten.join(a.to_string()), &p0).unwrap();
        fs::write(overwritten.join(b.to_string()), &p1).unwrap();
        fs::write(overwritten.join(c.to_string()), &p0).unwrap();
        fs::write(overwritten.join(format!("{a}.staging.{b}")), b"torn").unwrap();
        let garbage = Uuid::new_v4().to_string();
        fs::write(overwritten.join(&garbage), b"garbage").unwrap();

        let Backups { links, unparsable } = read_backups(dir.path()).unwrap();
        // The type holds no bytes: links carry the chain fields only.
        let links: HashMap<String, Link> = links;
        let expected = HashMap::from([
            (payload_etag(&p0), link(Uuid::nil(), None)),
            (payload_etag(&p1), link(a, Some(payload_etag(&p0)))),
        ]);
        assert_eq!(links, expected);
        assert!(
            matches!(&unparsable[..], [detail] if detail.contains(&garbage)),
            "{unparsable:?}"
        );
    }
}

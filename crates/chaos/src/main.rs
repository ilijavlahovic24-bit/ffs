//! Chaos runner CLI.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::Parser;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use chaos::cluster::Cluster;
use chaos::invariants::{check_invariants, Violation};
use chaos::metrics::Metrics;
use chaos::report::RunReport;

#[derive(Parser, Debug)]
#[command(name = "chaos")]
struct Cli {
    #[arg(long, default_value_t = 42)]
    seed: u64,
    #[arg(long, default_value_t = 30)]
    duration: u64,
    #[arg(long, default_value_t = 3)]
    nodes: u32,
    #[arg(long, default_value_t = 10)]
    tick_ms: u64,
    #[arg(long, default_value_t = 1)]
    kill_probability: u32,
    #[arg(long, default_value_t = 5)]
    restart_probability: u32,
    #[arg(long, default_value = "/tmp/ferumfs-chaos.json")]
    report_json: PathBuf,
    #[arg(long, default_value = "/tmp/ferumfs-chaos.md")]
    report_md: PathBuf,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();

    let cli = Cli::parse();

    // Watchdog: force-exit if the main loop exceeds duration + grace.
    let hard_deadline = Duration::from_secs(cli.duration + 30);
    std::thread::spawn(move || {
        std::thread::sleep(hard_deadline);
        eprintln!("chaos: watchdog fired, force-exiting");
        std::process::exit(2);
    });

    let mut rng = StdRng::seed_from_u64(cli.seed);

    let ids: Vec<u64> = (1..=cli.nodes as u64).collect();
    let mut cluster = Cluster::new(&ids);
    let metrics = Metrics::new();

    println!(
        "chaos: seed={} duration={}s nodes={} tick={}ms",
        cli.seed, cli.duration, cli.nodes, cli.tick_ms
    );
    std::io::Write::flush(&mut std::io::stdout()).ok();

    if cluster.wait_for_leader_fast(1000).is_none() {
        eprintln!("chaos: no initial leader elected; aborting");
        std::process::exit(1);
    }
    metrics.inc_election();

    let deadline = Instant::now() + Duration::from_secs(cli.duration);
    let mut all_violations: Vec<Violation> = Vec::new();
    let mut data_loss_incidents: u64 = 0;
    let mut split_brain_incidents: u64 = 0;

    let mut next_proposal_at = Instant::now();

    // Track leader transitions and commit progress.
    let mut last_leader: Option<u64> = cluster.leader();
    let mut last_commit_index: u64 = last_leader
        .and_then(|id| cluster.commit_index(id))
        .unwrap_or(0);
    let mut election_started: Option<Instant> = None;

    // Propose → commit latency: index → timestamp.
    let mut proposed_at: HashMap<u64, Instant> = HashMap::new();

    // Monotonic set of entries that were ever committed. Once here,
    // the entry must survive forever.
    let mut ever_committed: HashMap<u64, u64> = HashMap::new();

    // Deduplicate loss reports.
    let mut reported_losses: HashSet<(u64, u64)> = HashSet::new();

    while Instant::now() < deadline {
        cluster.tick_all();

        // --- Fault injection ------------------------------------------
        if rng.gen_range(0..100) < cli.kill_probability {
            let live = cluster.live_ids();
            if live.len() > 1 {
                let victim = live[rng.gen_range(0..live.len())];
                cluster.kill(victim);
                metrics.inc_kill();
            }
        }

        if rng.gen_range(0..100) < cli.restart_probability {
            let dead: Vec<u64> = ids
                .iter()
                .copied()
                .filter(|id| !cluster.is_live(*id))
                .collect();
            if let Some(&victim) = dead.first() {
                cluster.restart(victim);
                metrics.inc_restart();
            }
        }

        // --- Detect leader transition ---------------------------------
        let current_leader = cluster.leader();
        if current_leader != last_leader {
            match (last_leader, current_leader) {
                (Some(_), None) => {
                    election_started = Some(Instant::now());
                }
                (_, Some(_)) => {
                    metrics.inc_election();
                    if let Some(started) = election_started.take() {
                        metrics.record_election(started.elapsed());
                    }
                }
                _ => {}
            }
            last_leader = current_leader;
        }

        // --- Detect commit progress -----------------------------------
        if let Some(leader) = current_leader {
            if let Some(ci) = cluster.commit_index(leader) {
                if ci > last_commit_index {
                    for idx in (last_commit_index + 1)..=ci {
                        metrics.inc_commit();
                        if let Some(started) = proposed_at.remove(&idx) {
                            metrics.record_propose(started.elapsed());
                        }
                    }
                    last_commit_index = ci;
                }
            }
        }

        // --- Client proposals -----------------------------------------
        if Instant::now() >= next_proposal_at {
            let payload = format!("cmd-{}", rng.gen::<u32>()).into_bytes();
            if let Some((_, idx)) = cluster.propose(payload) {
                metrics.inc_proposal();
                proposed_at.insert(idx, Instant::now());
            }
            next_proposal_at = Instant::now() + Duration::from_millis(50);
        }

        // --- Accumulate ever-committed entries ------------------------
        for id in cluster.live_ids() {
            if let Some(node) = cluster.node(id) {
                for idx in 1..=node.commit_index {
                    if let Some(entry) = node.log.get(idx) {
                        ever_committed.entry(idx).or_insert(entry.term);
                    }
                }
            }
        }

        // --- Invariants -----------------------------------------------
        let v = check_invariants(&cluster);
        if !v.is_empty() {
            for violation in &v {
                if violation.kind == "election_safety" {
                    split_brain_incidents += 1;
                }
                metrics.inc_violation();
                all_violations.push(violation.clone());
            }
        }

        // --- Data loss check (correct) --------------------------------
        // Every entry that was ever committed must be present on SOME
        // node (live or dead — kill preserves the log, restart
        // preserves durable state).
        for (idx, term) in &ever_committed {
            let still_present = ids.iter().any(|&id| {
                cluster
                    .node(id)
                    .and_then(|n| n.log.get(*idx))
                    .map(|e| e.term == *term)
                    .unwrap_or(false)
            });
            if !still_present && reported_losses.insert((*idx, *term)) {
                data_loss_incidents += 1;
                all_violations.push(Violation {
                    kind: "data_loss".into(),
                    details: format!("entry ({idx}, {term}) missing from all nodes"),
                });
            }
        }

        tokio::time::sleep(Duration::from_millis(cli.tick_ms)).await;
    }

    let snapshot = metrics.snapshot();
    let report = RunReport {
        seed: cli.seed,
        duration_sec: cli.duration,
        nodes: cli.nodes,
        metrics: snapshot,
        violations: all_violations,
        data_loss_incidents,
        split_brain_incidents,
    };

    std::fs::write(&cli.report_json, report.to_json())?;
    std::fs::write(&cli.report_md, report.to_markdown())?;

    println!("\n{}", report.to_markdown());
    println!("JSON: {}", cli.report_json.display());
    println!("MD:   {}", cli.report_md.display());

    std::process::exit(0);
}
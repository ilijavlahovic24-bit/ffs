//! Run report — captures metrics and violations, emits JSON + Markdown.

use serde::Serialize;

use crate::invariants::Violation;
use crate::metrics::MetricsSnapshot;

#[derive(Debug, Clone, Serialize)]
pub struct RunReport {
    pub seed: u64,
    pub duration_sec: u64,
    pub nodes: u32,
    pub metrics: MetricsSnapshot,
    pub violations: Vec<Violation>,
    pub data_loss_incidents: u64,
    pub split_brain_incidents: u64,
}

impl RunReport {
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".into())
    }

    pub fn to_markdown(&self) -> String {
        let m = &self.metrics;
        let mut s = String::new();
        s.push_str("# FerumFS chaos run\n\n");
        s.push_str(&format!("- **Seed:** {}\n", self.seed));
        s.push_str(&format!("- **Duration:** {}s\n", self.duration_sec));
        s.push_str(&format!("- **Nodes:** {}\n\n", self.nodes));

        s.push_str("## Faults\n\n");
        s.push_str(&format!("- Kills: {}\n", m.kills));
        s.push_str(&format!("- Restarts: {}\n", m.restarts));
        s.push_str(&format!("- Elections: {}\n\n", m.elections));

        s.push_str("## Client operations\n\n");
        s.push_str(&format!("- Proposals: {}\n", m.proposals));
        s.push_str(&format!("- Commits: {}\n", m.commits));
        s.push_str(&format!(
            "- Propose P50/P99: {}ms / {}ms\n\n",
            m.propose_p50_ms, m.propose_p99_ms
        ));

        s.push_str("## Election timing\n\n");
        s.push_str(&format!(
            "- P50/P99: {}ms / {}ms\n\n",
            m.election_p50_ms, m.election_p99_ms
        ));

        s.push_str("## Invariants\n\n");
        s.push_str(&format!("- Violations: {}\n", self.violations.len()));
        s.push_str(&format!(
            "- Data loss incidents: {}\n",
            self.data_loss_incidents
        ));
        s.push_str(&format!(
            "- Split-brain incidents: {}\n\n",
            self.split_brain_incidents
        ));

        if !self.violations.is_empty() {
            s.push_str("### Violation details\n\n");
            for v in &self.violations {
                s.push_str(&format!("- **{}**: {}\n", v.kind, v.details));
            }
        }

        s
    }
}
use serde::{Deserialize, Serialize};

// One entry in the Raft log.
//
// `command` is opaque bytes — Raft doesn't know what's inside.
// In FerumFS it is a serialized `WalOperation` (or group of operations).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
}
#[derive(Debug, Clone)]
pub struct RaftLog {
    // Entries are 1-indexed per Raft specification.
    // `entries[0]` is sentinel for index 0 (empyt),
    // `entries[i].index == i`.
    // Simpliefied `get(i)`.
    entries: Vec<LogEntry>,
}
impl RaftLog {
    pub fn new() -> Self {
        // sentinel wit index 0
        Self {
            entries: vec![LogEntry {
                term: 0,
                index: 0,
                command: Vec::new(),
            }],
        }
    }
    pub fn append(&mut self, mut entry: LogEntry) {
        entry.index = self.entries.len() as u64;
        self.entries.push(entry);
    }

    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        self.entries.get(index as usize)
    }

    pub fn last_index(&self) -> u64 {
        (self.entries.len() - 1) as u64
    }

    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn term_at(&self, index: u64) -> u64 {
        self.get(index).map(|e| e.term).unwrap_or(0)
    }
    //Removes all from index including index. Cannot delete sentinel on 0.
    pub fn truncate_from(&mut self, index: u64) {
        if index == 0 {
            return;
        }
        self.entries.truncate(index as usize);
    }

    /// Vrati sve entry-je od `index` (uključujući).
    pub fn slice_from(&self, index: u64) -> &[LogEntry] {
        let i = index as usize;
        if i >= self.entries.len() {
            &[]
        } else {
            &self.entries[i..]
        }
    }

    pub fn len(&self) -> u64 {
        self.last_index()
    }

    pub fn is_empty(&self) -> bool {
        self.last_index() == 0
    }
}

impl Default for RaftLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_get() {
        let mut log = RaftLog::new();
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);

        log.append(LogEntry { term: 1, index: 0, command: vec![1] });
        log.append(LogEntry { term: 1, index: 0, command: vec![2] });
        log.append(LogEntry { term: 2, index: 0, command: vec![3] });

        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.get(1).unwrap().command, vec![1]);
        assert_eq!(log.get(3).unwrap().term, 2);
        assert!(log.get(4).is_none());
    }

    #[test]
    fn truncate_preserves_sentinel() {
        let mut log = RaftLog::new();
        log.append(LogEntry { term: 1, index: 0, command: vec![1] });
        log.append(LogEntry { term: 2, index: 0, command: vec![2] });
        log.truncate_from(1);
        assert_eq!(log.last_index(), 0);
        log.truncate_from(0);
        assert_eq!(log.last_index(), 0);
    }

    #[test]
    fn slice_from_index() {
        let mut log = RaftLog::new();
        for i in 0..5 {
            log.append(LogEntry { term: 1, index: 0, command: vec![i] });
        }
        assert_eq!(log.slice_from(2).len(), 4);
        assert_eq!(log.slice_from(10).len(), 0);
    }
}
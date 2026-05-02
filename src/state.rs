use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct State {
    pub tokens: Tokens,
    #[serde(default)]
    pub cursors: Cursors,
    #[serde(default)]
    pub lifetime_counters: LifetimeCounters,
    #[serde(default)]
    pub finalized_days_emitted: BTreeSet<String>,
    #[serde(default)]
    pub emitted_record_ids: EmittedIds,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tokens {
    pub access_token: String,
    pub refresh_token: String,
    /// Unix timestamp (seconds) at which the access token expires.
    pub expires_at: i64,
    pub scope: String,
    pub userid: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Cursors {
    #[serde(default)]
    pub measure: i64,
    #[serde(default)]
    pub activity: i64,
    #[serde(default)]
    pub workouts: i64,
    #[serde(default)]
    pub sleep: i64,
    #[serde(default)]
    pub intraday: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct LifetimeCounters {
    #[serde(default)]
    pub steps_total: u64,
    #[serde(default)]
    pub distance_meters_total: f64,
    #[serde(default)]
    pub active_calories_kcal_total: f64,
    /// `YYYY-MM-DD` of today (per user tz) the last `last_partial_*` were captured for.
    #[serde(default)]
    pub last_partial_day: Option<String>,
    #[serde(default)]
    pub last_partial_steps: u64,
    #[serde(default)]
    pub last_partial_distance_meters: f64,
    #[serde(default)]
    pub last_partial_calories_kcal: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct EmittedIds {
    #[serde(default)]
    pub sleep: Vec<EmittedIdEntry>,
    #[serde(default)]
    pub workouts: Vec<EmittedIdEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmittedIdEntry {
    pub id: i64,
    /// Unix seconds when first emitted (for pruning).
    pub emitted_at: i64,
}

pub fn load(path: &Path) -> Result<State> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read state file {}", path.display()))?;
    let state: State = serde_json::from_str(&raw)
        .with_context(|| format!("parse state file {}", path.display()))?;
    Ok(state)
}

pub fn save(path: &Path, state: &State) -> Result<()> {
    let tmp_path = tmp_path(path);
    let json = serde_json::to_vec_pretty(state).context("serialize state")?;
    std::fs::write(&tmp_path, &json)
        .with_context(|| format!("write tmp state {}", tmp_path.display()))?;
    // fsync the tmp file before rename to guarantee durability.
    let f = std::fs::File::open(&tmp_path)?;
    f.sync_all()?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} -> {}", tmp_path.display(), path.display()))?;
    Ok(())
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Drop emitted-ID entries older than `max_age_secs`.
pub fn prune_emitted_ids(ids: &mut Vec<EmittedIdEntry>, now_secs: i64, max_age_secs: i64) {
    ids.retain(|e| now_secs - e.emitted_at <= max_age_secs);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_state() -> State {
        State {
            tokens: Tokens {
                access_token: "atk".into(),
                refresh_token: "rtk".into(),
                expires_at: 1_700_000_000,
                scope: "user.metrics".into(),
                userid: "12345".into(),
            },
            cursors: Cursors {
                measure: 1,
                activity: 2,
                workouts: 3,
                sleep: 4,
                intraday: 5,
            },
            lifetime_counters: LifetimeCounters::default(),
            finalized_days_emitted: BTreeSet::new(),
            emitted_record_ids: EmittedIds::default(),
        }
    }

    #[test]
    fn round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        let s = sample_state();
        save(&path, &s).unwrap();
        let loaded = load(&path).unwrap();
        assert_eq!(loaded, s);
    }

    #[test]
    fn save_is_atomic_no_partial() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        save(&path, &sample_state()).unwrap();
        assert!(path.exists());
        assert!(!dir.path().join("state.json.tmp").exists());
    }

    #[test]
    fn load_missing_file_errors() {
        let dir = tempdir().unwrap();
        let err = load(&dir.path().join("nope.json")).unwrap_err();
        assert!(err.to_string().contains("read state file"));
    }

    #[test]
    fn prunes_old_emitted_ids() {
        let mut ids = vec![
            EmittedIdEntry { id: 1, emitted_at: 499 },
            EmittedIdEntry { id: 2, emitted_at: 500 },
            EmittedIdEntry { id: 3, emitted_at: 1_000 },
        ];
        prune_emitted_ids(&mut ids, 1_000, 500);
        assert_eq!(ids.iter().map(|e| e.id).collect::<Vec<_>>(), vec![2, 3]);
    }

    #[test]
    fn missing_optional_fields_default() {
        let json = r#"{
            "tokens": {
                "access_token": "a", "refresh_token": "r",
                "expires_at": 1, "scope": "s", "userid": "1"
            }
        }"#;
        let s: State = serde_json::from_str(json).unwrap();
        assert_eq!(s.cursors, Cursors::default());
        assert!(s.finalized_days_emitted.is_empty());
    }
}

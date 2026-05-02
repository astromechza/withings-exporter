use anyhow::{Context, Result};
use jiff::Timestamp;

use crate::config::Config;
use crate::metrics::Instruments;
use crate::state::{EmittedIdEntry, State};
use crate::withings::api::{
    activity::ActivityBody, intraday::IntradayBody, measure::MeasureBody, sleep::SleepBody,
    unwrap_envelope, workouts::WorkoutsBody,
};
use crate::withings::client::WithingsClient;

const MEASTYPES: &str = "1,5,6,8,11,54,71,73,76,77,88";
const ACTIVITY_FIELDS: &str = "steps,distance,calories,totalcalories";
const SLEEP_FIELDS: &str = "lightsleepduration,deepsleepduration,remsleepduration,wakeupduration,wakeupcount,durationtosleep,durationtowakeup,hr_average,hr_min,hr_max";
const WORKOUT_FIELDS: &str = "calories,intensity,manual_distance,manual_calories,hr_average,hr_min,hr_max,hr_zone_0,hr_zone_1,hr_zone_2,hr_zone_3,pause_duration,steps,distance,elevation,spo2_average";
const INTRADAY_FIELDS: &str = "steps,elevation,calories,distance,duration,heart_rate,spo2_auto";

const EMITTED_ID_TTL_SECS: i64 = 90 * 24 * 3600;

pub async fn run_poll(
    cfg: &Config,
    client: &WithingsClient,
    inst: &Instruments,
    state: &mut State,
) -> Result<()> {
    let now = Timestamp::now().as_second();

    // Body measurements
    let raw = client
        .post_data(
            "/measure",
            &[
                ("action", "getmeas".into()),
                ("meastypes", MEASTYPES.into()),
                (
                    "lastupdate",
                    since_or_backfill(state.cursors.measure, cfg, now).to_string(),
                ),
            ],
        )
        .await
        .context("getmeas")?;
    let body: MeasureBody = unwrap_envelope(&raw)?;
    inst.record_body_measures(&body.measuregrps);
    if let Some(max) = body.measuregrps.iter().map(|g| g.modified).max() {
        state.cursors.measure = state.cursors.measure.max(max);
    }

    // Workouts
    let raw = client
        .post_data(
            "/v2/measure",
            &[
                ("action", "getworkouts".into()),
                ("data_fields", WORKOUT_FIELDS.into()),
                (
                    "lastupdate",
                    since_or_backfill(state.cursors.workouts, cfg, now).to_string(),
                ),
            ],
        )
        .await
        .context("getworkouts")?;
    let body: WorkoutsBody = unwrap_envelope(&raw)?;
    let to_emit: Vec<_> = body
        .series
        .iter()
        .filter(|w| !id_already_emitted(&state.emitted_record_ids.workouts, w.id))
        .cloned()
        .collect();
    inst.record_workouts(&to_emit);
    for w in &to_emit {
        state.emitted_record_ids.workouts.push(EmittedIdEntry {
            id: w.id,
            emitted_at: now,
        });
    }
    if let Some(max) = body.series.iter().map(|w| w.modified).max() {
        state.cursors.workouts = state.cursors.workouts.max(max);
    }
    crate::state::prune_emitted_ids(
        &mut state.emitted_record_ids.workouts,
        now,
        EMITTED_ID_TTL_SECS,
    );

    // Sleep summary
    let raw = client
        .post_data(
            "/v2/sleep",
            &[
                ("action", "getsummary".into()),
                ("data_fields", SLEEP_FIELDS.into()),
                (
                    "lastupdate",
                    since_or_backfill(state.cursors.sleep, cfg, now).to_string(),
                ),
            ],
        )
        .await
        .context("getsleep")?;
    let body: SleepBody = unwrap_envelope(&raw)?;
    let to_emit: Vec<_> = body
        .series
        .iter()
        .filter(|n| !id_already_emitted(&state.emitted_record_ids.sleep, n.id))
        .cloned()
        .collect();
    inst.record_sleep(&to_emit);
    for n in &to_emit {
        state.emitted_record_ids.sleep.push(EmittedIdEntry {
            id: n.id,
            emitted_at: now,
        });
    }
    if let Some(max) = body.series.iter().map(|n| n.modified).max() {
        state.cursors.sleep = state.cursors.sleep.max(max);
    }
    crate::state::prune_emitted_ids(
        &mut state.emitted_record_ids.sleep,
        now,
        EMITTED_ID_TTL_SECS,
    );

    // Daily activity
    let today_str = today_in_tz(&cfg.user_tz)?;
    let raw = client
        .post_data(
            "/v2/measure",
            &[
                ("action", "getactivity".into()),
                ("data_fields", ACTIVITY_FIELDS.into()),
                (
                    "lastupdate",
                    since_or_backfill(state.cursors.activity, cfg, now).to_string(),
                ),
            ],
        )
        .await
        .context("getactivity")?;
    let body: ActivityBody = unwrap_envelope(&raw)?;
    apply_activity(state, inst, &body, &today_str);
    if let Some(max) = body.activities.iter().map(|a| a.modified).max() {
        state.cursors.activity = state.cursors.activity.max(max);
    }

    // Intraday (1h cursor overlap for late-syncing samples)
    let start = if state.cursors.intraday > 0 {
        state.cursors.intraday - 3600
    } else {
        now - cfg.backfill_days * 86400
    };
    let raw = client
        .post_data(
            "/v2/measure",
            &[
                ("action", "getintradayactivity".into()),
                ("data_fields", INTRADAY_FIELDS.into()),
                ("startdate", start.to_string()),
                ("enddate", now.to_string()),
            ],
        )
        .await
        .context("getintraday")?;
    let body: IntradayBody = unwrap_envelope(&raw)?;
    inst.record_intraday(&body);
    if let Some(max) = body.samples_sorted().iter().map(|(t, _)| *t).max() {
        state.cursors.intraday = state.cursors.intraday.max(max);
    }

    Ok(())
}

fn since_or_backfill(cursor: i64, cfg: &Config, now: i64) -> i64 {
    if cursor > 0 {
        cursor
    } else {
        now - cfg.backfill_days * 86400
    }
}

fn id_already_emitted(set: &[EmittedIdEntry], id: i64) -> bool {
    set.iter().any(|e| e.id == id)
}

fn today_in_tz(tz: &str) -> Result<String> {
    let z = jiff::tz::TimeZone::get(tz).context("parse user_tz")?;
    let zoned = jiff::Zoned::now().with_time_zone(z);
    Ok(zoned.date().to_string())
}

fn apply_activity(state: &mut State, inst: &Instruments, body: &ActivityBody, today: &str) {
    use std::collections::HashMap;
    let by_date: HashMap<&str, &crate::withings::api::activity::DailyActivity> = body
        .activities
        .iter()
        .map(|a| (a.date.as_str(), a))
        .collect();

    if let Some(today_act) = by_date.get(today) {
        inst.record_activity_totals_for_today(today_act);
        let prev_steps = if state.lifetime_counters.last_partial_day.as_deref() == Some(today) {
            state.lifetime_counters.last_partial_steps
        } else {
            0
        };
        let prev_dist = if state.lifetime_counters.last_partial_day.as_deref() == Some(today) {
            state.lifetime_counters.last_partial_distance_meters
        } else {
            0.0
        };
        let prev_cal = if state.lifetime_counters.last_partial_day.as_deref() == Some(today) {
            state.lifetime_counters.last_partial_calories_kcal
        } else {
            0.0
        };
        let cur_steps = today_act.steps.unwrap_or(0);
        let cur_dist = today_act.distance.unwrap_or(0.0);
        let cur_cal = today_act.calories.unwrap_or(0.0);
        if cur_steps >= prev_steps {
            state.lifetime_counters.steps_total += cur_steps - prev_steps;
        }
        if cur_dist >= prev_dist {
            state.lifetime_counters.distance_meters_total += cur_dist - prev_dist;
        }
        if cur_cal >= prev_cal {
            state.lifetime_counters.active_calories_kcal_total += cur_cal - prev_cal;
        }
        state.lifetime_counters.last_partial_day = Some(today.to_string());
        state.lifetime_counters.last_partial_steps = cur_steps;
        state.lifetime_counters.last_partial_distance_meters = cur_dist;
        state.lifetime_counters.last_partial_calories_kcal = cur_cal;
    }

    for a in &body.activities {
        if a.date.as_str() == today {
            continue;
        }
        if state.finalized_days_emitted.contains(&a.date) {
            continue;
        }
        let steps = a.steps.unwrap_or(0);
        let dist = a.distance.unwrap_or(0.0);
        let cal = a.calories.unwrap_or(0.0);
        inst.record_finalized_day(&a.date, steps, dist, cal);
        state.lifetime_counters.steps_total += steps;
        state.lifetime_counters.distance_meters_total += dist;
        state.lifetime_counters.active_calories_kcal_total += cal;
        state.finalized_days_emitted.insert(a.date.clone());
    }

    inst.record_lifetime(
        state.lifetime_counters.steps_total,
        state.lifetime_counters.distance_meters_total,
        state.lifetime_counters.active_calories_kcal_total,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{EmittedIds, LifetimeCounters, Tokens};
    use std::collections::BTreeSet;

    #[allow(dead_code)]
    fn empty_state() -> State {
        State {
            tokens: Tokens {
                access_token: "a".into(),
                refresh_token: "r".into(),
                expires_at: i64::MAX,
                scope: "".into(),
                userid: "1".into(),
            },
            cursors: Default::default(),
            lifetime_counters: LifetimeCounters::default(),
            finalized_days_emitted: BTreeSet::new(),
            emitted_record_ids: EmittedIds::default(),
        }
    }

    #[test]
    fn since_or_backfill_uses_backfill_when_zero() {
        let cfg = Config {
            client_id: "".into(),
            client_secret: "".into(),
            state_path: "/tmp/x".into(),
            otlp_endpoint: "".into(),
            backfill_days: 30,
            user_tz: "UTC".into(),
            user_agent: "".into(),
        };
        let now = 1_700_000_000;
        assert_eq!(since_or_backfill(0, &cfg, now), now - 30 * 86400);
        assert_eq!(since_or_backfill(123, &cfg, now), 123);
    }

    #[test]
    fn id_emitted_check() {
        let s = vec![EmittedIdEntry {
            id: 1,
            emitted_at: 0,
        }];
        assert!(id_already_emitted(&s, 1));
        assert!(!id_already_emitted(&s, 2));
    }

    #[test]
    fn today_in_tz_parses() {
        let s = today_in_tz("UTC").unwrap();
        assert_eq!(s.len(), 10); // YYYY-MM-DD
    }
}

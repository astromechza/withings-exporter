use crate::mappings::{measure_attrib, workout_category};
use crate::withings::api::{
    activity::DailyActivity, intraday::IntradayBody, measure::MeasureGroup, sleep::SleepNight,
    workouts::Workout,
};
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;

pub struct Instruments {
    meter: Meter,
}

impl Instruments {
    pub fn new(meter: Meter) -> Self {
        Self { meter }
    }

    pub fn record_body_measures(&self, groups: &[MeasureGroup]) {
        for g in groups {
            let attrib = measure_attrib(g.attrib);
            let model = g.model.as_deref().unwrap_or("unknown");
            let attrs = vec![
                KeyValue::new("attrib", attrib.to_string()),
                KeyValue::new("model", model.replace(' ', "_")),
            ];
            for m in &g.measures {
                let v = m.real();
                match m.kind {
                    1 => gauge_record(&self.meter, "withings_body_weight_kg", v, &attrs),
                    5 => gauge_record(&self.meter, "withings_body_fat_free_mass_kg", v, &attrs),
                    6 => gauge_record(&self.meter, "withings_body_fat_ratio", v / 100.0, &attrs),
                    8 => gauge_record(&self.meter, "withings_body_fat_mass_kg", v, &attrs),
                    11 => {
                        let mut a = attrs.clone();
                        a.push(KeyValue::new("source", "spot"));
                        gauge_record(&self.meter, "withings_heart_rate_bpm", v, &a);
                    }
                    54 => {
                        let mut a = attrs.clone();
                        a.push(KeyValue::new("source", "spot"));
                        gauge_record(&self.meter, "withings_spo2_ratio", v / 100.0, &a);
                    }
                    71 => {
                        let mut a = attrs.clone();
                        a.push(KeyValue::new("kind", "body"));
                        gauge_record(&self.meter, "withings_temperature_celsius", v, &a);
                    }
                    73 => {
                        let mut a = attrs.clone();
                        a.push(KeyValue::new("kind", "skin"));
                        gauge_record(&self.meter, "withings_temperature_celsius", v, &a);
                    }
                    76 => gauge_record(&self.meter, "withings_body_muscle_mass_kg", v, &attrs),
                    77 => gauge_record(&self.meter, "withings_body_water_ratio", v / 100.0, &attrs),
                    88 => gauge_record(&self.meter, "withings_body_bone_mass_kg", v, &attrs),
                    other => {
                        tracing::debug!(meastype = other, "unmapped meastype");
                    }
                }
            }
        }
    }

    pub fn record_workouts(&self, workouts: &[Workout]) {
        for w in workouts {
            let cat = workout_category(w.category);
            let attrs = vec![
                KeyValue::new("category", cat.clone()),
                KeyValue::new("attrib", measure_attrib(w.attrib).to_string()),
            ];
            gauge_record(
                &self.meter,
                "withings_workout_duration_seconds",
                w.duration_seconds() as f64,
                &attrs,
            );
            if let Some(c) = w.data.calories {
                gauge_record(&self.meter, "withings_workout_calories_kcal", c, &attrs);
            }
            if let Some(d) = w.data.distance {
                gauge_record(&self.meter, "withings_workout_distance_meters", d, &attrs);
            }
            if let Some(hr) = w.data.hr_average {
                let mut a = attrs.clone();
                a.push(KeyValue::new("source", "workout"));
                gauge_record(&self.meter, "withings_heart_rate_bpm", hr, &a);
            }
        }
    }

    pub fn record_sleep(&self, nights: &[SleepNight]) {
        for n in nights {
            let attrs: Vec<KeyValue> = vec![];
            gauge_record(
                &self.meter,
                "withings_sleep_duration_seconds",
                n.duration_seconds() as f64,
                &attrs,
            );
            if let Some(s) = n.data.lightsleepduration {
                let a = vec![KeyValue::new("stage", "light")];
                gauge_record(&self.meter, "withings_sleep_stage_seconds", s as f64, &a);
            }
            if let Some(s) = n.data.deepsleepduration {
                let a = vec![KeyValue::new("stage", "deep")];
                gauge_record(&self.meter, "withings_sleep_stage_seconds", s as f64, &a);
            }
            if let Some(s) = n.data.remsleepduration {
                let a = vec![KeyValue::new("stage", "rem")];
                gauge_record(&self.meter, "withings_sleep_stage_seconds", s as f64, &a);
            }
            if let Some(s) = n.data.wakeupduration {
                let a = vec![KeyValue::new("stage", "awake")];
                gauge_record(&self.meter, "withings_sleep_stage_seconds", s as f64, &a);
            }
            if let Some(hr) = n.data.hr_average {
                let a = vec![KeyValue::new("source", "sleep")];
                gauge_record(&self.meter, "withings_heart_rate_bpm", hr, &a);
            }
        }
    }

    pub fn record_intraday(&self, body: &IntradayBody) {
        for (_ts, s) in body.samples_sorted() {
            if let Some(hr) = s.heart_rate {
                let attrs = vec![KeyValue::new("source", "intraday")];
                gauge_record(&self.meter, "withings_heart_rate_bpm", hr, &attrs);
            }
            if let Some(spo2) = s.spo2_auto {
                let attrs = vec![KeyValue::new("source", "intraday")];
                gauge_record(&self.meter, "withings_spo2_ratio", spo2 / 100.0, &attrs);
            }
        }
    }

    pub fn record_lifetime(&self, steps: u64, distance_m: f64, calories_kcal: f64) {
        let s = std::sync::Arc::new(steps);
        let d = std::sync::Arc::new(distance_m);
        let c = std::sync::Arc::new(calories_kcal);
        let _ = self
            .meter
            .u64_observable_counter("withings_steps_total")
            .with_unit("1")
            .with_callback({
                let s = s.clone();
                move |obs| obs.observe(*s, &[])
            })
            .build();
        let _ = self
            .meter
            .f64_observable_counter("withings_distance_meters_total")
            .with_unit("m")
            .with_callback({
                let d = d.clone();
                move |obs| obs.observe(*d, &[])
            })
            .build();
        let _ = self
            .meter
            .f64_observable_counter("withings_active_calories_kcal_total")
            .with_unit("kcal")
            .with_callback({
                let c = c.clone();
                move |obs| obs.observe(*c, &[])
            })
            .build();
    }

    pub fn record_finalized_day(&self, day: &str, steps: u64, distance_m: f64, calories_kcal: f64) {
        let attrs = vec![KeyValue::new("date", day.to_string())];
        gauge_record(
            &self.meter,
            "withings_steps_daily_finalized",
            steps as f64,
            &attrs,
        );
        gauge_record(
            &self.meter,
            "withings_distance_meters_daily_finalized",
            distance_m,
            &attrs,
        );
        gauge_record(
            &self.meter,
            "withings_active_calories_kcal_daily_finalized",
            calories_kcal,
            &attrs,
        );
    }

    pub fn record_activity_totals_for_today(&self, a: &DailyActivity) {
        let attrs: Vec<KeyValue> = vec![];
        if let Some(s) = a.steps {
            gauge_record(
                &self.meter,
                "withings_steps_today_running",
                s as f64,
                &attrs,
            );
        }
        if let Some(d) = a.distance {
            gauge_record(
                &self.meter,
                "withings_distance_meters_today_running",
                d,
                &attrs,
            );
        }
        if let Some(c) = a.calories {
            gauge_record(
                &self.meter,
                "withings_active_calories_kcal_today_running",
                c,
                &attrs,
            );
        }
    }
}

fn gauge_record(meter: &Meter, name: &'static str, value: f64, attrs: &[KeyValue]) {
    let g = meter.f64_gauge(name).build();
    g.record(value, attrs);
}

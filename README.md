# withings-exporter

Pulls Withings ScanWatch health data via the public API and pushes
OTLP metrics to an OpenTelemetry collector. Designed to run as a
Kubernetes CronJob.

## Bootstrap

1. Register a Withings developer app at https://developer.withings.com/dashboard/
   - Pick "Public Health Data API"
   - Set a registered URL — for one-time bootstrap, the easiest is a
     unique URL from https://webhook.site that captures the redirect.

2. Build & request an authorization URL:

   ```bash
   cargo run -- auth-url \
     --client-id $WITHINGS_CLIENT_ID \
     --redirect-uri https://webhook.site/<your-uuid>
   ```

   Open the printed URL in a browser and complete consent. The
   browser is redirected to your webhook.site URL with `?code=...`.
   Copy the code (it expires in 30 seconds).

3. Exchange the code for an initial state file:

   ```bash
   cargo run -- exchange \
     --client-id $WITHINGS_CLIENT_ID \
     --client-secret $WITHINGS_CLIENT_SECRET \
     --redirect-uri https://webhook.site/<your-uuid> \
     --code <code-from-redirect> \
     --state-file ./state.json
   ```

4. Copy `state.json` to your PVC (see Kubernetes manifests in
   `home-infra/hensteeth-helm/withings-exporter-manifests.yaml`).

## Running a poll

Required env:

| Var | Default |
|---|---|
| `WITHINGS_CLIENT_ID` | (required) |
| `WITHINGS_CLIENT_SECRET` | (required) |
| `WITHINGS_STATE_PATH` | `/state/state.json` |
| `OTLP_ENDPOINT` | `http://otel-collector.monitoring:4318` |
| `WITHINGS_BACKFILL_DAYS` | `30` |
| `WITHINGS_USER_TZ` | `UTC` |
| `RUST_LOG` | `info` |

```bash
WITHINGS_CLIENT_ID=... WITHINGS_CLIENT_SECRET=... \
  WITHINGS_STATE_PATH=./state.json \
  OTLP_ENDPOINT=http://localhost:4318 \
  cargo run -- poll
```

## Metrics

| Metric | Type | Description |
|---|---|---|
| `withings_body_weight_kg` | gauge | Body weight |
| `withings_body_fat_ratio` | gauge | Fat ratio (0–1) |
| `withings_body_fat_mass_kg` | gauge | Fat mass |
| `withings_body_muscle_mass_kg` | gauge | Muscle mass |
| `withings_body_bone_mass_kg` | gauge | Bone mass |
| `withings_body_water_ratio` | gauge | Hydration ratio (0–1) |
| `withings_heart_rate_bpm` | gauge | Heart rate (source=spot/intraday/workout/sleep) |
| `withings_spo2_ratio` | gauge | SpO2 (0–1, source=spot/intraday) |
| `withings_temperature_celsius` | gauge | Temperature (kind=body/skin) |
| `withings_workout_duration_seconds` | gauge | Workout duration |
| `withings_workout_calories_kcal` | gauge | Workout calories |
| `withings_workout_distance_meters` | gauge | Workout distance |
| `withings_sleep_duration_seconds` | gauge | Total sleep duration |
| `withings_sleep_stage_seconds` | gauge | Per-stage duration (stage=light/deep/rem/awake) |
| `withings_steps_total` | counter | Lifetime steps (monotonic) |
| `withings_distance_meters_total` | counter | Lifetime distance |
| `withings_active_calories_kcal_total` | counter | Lifetime active calories |
| `withings_steps_daily_finalized` | gauge | Steps for a completed day |
| `withings_distance_meters_daily_finalized` | gauge | Distance for a completed day |
| `withings_active_calories_kcal_daily_finalized` | gauge | Calories for a completed day |

Resource attributes: `service.name=withings-exporter`, `withings.user_id=<uid>`.

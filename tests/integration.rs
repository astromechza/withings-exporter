use tempfile::tempdir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use withings_exporter::config::Config;
use withings_exporter::metrics::Instruments;
use withings_exporter::poll::run_poll;
use withings_exporter::state::{State, Tokens};
use withings_exporter::withings::client::WithingsClient;

fn fixture(name: &str) -> String {
    std::fs::read_to_string(format!("tests/fixtures/{name}")).unwrap()
}

#[tokio::test]
async fn poll_advances_cursors_and_emits() {
    let withings = MockServer::start().await;
    let otel = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/measure"))
        .respond_with(ResponseTemplate::new(200).set_body_string(fixture("getmeas.json")))
        .mount(&withings)
        .await;

    Mock::given(method("POST"))
        .and(path("/v2/measure"))
        .respond_with(move |req: &wiremock::Request| {
            let body = std::str::from_utf8(&req.body).unwrap_or("");
            if body.contains("action=getworkouts") {
                ResponseTemplate::new(200).set_body_string(fixture("getworkouts.json"))
            } else if body.contains("action=getactivity") {
                ResponseTemplate::new(200).set_body_string(fixture("getactivity.json"))
            } else if body.contains("action=getintradayactivity") {
                ResponseTemplate::new(200).set_body_string(fixture("getintraday.json"))
            } else {
                ResponseTemplate::new(404)
            }
        })
        .mount(&withings)
        .await;

    Mock::given(method("POST"))
        .and(path("/v2/sleep"))
        .respond_with(ResponseTemplate::new(200).set_body_string(fixture("getsleep.json")))
        .mount(&withings)
        .await;

    Mock::given(method("POST"))
        .and(path("/v1/metrics"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&otel)
        .await;

    let dir = tempdir().unwrap();
    let state_path = dir.path().join("state.json");
    let cfg = Config {
        client_id: "CID".into(),
        client_secret: "SECRET".into(),
        state_path: state_path.clone(),
        otlp_endpoint: otel.uri(),
        backfill_days: 30,
        user_tz: "UTC".into(),
        user_agent: "withings-exporter/test".into(),
    };

    let mut state = State {
        tokens: Tokens {
            access_token: "atk".into(),
            refresh_token: "rtk".into(),
            expires_at: i64::MAX,
            scope: "".into(),
            userid: "12345".into(),
        },
        cursors: Default::default(),
        lifetime_counters: Default::default(),
        finalized_days_emitted: Default::default(),
        emitted_record_ids: Default::default(),
    };

    let provider = withings_exporter::otlp::init(&cfg.otlp_endpoint, &state.tokens.userid).unwrap();
    let inst = Instruments::new(opentelemetry::global::meter("test"));
    let http = reqwest::Client::new();
    let client = WithingsClient::new(
        http,
        cfg.client_id.clone(),
        cfg.client_secret.clone(),
        state.tokens.clone(),
    )
    .with_base_url(withings.uri());

    run_poll(&cfg, &client, &inst, &mut state).await.unwrap();
    withings_exporter::otlp::shutdown(provider).await.unwrap();

    // Cursors should have advanced
    assert!(state.cursors.measure > 0, "measure cursor should advance");
    assert!(state.cursors.workouts > 0, "workouts cursor should advance");
    assert!(state.cursors.sleep > 0, "sleep cursor should advance");
    assert!(state.cursors.activity > 0, "activity cursor should advance");
    assert!(state.cursors.intraday > 0, "intraday cursor should advance");

    // Emit-once: workout IDs tracked
    assert!(!state.emitted_record_ids.workouts.is_empty());
    let prior_len = state.emitted_record_ids.workouts.len();

    // Second poll: same fixture → no new IDs added
    let provider2 =
        withings_exporter::otlp::init(&cfg.otlp_endpoint, &state.tokens.userid).unwrap();
    let inst2 = Instruments::new(opentelemetry::global::meter("test2"));
    run_poll(&cfg, &client, &inst2, &mut state).await.unwrap();
    withings_exporter::otlp::shutdown(provider2).await.unwrap();
    assert_eq!(
        state.emitted_record_ids.workouts.len(),
        prior_len,
        "second poll should not add duplicate workout IDs"
    );

    // Lifetime counter positive
    assert!(state.lifetime_counters.steps_total > 0);
}

#[test]
fn state_round_trip_is_lossless() {
    use withings_exporter::state::{load, save};
    let dir = tempdir().unwrap();
    let path = dir.path().join("s.json");
    let s = State {
        tokens: Tokens {
            access_token: "a".into(),
            refresh_token: "r".into(),
            expires_at: 1,
            scope: "x".into(),
            userid: "1".into(),
        },
        cursors: Default::default(),
        lifetime_counters: Default::default(),
        finalized_days_emitted: ["2026-01-01".into()].into_iter().collect(),
        emitted_record_ids: Default::default(),
    };
    save(&path, &s).unwrap();
    let loaded = load(&path).unwrap();
    assert_eq!(loaded, s);
}

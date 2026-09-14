//! Tests for the REST snapshot fetcher, against a mock HTTP server.

use std::time::Duration;

use binance_book_sync::{Error, SnapshotFetcher, SyncConfig};

fn config(server: &mockito::ServerGuard) -> SyncConfig {
    SyncConfig {
        rest_url: server.url(),
        snapshot_depth: 100,
        snapshot_retries: 3,
        // Keep the tests fast; the retry path is what matters, not the wait.
        snapshot_retry_delay: Duration::from_millis(1),
        ..SyncConfig::default()
    }
}

const BODY: &str = r#"{
    "lastUpdateId": 12345,
    "bids": [["50000.00", "10.5"], ["49999.10", "5.0"]],
    "asks": [["50001.00", "8.0"]]
}"#;

#[tokio::test]
async fn fetches_and_parses_a_snapshot() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("GET", "/api/v3/depth")
        .match_query(mockito::Matcher::AllOf(vec![
            mockito::Matcher::UrlEncoded("symbol".into(), "BTCUSDT".into()),
            mockito::Matcher::UrlEncoded("limit".into(), "100".into()),
        ]))
        .with_status(200)
        .with_body(BODY)
        .create_async()
        .await;

    let snapshot = SnapshotFetcher::new(&config(&server))
        .unwrap()
        .fetch("BTCUSDT")
        .await
        .expect("snapshot should parse");

    // The requested depth must reach the query string; the original test suite mocked
    // limit=5000 while the code asked for 50, so the mock never matched and the assertion
    // silently proved nothing.
    mock.assert_async().await;

    assert_eq!(snapshot.last_update_id, 12345);
    assert_eq!(snapshot.bids.len(), 2);
    assert_eq!(snapshot.asks.len(), 1);
    assert_eq!(snapshot.bids[0].price_e9, 50_000_000_000_000);
    assert_eq!(snapshot.bids[0].qty_e9, 10_500_000_000);
    assert_eq!(snapshot.bids[1].price_e9, 49_999_100_000_000);
    assert_eq!(snapshot.asks[0].qty_e9, 8_000_000_000);
}

#[tokio::test]
async fn retries_after_being_throttled_then_succeeds() {
    let mut server = mockito::Server::new_async().await;

    let throttled = server
        .mock("GET", "/api/v3/depth")
        .match_query(mockito::Matcher::Any)
        .with_status(429)
        .with_body("too many requests")
        .expect(1)
        .create_async()
        .await;

    let ok = server
        .mock("GET", "/api/v3/depth")
        .match_query(mockito::Matcher::Any)
        .with_status(200)
        .with_body(BODY)
        .create_async()
        .await;

    let snapshot = SnapshotFetcher::new(&config(&server))
        .unwrap()
        .fetch("BTCUSDT")
        .await
        .expect("the retry should succeed");

    throttled.assert_async().await;
    ok.assert_async().await;
    assert_eq!(snapshot.last_update_id, 12345);
}

#[tokio::test]
async fn gives_up_after_repeated_throttling() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("GET", "/api/v3/depth")
        .match_query(mockito::Matcher::Any)
        .with_status(429)
        .expect(3)
        .create_async()
        .await;

    let error = SnapshotFetcher::new(&config(&server))
        .unwrap()
        .fetch("BTCUSDT")
        .await
        .expect_err("three throttled attempts should fail");

    mock.assert_async().await;
    assert!(
        matches!(error, Error::RateLimited { attempts: 3 }),
        "expected RateLimited, got {error:?}"
    );
}

#[tokio::test]
async fn a_client_error_fails_without_retrying() {
    let mut server = mockito::Server::new_async().await;
    // Retrying a 400 will never help, so it must not consume the retry budget.
    let mock = server
        .mock("GET", "/api/v3/depth")
        .match_query(mockito::Matcher::Any)
        .with_status(400)
        .with_body("Invalid symbol.")
        .expect(1)
        .create_async()
        .await;

    let error = SnapshotFetcher::new(&config(&server))
        .unwrap()
        .fetch("NOTASYMBOL")
        .await
        .expect_err("a 400 should fail");

    mock.assert_async().await;
    match error {
        Error::SnapshotStatus { status, body } => {
            assert_eq!(status, 400);
            assert!(body.contains("Invalid symbol"));
        }
        other => panic!("expected SnapshotStatus, got {other:?}"),
    }
}

#[tokio::test]
async fn malformed_json_is_reported_as_a_parse_failure() {
    let mut server = mockito::Server::new_async().await;
    let _mock = server
        .mock("GET", "/api/v3/depth")
        .match_query(mockito::Matcher::Any)
        .with_status(200)
        .with_body("{ not json ]")
        .create_async()
        .await;

    let error = SnapshotFetcher::new(&config(&server))
        .unwrap()
        .fetch("BTCUSDT")
        .await
        .expect_err("malformed JSON should fail");

    assert!(
        matches!(
            error,
            Error::Json {
                context: "depth snapshot",
                ..
            }
        ),
        "expected a Json error, got {error:?}"
    );
}

#[tokio::test]
async fn an_unparseable_price_is_refused_rather_than_zeroed() {
    let mut server = mockito::Server::new_async().await;
    let _mock = server
        .mock("GET", "/api/v3/depth")
        .match_query(mockito::Matcher::Any)
        .with_status(200)
        .with_body(r#"{"lastUpdateId": 1, "bids": [["notaprice", "1.0"]], "asks": []}"#)
        .create_async()
        .await;

    let error = SnapshotFetcher::new(&config(&server))
        .unwrap()
        .fetch("BTCUSDT")
        .await
        .expect_err("an unparseable price should fail");

    // The original parsed with unwrap_or(0.0), turning a bad price into a level removal.
    match error {
        Error::Decimal { context, value } => {
            assert_eq!(context, "snapshot bid");
            assert_eq!(value, "notaprice");
        }
        other => panic!("expected Decimal, got {other:?}"),
    }
}

//! Stream BTCUSDT depth from Binance and report what the synchroniser emits.
//!
//! ```bash
//! cargo run --example stream_btcusdt
//! ```
//!
//! This connects to the live exchange. Press Ctrl-C to stop.

use binance_book_sync::{DepthEvent, DepthStream, SyncConfig};

#[tokio::main]
async fn main() -> Result<(), binance_book_sync::Error> {
    let config = SyncConfig {
        snapshot_depth: 100,
        ..SyncConfig::default()
    };

    let stream = DepthStream::new(vec!["BTCUSDT".to_string()], config)?;
    let mut events = stream.spawn(1024);

    let mut deltas = 0u64;

    while let Some(event) = events.recv().await {
        match event {
            DepthEvent::Snapshot {
                symbol,
                last_update_id,
                bids,
                asks,
            } => {
                let best_bid = bids.first().map(|l| l.price_e9).unwrap_or_default();
                let best_ask = asks.first().map(|l| l.price_e9).unwrap_or_default();
                println!(
                    "{symbol} snapshot @{last_update_id}: {} bids / {} asks, best {} / {}",
                    bids.len(),
                    asks.len(),
                    e9(best_bid),
                    e9(best_ask),
                );
            }
            DepthEvent::Delta {
                symbol,
                last_update_id,
                bids,
                asks,
            } => {
                deltas += 1;
                // Deltas arrive every 100ms; print occasionally rather than every one.
                if deltas % 20 == 0 {
                    println!(
                        "{symbol} delta @{last_update_id}: {} bid / {} ask changes ({deltas} so far)",
                        bids.len(),
                        asks.len(),
                    );
                }
            }
            DepthEvent::Gap {
                symbol,
                expected,
                received,
            } => {
                eprintln!("{symbol} GAP: expected {expected}, received {received}");
            }
            DepthEvent::Resync { symbol } => {
                eprintln!("{symbol} resyncing: discard the book until the next snapshot");
            }
        }
    }

    Ok(())
}

/// Render a scale-9 integer as a decimal, for display only.
fn e9(value: i64) -> String {
    format!(
        "{}.{:09}",
        value / 1_000_000_000,
        (value % 1_000_000_000).abs()
    )
}

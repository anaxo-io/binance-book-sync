//! Fetching depth snapshots from the REST endpoint.

use std::time::Duration;

use crate::config::SyncConfig;
use crate::error::{Error, Result};
use crate::types::{levels_from_wire, Snapshot, WireSnapshot};

/// Fetches depth snapshots, retrying on rate limits and transport errors.
#[derive(Debug, Clone)]
pub struct SnapshotFetcher {
    client: reqwest::Client,
    rest_url: String,
    depth: u16,
    retries: u32,
    retry_delay: Duration,
}

impl SnapshotFetcher {
    /// Build a fetcher from configuration.
    pub fn new(config: &SyncConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        Ok(Self {
            client,
            rest_url: config.rest_url.trim_end_matches('/').to_string(),
            depth: config.snapshot_depth,
            retries: config.snapshot_retries.max(1),
            retry_delay: config.snapshot_retry_delay,
        })
    }

    /// Fetch the current depth snapshot for `symbol`.
    ///
    /// Retries on transport errors, `429 Too Many Requests` and `418`/`403`, backing off
    /// linearly. Any other non-success status fails immediately, since retrying a 400 will
    /// not help.
    pub async fn fetch(&self, symbol: &str) -> Result<Snapshot> {
        let url = format!("{}/api/v3/depth", self.rest_url);
        let depth = self.depth.to_string();
        let mut last_error: Option<Error> = None;

        for attempt in 1..=self.retries {
            if attempt > 1 {
                tokio::time::sleep(self.retry_delay * (attempt - 1)).await;
            }

            let response = match self
                .client
                .get(&url)
                .header("Accept", "application/json")
                .query(&[("symbol", symbol), ("limit", depth.as_str())])
                .send()
                .await
            {
                Ok(response) => response,
                Err(source) => {
                    tracing::warn!(symbol, attempt, %source, "snapshot request failed");
                    last_error = Some(Error::from(source));
                    continue;
                }
            };

            let status = response.status();

            if status.as_u16() == 429 || status.as_u16() == 418 || status.as_u16() == 403 {
                tracing::warn!(symbol, attempt, status = status.as_u16(), "throttled");
                last_error = Some(Error::RateLimited {
                    attempts: self.retries,
                });
                continue;
            }

            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(Error::SnapshotStatus {
                    status: status.as_u16(),
                    body: body.chars().take(200).collect(),
                });
            }

            let body = response.text().await?;
            let wire: WireSnapshot = serde_json::from_str(&body).map_err(|source| Error::Json {
                context: "depth snapshot",
                source,
            })?;

            let snapshot = Snapshot {
                last_update_id: wire.last_update_id,
                bids: levels_from_wire(&wire.bids, "snapshot bid")?,
                asks: levels_from_wire(&wire.asks, "snapshot ask")?,
            };

            tracing::debug!(
                symbol,
                last_update_id = snapshot.last_update_id,
                bids = snapshot.bids.len(),
                asks = snapshot.asks.len(),
                "fetched snapshot"
            );

            return Ok(snapshot);
        }

        Err(last_error.unwrap_or(Error::RateLimited {
            attempts: self.retries,
        }))
    }
}

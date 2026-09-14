//! Tests for Binance's "manage a local order book correctly" procedure.
//!
//! These drive [`BookSync`] directly, with no network and no mocked HTTP, and assert
//! which events come out and in what order — not merely that a call returned `Ok`.

use binance_book_sync::{BookSync, DepthEvent, DepthUpdate, PriceLevel, Snapshot, SyncState};

fn level(price: i64, qty: i64) -> PriceLevel {
    PriceLevel {
        price_e9: price,
        qty_e9: qty,
    }
}

fn update(first: u64, last: u64) -> DepthUpdate {
    DepthUpdate {
        first_update_id: first,
        last_update_id: last,
        bids: vec![level(50_000_000_000_000, 1_000_000_000)],
        asks: vec![],
    }
}

fn snapshot(last_update_id: u64) -> Snapshot {
    Snapshot {
        last_update_id,
        bids: vec![level(50_000_000_000_000, 10_000_000_000)],
        asks: vec![level(50_001_000_000_000, 8_000_000_000)],
    }
}

/// Ids of the deltas emitted, so sequences can be asserted compactly.
fn delta_ids(events: &[DepthEvent]) -> Vec<u64> {
    events
        .iter()
        .filter_map(|e| match e {
            DepthEvent::Delta { last_update_id, .. } => Some(*last_update_id),
            _ => None,
        })
        .collect()
}

#[test]
fn updates_before_a_snapshot_are_buffered_not_emitted() {
    let mut sync = BookSync::new("BTCUSDT");

    assert_eq!(sync.state(), SyncState::AwaitingSnapshot);
    assert!(
        !sync.needs_snapshot(),
        "no snapshot needed before any update"
    );

    assert!(sync.apply_update(update(100, 105)).is_empty());
    assert!(sync.apply_update(update(106, 110)).is_empty());

    assert_eq!(sync.buffered_len(), 2);
    assert!(sync.needs_snapshot());
    assert_eq!(sync.last_update_id(), None);
}

#[test]
fn snapshot_emits_book_then_replays_buffered_updates_in_order() {
    let mut sync = BookSync::new("BTCUSDT");

    sync.apply_update(update(100, 105));
    sync.apply_update(update(106, 110));
    sync.apply_update(update(111, 115));

    let events = sync.apply_snapshot(snapshot(99));

    assert!(matches!(
        events[0],
        DepthEvent::Snapshot {
            last_update_id: 99,
            ..
        }
    ));
    assert_eq!(delta_ids(&events), vec![105, 110, 115]);
    assert_eq!(sync.state(), SyncState::Live);
    assert_eq!(sync.last_update_id(), Some(115));
    assert_eq!(sync.buffered_len(), 0);
}

#[test]
fn updates_already_covered_by_the_snapshot_are_dropped() {
    let mut sync = BookSync::new("BTCUSDT");

    sync.apply_update(update(100, 105));
    sync.apply_update(update(106, 110));
    sync.apply_update(update(111, 115));

    // The snapshot already contains everything up to 110.
    let events = sync.apply_snapshot(snapshot(110));

    assert_eq!(
        delta_ids(&events),
        vec![115],
        "only the update extending past the snapshot should be replayed"
    );
    assert_eq!(sync.last_update_id(), Some(115));
}

#[test]
fn a_snapshot_covering_every_buffered_update_still_goes_live() {
    let mut sync = BookSync::new("BTCUSDT");
    sync.apply_update(update(100, 105));

    let events = sync.apply_snapshot(snapshot(200));

    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], DepthEvent::Snapshot { .. }));
    assert_eq!(sync.state(), SyncState::Live);
    assert_eq!(sync.last_update_id(), Some(200));
}

#[test]
fn a_snapshot_older_than_the_buffer_is_refused() {
    let mut sync = BookSync::new("BTCUSDT");
    sync.apply_update(update(100, 105));

    // Changes 51..=99 were never seen, so joining these would produce a wrong book.
    let events = sync.apply_snapshot(snapshot(50));

    assert!(events.is_empty(), "the snapshot must be rejected");
    assert_eq!(sync.state(), SyncState::AwaitingSnapshot);
    assert!(
        sync.needs_snapshot(),
        "the buffered update is kept so a newer snapshot can be reconciled"
    );

    // A newer snapshot that straddles the buffered update is accepted.
    let events = sync.apply_snapshot(snapshot(99));
    assert_eq!(delta_ids(&events), vec![105]);
    assert_eq!(sync.state(), SyncState::Live);
}

#[test]
fn the_first_update_may_straddle_the_snapshot() {
    let mut sync = BookSync::new("BTCUSDT");
    // U=100 <= lastUpdateId+1=101 <= u=105, which the procedure allows.
    sync.apply_update(update(100, 105));

    let events = sync.apply_snapshot(snapshot(100));

    assert_eq!(delta_ids(&events), vec![105]);
    assert_eq!(sync.last_update_id(), Some(105));
}

#[test]
fn live_updates_apply_in_sequence() {
    let mut sync = BookSync::new("BTCUSDT");
    sync.apply_update(update(100, 105));
    sync.apply_snapshot(snapshot(99));

    assert_eq!(delta_ids(&sync.apply_update(update(106, 110))), vec![110]);
    assert_eq!(delta_ids(&sync.apply_update(update(111, 115))), vec![115]);
    assert_eq!(sync.last_update_id(), Some(115));
}

#[test]
fn stale_updates_are_ignored_when_live() {
    let mut sync = BookSync::new("BTCUSDT");
    sync.apply_update(update(100, 105));
    sync.apply_snapshot(snapshot(99));
    sync.apply_update(update(106, 110));

    // Replayed or out-of-order duplicate, entirely below the book's sequence.
    let events = sync.apply_update(update(90, 95));

    assert!(events.is_empty(), "a stale update must not be applied");
    assert_eq!(sync.last_update_id(), Some(110));
    assert_eq!(sync.state(), SyncState::Live);
}

#[test]
fn a_sequence_gap_reports_and_starts_a_rebuild() {
    let mut sync = BookSync::new("BTCUSDT");
    sync.apply_update(update(100, 105));
    sync.apply_snapshot(snapshot(99));

    // 106..=109 never arrived.
    let events = sync.apply_update(update(110, 115));

    assert_eq!(events.len(), 2);
    assert!(
        matches!(
            events[0],
            DepthEvent::Gap {
                expected: 106,
                received: 110,
                ..
            }
        ),
        "the gap must name the expected and received ids, got {:?}",
        events[0]
    );
    assert!(matches!(events[1], DepthEvent::Resync { .. }));

    assert!(
        delta_ids(&events).is_empty(),
        "the update that exposed the gap must not be applied"
    );
    assert_eq!(sync.state(), SyncState::AwaitingSnapshot);
    assert_eq!(sync.last_update_id(), None);
    assert!(sync.needs_snapshot());
}

#[test]
fn the_update_that_exposed_the_gap_is_retained_for_the_rebuild() {
    let mut sync = BookSync::new("BTCUSDT");
    sync.apply_update(update(100, 105));
    sync.apply_snapshot(snapshot(99));
    sync.apply_update(update(110, 115));

    assert_eq!(sync.buffered_len(), 1);

    // Rebuilding from a snapshot that straddles it recovers cleanly.
    let events = sync.apply_snapshot(snapshot(109));
    assert!(matches!(events[0], DepthEvent::Snapshot { .. }));
    assert_eq!(delta_ids(&events), vec![115]);
    assert_eq!(sync.state(), SyncState::Live);
    assert_eq!(sync.last_update_id(), Some(115));
}

#[test]
fn a_gap_inside_the_buffer_is_caught_during_replay() {
    let mut sync = BookSync::new("BTCUSDT");

    sync.apply_update(update(100, 105));
    sync.apply_update(update(106, 110));
    // 111..=119 missing.
    sync.apply_update(update(120, 125));

    let events = sync.apply_snapshot(snapshot(99));

    assert_eq!(
        delta_ids(&events),
        vec![105, 110],
        "replay stops at the gap rather than applying past it"
    );
    assert!(events.iter().any(|e| matches!(
        e,
        DepthEvent::Gap {
            expected: 111,
            received: 120,
            ..
        }
    )));
    assert_eq!(sync.state(), SyncState::AwaitingSnapshot);
}

#[test]
fn reset_discards_everything_and_asks_for_a_rebuild() {
    let mut sync = BookSync::new("BTCUSDT");
    sync.apply_update(update(100, 105));
    sync.apply_snapshot(snapshot(99));
    assert_eq!(sync.state(), SyncState::Live);

    let events = sync.reset();

    assert!(matches!(events[0], DepthEvent::Resync { .. }));
    assert_eq!(sync.state(), SyncState::AwaitingSnapshot);
    assert_eq!(sync.last_update_id(), None);
    assert_eq!(sync.buffered_len(), 0);
}

#[test]
fn the_buffer_is_bounded_and_keeps_the_newest_updates() {
    let mut sync = BookSync::with_buffer_limit("BTCUSDT", 3);

    for i in 0..10u64 {
        sync.apply_update(update(100 + i * 6, 105 + i * 6));
    }

    assert_eq!(
        sync.buffered_len(),
        3,
        "the buffer must not grow without limit"
    );

    // Ids are contiguous, so the three newest are 142..147, 148..153, 154..159.
    let events = sync.apply_snapshot(snapshot(141));
    assert_eq!(delta_ids(&events), vec![147, 153, 159]);
}

#[test]
fn events_carry_the_symbol() {
    let mut sync = BookSync::new("ETHUSDT");
    sync.apply_update(update(100, 105));
    let events = sync.apply_snapshot(snapshot(99));

    for event in &events {
        let symbol = match event {
            DepthEvent::Snapshot { symbol, .. }
            | DepthEvent::Delta { symbol, .. }
            | DepthEvent::Gap { symbol, .. }
            | DepthEvent::Resync { symbol } => symbol,
        };
        assert_eq!(symbol, "ETHUSDT");
    }
}

#[test]
fn level_payloads_survive_the_round_trip() {
    let mut sync = BookSync::new("BTCUSDT");

    sync.apply_update(DepthUpdate {
        first_update_id: 100,
        last_update_id: 105,
        bids: vec![level(49_999_750_000_000, 3_500_000_000)],
        asks: vec![level(50_002_500_000_000, 0)],
    });

    let events = sync.apply_snapshot(snapshot(99));

    let DepthEvent::Delta { bids, asks, .. } = &events[1] else {
        panic!("expected a delta, got {:?}", events[1]);
    };
    assert_eq!(bids[0], level(49_999_750_000_000, 3_500_000_000));
    assert_eq!(asks[0].qty_e9, 0, "a zero quantity means level removal");
    assert!(asks[0].is_removal());
}

#[test]
fn two_symbols_keep_independent_state() {
    let mut btc = BookSync::new("BTCUSDT");
    let mut eth = BookSync::new("ETHUSDT");

    btc.apply_update(update(100, 105));
    btc.apply_snapshot(snapshot(99));

    eth.apply_update(update(500, 505));

    assert_eq!(btc.state(), SyncState::Live);
    assert_eq!(eth.state(), SyncState::AwaitingSnapshot);
    assert_eq!(btc.last_update_id(), Some(105));
    assert_eq!(eth.last_update_id(), None);
}

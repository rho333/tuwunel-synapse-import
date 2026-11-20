//! One room: its events, the state at each, its current state and forward
//! extremities. Modelled on how tuwunel accepts a federated event: stored as
//! an outlier first, then given its state and appended.

use std::{collections::HashSet, pin::pin, sync::Arc};

use futures::TryStreamExt;
use ruma::{EventId, OwnedUserId, RoomId, RoomVersionId, events::StateEventType};
use tuwunel_core::{Result, err};
use tuwunel_matrix::Event as _;
use tuwunel_service::{
	Services,
	rooms::{state::RoomMutexGuard, state_compressor::CompressedState, timeline::RawPduId},
};

use super::{
	super::{Report, failpoint, source::Source},
	events::{self, Placement, imported},
	redactions,
	state::StateGroups,
};

pub(super) async fn import(
	services: &Services,
	source: &Source,
	room_id: &RoomId,
	version: &RoomVersionId,
	erased: &HashSet<OwnedUserId>,
	report: &mut Report,
) -> Result<bool> {
	let state_rows = source
		.rows(
			"SELECT type, state_key, event_id FROM current_state_events WHERE room_id = $1",
			&[&room_id.as_str()],
		)
		.await?;

	let shortroomid = services
		.short
		.get_or_create_shortroomid(room_id)
		.await;

	let mut skipped =
		events::store_outliers(services, source, room_id, version, erased, report).await?;

	if let Some(missing_critical) = unstored_critical_state(services, &state_rows).await {
		report.skip(format!(
			"room {room_id}: critical state {missing_critical} could not be imported"
		));
		return Ok(false);
	}

	let mut groups = StateGroups::load(services, source, room_id).await?;
	let state_lock = services.state.mutex.lock(room_id).await;

	append_timeline(
		services,
		source,
		room_id,
		version,
		&mut groups,
		&mut skipped,
		&state_lock,
		report,
	)
	.await?;

	let state = set_current_state(services, room_id, &state_rows, &state_lock).await?;
	let extremities =
		set_forward_extremities(services, source, room_id, &skipped, &state_lock).await?;

	let dropped = groups
		.dropped
		.saturating_add(state)
		.saturating_add(extremities);

	if dropped > 0 {
		report.skip(format!(
			"{room_id}: left out {dropped} state entries with an invalid event ID and forward \
			 extremities naming an event that wasn't imported"
		));
	}

	// After current state, so the room's power levels decide who could redact.
	redactions::apply_late(services, source, room_id, shortroomid, &state_lock, report).await?;

	Ok(true)
}

async fn unstored_critical_state<'a>(
	services: &Services,
	rows: &'a [tokio_postgres::Row],
) -> Option<&'a str> {
	for row in rows {
		let event_type: &str = row.get(0);
		let state_key: &str = row.get(1);
		let event_id: &str = row.get(2);
		if state_key.is_empty() && matches!(event_type, "m.room.create" | "m.room.power_levels")
		{
			let Ok(id) = <&EventId>::try_from(event_id) else {
				return Some(event_type);
			};

			if services.timeline.get_pdu_json(id).await.is_err() {
				return Some(event_type);
			}
		}
	}

	None
}

/// Gives each timeline event the state before it, then appends it, or only
/// marks it soft-failed.
#[expect(clippy::too_many_arguments)]
async fn append_timeline(
	services: &Services,
	source: &Source,
	room_id: &RoomId,
	room_version: &RoomVersionId,
	groups: &mut StateGroups,
	skipped: &mut HashSet<String>,
	state_lock: &RoomMutexGuard,
	report: &mut Report,
) -> Result {
	let mut timeline = pin!(events::timeline(source, room_id).await?);
	let mut appended: usize = 0;
	while let Some(placement) = timeline.try_next().await? {
		let Placement {
			event_id,
			state_group,
			soft_failed,
			synapse_position: (topological, stream),
		} = placement;
		let event_id = &event_id;
		let record = |pdu_id: RawPduId| {
			services.timeline.record_synapse_position(
				u64::from_be_bytes(pdu_id.shortroomid()),
				topological,
				stream,
				pdu_id.pdu_count(),
			);
		};

		// Appended by an earlier run that stopped partway through this room.
		if let Ok(pdu_id) = services.timeline.get_pdu_id(event_id).await {
			record(pdu_id);
			continue;
		}

		// Not stored when it couldn't be converted, which is already reported.
		let Ok(json) = services
			.timeline
			.get_outlier_pdu_json(event_id)
			.await
		else {
			skipped.insert(event_id.to_string());
			continue;
		};

		let pdu = services
			.timeline
			.get_outlier_pdu(event_id)
			.await?;

		let before = groups
			.before(services, room_id, room_version, &pdu, state_group, soft_failed)
			.await
			.and_then(|before| {
				before.ok_or_else(|| err!("Synapse doesn't record the state before it"))
			});

		let Some(before) = report.keep(format_args!("{room_id} event {event_id}"), before) else {
			skipped.insert(event_id.to_string());
			continue;
		};

		let shortstatehash = services
			.state
			.set_event_state(event_id, room_id, Arc::new(before))
			.await?;

		// The next event's state is stored as a diff against the room's current one.
		services
			.state
			.set_room_state(room_id, shortstatehash, state_lock);

		if soft_failed {
			services
				.pdu_metadata
				.mark_as_referenced(room_id, pdu.prev_events());
			services
				.pdu_metadata
				.mark_event_soft_failed(event_id);
			Report::count(&mut report.soft_failed);
			continue;
		}

		record(
			services
				.timeline
				.append_imported_pdu(&pdu, json, state_lock)
				.await?,
		);

		Report::count(&mut report.events);
		Report::count(&mut appended);
		failpoint::reached("room_events", appended).await;
	}

	Ok(())
}

/// Synapse's current state, installed as the room's, with membership caches
/// brought in line. Events that couldn't be imported stay in it, as they do in
/// the state other servers hold. Returns how many entries had an invalid ID.
async fn set_current_state(
	services: &Services,
	room_id: &RoomId,
	rows: &[tokio_postgres::Row],
	state_lock: &RoomMutexGuard,
) -> Result<usize> {
	if rows.is_empty() {
		return Ok(0);
	}

	let mut dropped: usize = 0;
	let mut state = CompressedState::new();
	for row in rows {
		let Ok(event_id) = <&EventId>::try_from(row.get::<_, &str>(2)) else {
			dropped = dropped.saturating_add(1);
			continue;
		};

		let event_type = StateEventType::from(row.get::<_, &str>(0));
		let key = services
			.short
			.get_or_create_shortstatekey(&event_type, row.get(1))
			.await;

		state.insert(
			services
				.state_compressor
				.compress_state_event(key, event_id)
				.await,
		);
	}

	let state = Arc::new(state);
	let saved = services
		.state_compressor
		.save_state(room_id, state.clone())
		.await?;

	services
		.state
		.sync_imported_state(room_id, saved.shortstatehash, &state, state_lock)
		.await?;

	Ok(dropped)
}

/// Returns how many extremities named an event not imported: new events can't
/// build on one Tuwunel doesn't have.
async fn set_forward_extremities(
	services: &Services,
	source: &Source,
	room_id: &RoomId,
	skipped: &HashSet<String>,
	state_lock: &RoomMutexGuard,
) -> Result<usize> {
	let rows = source
		.rows("SELECT event_id FROM event_forward_extremities WHERE room_id = $1", &[
			&room_id.as_str()
		])
		.await?;

	let extremities: Vec<&EventId> = rows
		.iter()
		.filter_map(|row| imported(row.get(0), skipped))
		.collect();

	services
		.state
		.set_forward_extremities(room_id, extremities.iter().copied(), state_lock)
		.await;

	Ok(rows.len().saturating_sub(extremities.len()))
}

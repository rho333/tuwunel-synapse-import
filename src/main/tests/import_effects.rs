#![cfg(test)]

use std::{iter::once, sync::Arc, time::Duration};

use futures::StreamExt;
use serde_json::json;
use tuwunel_core::{
	Result,
	ruma::{
		OwnedEventId, RoomId, UserId, event_id,
		events::{
			StateEventType, TimelineEventType,
			room::{
				member::{MembershipState, RoomMemberEventContent},
				message::RoomMessageEventContent,
			},
		},
		server_name, user_id,
	},
	utils::to_canonical_object,
};
use tuwunel_matrix::{PduEvent, event::Event, pdu::PduBuilder};
use tuwunel_service::{Services, rooms::state_compressor::CompressedState};

use self::{
	client::{Client, poll_until, register},
	fixture::boot,
	timeline::{append_message, append_pdu},
};

mod client;
mod fixture;
mod timeline;

const ADMIN_TOKEN: &str = "import-effects-admin-access-token";
const READER_TOKEN: &str = "import-effects-reader-access-token";

const DEADLINE: Duration = Duration::from_secs(10);

/// Imported history is stored and searchable but replays none of its effects.
///
/// Each imported event has a live twin that does take effect. The workers run
/// in arrival order and the imported events go first, so once the live twin's
/// effect shows, the imported one would already have shown had it been queued.
/// Imports also leave the forward extremities and the sender's read marker.
#[test]
fn imported_events_notify_no_one_and_run_nothing() -> Result {
	let options = [
		"create_admin_room=true",
		"grant_admin_to_first_user=false",
		"auto_accept_invites=true",
	];

	boot("import-effects", options, exercise)
}

async fn exercise(services: &Services, base: &str) -> Result {
	let admin_id = register(services, "importadmin", ADMIN_TOKEN).await?;
	let reader_id = register(services, "importreader", READER_TOKEN).await?;
	let user = async |name: &str| {
		let token = format!("import-effects-{name}-access-token");

		register(services, name, &token).await
	};

	let imported_target = user("importedtarget").await?;
	let live_target = user("livetarget").await?;
	let imported_invitee = user("importedinvitee").await?;
	let live_invitee = user("liveinvitee").await?;

	services.admin.make_user_admin(&admin_id).await?;

	let admin = Client { services, base, token: ADMIN_TOKEN };
	let reader = Client { services, base, token: READER_TOKEN };
	let room = admin
		.create_room(&json!({ "preset": "public_chat" }))
		.await?;

	reader
		.post(&format!("rooms/{room}/join"), &json!({}))
		.await?;

	let admin_room = services.admin.get_admin_room().await?;
	let deactivate = |user_id: &UserId| {
		RoomMessageEventContent::text_plain(format!("!admin users deactivate {user_id}"))
	};

	let hello = RoomMessageEventContent::text_plain("hello");
	let invite = |user_id: &UserId| {
		let content = RoomMemberEventContent::new(MembershipState::Invite);

		PduBuilder::state(user_id.as_str(), &content)
	};

	let read_marker = async || {
		services
			.read_receipt
			.private_read_get_count(&room, &admin_id)
			.await
			.ok()
	};

	let marker_before = read_marker().await;
	let imported_hello =
		import_pdu(services, &admin_id, &room, PduBuilder::timeline(&hello)).await?;
	import_pdu(
		services,
		&admin_id,
		&admin_room,
		PduBuilder::timeline(&deactivate(&imported_target)),
	)
	.await?;
	import_pdu(services, &admin_id, &room, invite(&imported_invitee)).await?;

	services
		.timeline
		.get_pdu_id(&imported_hello)
		.await?;

	assert_eq!(read_marker().await, marker_before, "an import moved the sender's marker");
	assert_eq!(
		services
			.pusher
			.notification_count(&reader_id, &room)
			.await,
		0
	);

	append_message(services, &admin_id, &room, &hello).await?;
	assert_ne!(read_marker().await, marker_before, "the live event left its sender's marker");

	let search = json!({"search_categories": {"room_events": {
		"search_term": "hello", "filter": {"rooms": [room]}
	}}});
	let found = admin.post("search", &search).await?;
	assert_eq!(found["search_categories"]["room_events"]["count"], json!(2));

	append_message(services, &admin_id, &admin_room, &deactivate(&live_target)).await?;
	append_pdu(services, &admin_id, &room, invite(&live_invitee)).await?;

	let membership = async |user_id: &UserId| {
		services
			.state_cache
			.user_membership(user_id, &room)
			.await
	};

	let notified = async || {
		services
			.pusher
			.notification_count(&reader_id, &room)
			.await
			== 1
	};
	let commanded = async || {
		services
			.users
			.is_deactivated(&live_target)
			.await
			.unwrap_or(false)
	};

	let accepted = async || membership(&live_invitee).await == Some(MembershipState::Join);

	assert!(poll_until(DEADLINE, notified).await, "the live message never notified");
	assert!(poll_until(DEADLINE, commanded).await, "the live command never ran");
	assert!(poll_until(DEADLINE, accepted).await, "the live invite was never accepted");

	assert!(
		!services
			.users
			.is_deactivated(&imported_target)
			.await?,
		"an imported command ran"
	);
	assert_eq!(membership(&imported_invitee).await, Some(MembershipState::Invite));

	exercise_resilience(services, base).await
}

/// Append an event as history imported from another server, then make it the
/// room's forward extremity as an importer does once the room's history is in.
///
/// Fails if the append itself moved the extremities.
async fn import_pdu(
	services: &Services,
	sender: &UserId,
	room_id: &RoomId,
	builder: PduBuilder,
) -> Result<OwnedEventId> {
	let state_lock = services.state.mutex.lock(room_id).await;
	let (pdu, pdu_json) = services
		.timeline
		.create_hash_and_sign_event(builder, sender, room_id, &state_lock)
		.await?;

	let statehashid = services.state.append_to_state(&pdu).await?;
	let extremities = forward_extremities(services, room_id).await;

	services
		.timeline
		.append_imported_pdu(&pdu, pdu_json, &state_lock)
		.await?;

	assert_eq!(
		forward_extremities(services, room_id).await,
		extremities,
		"an imported append moved the forward extremities"
	);

	services
		.state
		.set_room_state(room_id, statehashid, &state_lock);

	services
		.state
		.set_forward_extremities(room_id, once(pdu.event_id()), &state_lock)
		.await;

	Ok(pdu.event_id().to_owned())
}

async fn forward_extremities(services: &Services, room_id: &RoomId) -> Vec<OwnedEventId> {
	services
		.state
		.get_forward_extremities(room_id)
		.map(ToOwned::to_owned)
		.collect()
		.await
}

async fn exercise_resilience(services: &Services, base: &str) -> Result {
	let creator_token = "import-resilience-creator-access-token";
	let loser_token = "import-resilience-loser-access-token";
	let creator_id = register(services, "resiliencecreator", creator_token).await?;
	let fork_loser = register(services, "forkloser", loser_token).await?;
	let creator = Client { services, base, token: creator_token };
	let loser = Client { services, base, token: loser_token };

	let room = creator
		.create_room(&json!({ "preset": "public_chat" }))
		.await?;
	loser
		.post(&format!("rooms/{room}/join"), &json!({}))
		.await?;

	exercise_malformed_and_redactions(services, &creator_id, &room).await?;
	exercise_sync_imported_state(services, &fork_loser, &room).await
}

async fn exercise_malformed_and_redactions(
	services: &Services,
	creator_id: &UserId,
	room: &RoomId,
) -> Result {
	let orphan_thread: PduBuilder = serde_json::from_value(json!({
		"type": "m.room.message",
		"content": {
			"msgtype": "m.text",
			"body": "reply to missing root",
			"m.relates_to": {
				"rel_type": "m.thread",
				"event_id": "$missing_thread_root_event"
			}
		}
	}))?;
	import_pdu(services, creator_id, room, orphan_thread).await?;

	append_raw_imported(services, room, json!({
		"event_id": "$malformed_member_state_key",
		"room_id": room,
		"sender": creator_id,
		"state_key": "not-a-valid-mxid",
		"origin_server_ts": 999,
		"type": "m.room.member",
		"content": { "membership": "join" },
		"prev_events": [],
		"auth_events": [],
		"depth": 9
	}))
	.await?;

	let bad_body: PduBuilder = serde_json::from_value(json!({
		"type": "m.room.message",
		"content": { "msgtype": "m.text", "body": 12345 }
	}))?;
	import_pdu(services, creator_id, room, bad_body).await?;

	let create_pdu = services
		.state_accessor
		.room_state_get(room, &StateEventType::RoomCreate, "")
		.await?;
	append_raw_imported(services, room, json!({
		"event_id": "$redact_create_attempt",
		"room_id": room,
		"sender": creator_id,
		"origin_server_ts": 1000,
		"type": "m.room.redaction",
		"redacts": create_pdu.event_id(),
		"content": { "redacts": create_pdu.event_id() },
		"prev_events": [],
		"auth_events": [],
		"depth": 9
	}))
	.await?;

	let remote_msg_id = event_id!("$remote_msg_to_redact");
	append_raw_imported(services, room, json!({
		"event_id": remote_msg_id,
		"room_id": room,
		"sender": "@user:remote.example",
		"origin_server_ts": 1000,
		"type": "m.room.message",
		"content": { "msgtype": "m.text", "body": "secret text" },
		"prev_events": [],
		"auth_events": [],
		"depth": 10
	}))
	.await?;

	append_raw_imported(services, room, json!({
		"event_id": "$remote_mod_redaction",
		"room_id": room,
		"sender": "@mod:remote.example",
		"origin_server_ts": 1001,
		"type": "m.room.redaction",
		"redacts": remote_msg_id,
		"content": { "redacts": remote_msg_id },
		"prev_events": [remote_msg_id],
		"auth_events": [],
		"depth": 11
	}))
	.await?;

	let after_redact = services.timeline.get_pdu(remote_msg_id).await?;
	assert!(
		after_redact.get_content_as_value().get("body").is_none(),
		"same-server federated redaction must redact target body during import"
	);

	Ok(())
}

async fn exercise_sync_imported_state(
	services: &Services,
	fork_loser: &UserId,
	room: &RoomId,
) -> Result {
	let cur_state = services.state.get_room_shortstatehash(room).await?;
	let prejoin_user = user_id!("@prejoin:remote.example");
	let prejoin_event_id = event_id!("$prejoin_outlier_member");
	let (_, prejoin_json) = raw_pdu(json!({
		"event_id": prejoin_event_id,
		"room_id": room,
		"sender": prejoin_user,
		"state_key": prejoin_user,
		"origin_server_ts": 500,
		"type": TimelineEventType::RoomMember,
		"content": { "membership": "join" },
		"prev_events": [],
		"auth_events": [],
		"depth": 1
	}))?;
	services
		.timeline
		.add_pdu_outlier(prejoin_event_id, &prejoin_json);

	let mut full_state = CompressedState::new();
	let entries: Vec<_> = services
		.state_accessor
		.state_full_ids(cur_state)
		.collect()
		.await;
	let loser_key = services
		.short
		.get_or_create_shortstatekey(&StateEventType::RoomMember, fork_loser.as_str())
		.await;
	for (shortkey, eid) in entries {
		if shortkey != loser_key {
			full_state.insert(
				services
					.state_compressor
					.compress_state_event(shortkey, &eid)
					.await,
			);
		}
	}
	let prejoin_key = services
		.short
		.get_or_create_shortstatekey(&StateEventType::RoomMember, prejoin_user.as_str())
		.await;
	full_state.insert(
		services
			.state_compressor
			.compress_state_event(prejoin_key, prejoin_event_id)
			.await,
	);

	let full_state = Arc::new(full_state);
	let saved = services
		.state_compressor
		.save_state(room, full_state.clone())
		.await?;
	let state_lock = services.state.mutex.lock(room).await;
	services
		.state
		.sync_imported_state(room, saved.shortstatehash, &full_state, &state_lock)
		.await?;
	drop(state_lock);

	assert_eq!(
		services
			.state_cache
			.user_membership(prejoin_user, room)
			.await,
		Some(MembershipState::Join),
		"pre-join remote outlier member populated into state_cache"
	);
	assert!(
		services
			.state_cache
			.server_in_room(server_name!("remote.example"), room)
			.await,
		"remote server indexed for federation in state_cache"
	);
	assert_ne!(
		services
			.state_cache
			.user_membership(fork_loser, room)
			.await,
		Some(MembershipState::Join),
		"fork-loser absent from final state cleared from joined members"
	);

	Ok(())
}

async fn append_raw_imported(
	services: &Services,
	room: &RoomId,
	value: serde_json::Value,
) -> Result {
	let (pdu, pdu_json) = raw_pdu(value)?;
	let state_lock = services.state.mutex.lock(room).await;
	services.state.append_to_state(&pdu).await?;
	services
		.timeline
		.append_imported_pdu(&pdu, pdu_json, &state_lock)
		.await?;

	Ok(())
}

fn raw_pdu(
	mut value: serde_json::Value,
) -> Result<(PduEvent, tuwunel_core::ruma::CanonicalJsonObject)> {
	value.as_object_mut().unwrap().insert(
		"hashes".to_owned(),
		json!({ "sha256": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" }),
	);
	let canonical = to_canonical_object(&value)?;
	let pdu: PduEvent = serde_json::from_value(value)?;

	Ok((pdu, canonical))
}

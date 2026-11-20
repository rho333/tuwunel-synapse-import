//! Rooms: their events and state, aliases, the public room directory, and
//! which rooms users forgot.

mod events;
mod forgotten;
mod redactions;
mod room;
mod state;
#[cfg(test)]
mod tests;

use std::collections::HashSet;

use ruma::{
	OwnedRoomAliasId, OwnedRoomId, OwnedUserId, RoomVersionId, UserId,
	events::{
		AnyStrippedStateEvent,
		room::member::{MembershipState, RoomMemberEventContent},
	},
	serde::Raw,
};
use serde_json::Value;
use tokio_postgres::Row;
use tuwunel_core::{Result, err, info};
use tuwunel_matrix::{PduCount, room_version};
use tuwunel_service::{Services, rooms::state_cache::MembershipUpdate};

pub(crate) use self::events::{convert, soft_failed, timeline};
use super::{
	Report, marker_present,
	source::{Source, user_id},
	stamp,
};

pub(super) async fn import(services: &Services, source: &Source, report: &mut Report) -> Result {
	let erased: HashSet<OwnedUserId> = source
		.rows("SELECT user_id FROM erased_users", &[])
		.await?
		.iter()
		.filter_map(|row| user_id(row, 0).ok())
		.collect();

	for user_id in &erased {
		services.users.set_erased(user_id);
	}

	let rows = source
		.rows(
			"SELECT room_id, room_version, coalesce(is_public, false) FROM rooms ORDER BY \
			 room_id",
			&[],
		)
		.await?;

	let total = rows.len();
	for (i, row) in rows.into_iter().enumerate() {
		let room_id = OwnedRoomId::try_from(row.get::<_, &str>(0))
			.map_err(|e| err!("Synapse room ID {:?}: {e}", row.get::<_, &str>(0)));

		let Some(room_id) = report.keep("room", room_id) else {
			continue;
		};

		// Per room as well as per phase, so a rerun doesn't redo finished rooms.
		let marker = format!("synapse_import_room:{room_id}");
		if marker_present(services, &marker) {
			continue;
		}

		let version = match room_version(row.get(1)) {
			| Ok(version) => version,
			| Err(e) => {
				report.skip(format!("room {room_id}: {e}"));
				continue;
			},
		};

		info!("Importing room {room_id} ({} of {total})", i.saturating_add(1));
		if !room::import(services, source, &room_id, &version, &erased, report).await? {
			continue;
		}

		if row.get::<_, bool>(2) {
			services.directory.set_public(&room_id, None);
			Report::count(&mut report.public_rooms);
		}

		stamp(services, &marker);
		Report::count(&mut report.rooms);
	}

	import_pending_invites(services, source, report).await?;
	import_aliases(services, source, report).await?;
	forgotten::import(services, source, report).await
}

/// Synapse leaves the column empty for some rooms it only heard of, and has
/// served experimental versions tuwunel doesn't know.
pub(crate) fn room_version(version: Option<&str>) -> Result<RoomVersionId> {
	let version: RoomVersionId = version
		.ok_or_else(|| err!("Synapse doesn't record its room version"))?
		.try_into()
		.map_err(|e| err!("room version: {e}"))?;

	room_version::rules(&version)?;
	Ok(version)
}

async fn import_aliases(services: &Services, source: &Source, report: &mut Report) -> Result {
	let rows = source
		.rows("SELECT room_alias, room_id, creator FROM room_aliases ORDER BY room_alias", &[])
		.await?;

	for row in rows {
		match import_alias(services, &row).await {
			| Ok(()) => Report::count(&mut report.aliases),
			| Err(e) => report.skip(format!("alias {}: {e}", row.get::<_, &str>(0))),
		}
	}

	Ok(())
}

/// An alias without a recorded creator is the server's. A Synapse room
/// holding tuwunel's own admin alias is refused here.
async fn import_alias(services: &Services, row: &Row) -> Result {
	let alias = OwnedRoomAliasId::try_from(row.get::<_, &str>(0)).map_err(|e| err!("{e}"))?;
	let room_id = OwnedRoomId::try_from(row.get::<_, &str>(1)).map_err(|e| err!("{e}"))?;
	let creator = match row.get::<_, Option<&str>>(2) {
		| Some(_) => user_id(row, 2)?,
		| None => services.globals.server_user.clone(),
	};

	if alias == services.admin.admin_alias {
		return Err(err!("it is tuwunel's admin room alias"));
	}

	if services
		.alias
		.resolve_local_alias(&alias)
		.await
		.is_ok_and(|existing| existing == room_id)
	{
		return Ok(());
	}

	services
		.alias
		.set_alias_by(&alias, &room_id, &creator)
}

/// Pending invites and knocks for local users in remote rooms where this server
/// has no joined member (stored as outliers with stripped state in `unsigned`).
async fn import_pending_invites(
	services: &Services,
	source: &Source,
	report: &mut Report,
) -> Result {
	if !source.has_table("local_current_membership").await? {
		return Ok(());
	}

	let rows = source
		.rows(
			"SELECT m.room_id, m.user_id, m.membership, j.json FROM local_current_membership m \
			 JOIN event_json j USING (event_id) WHERE m.membership IN ('invite', 'knock') ORDER \
			 BY m.room_id, m.user_id",
			&[],
		)
		.await?;

	for row in rows {
		let result = import_pending_invite(services, &row).await;
		report.keep(
			format_args!("pending {} in {}", row.get::<_, &str>(2), row.get::<_, &str>(0)),
			result,
		);
	}

	Ok(())
}

async fn import_pending_invite(services: &Services, row: &Row) -> Result {
	let room_id = OwnedRoomId::try_from(row.get::<_, &str>(0)).map_err(|e| err!("{e}"))?;
	let user_id = user_id(row, 1)?;
	if !services.users.is_active(&user_id).await {
		return Ok(());
	}

	let target_state = match row.get::<_, &str>(2) {
		| "invite" => MembershipState::Invite,
		| "knock" => MembershipState::Knock,
		| _ => return Ok(()),
	};

	if services
		.state_cache
		.user_membership(&user_id, &room_id)
		.await
		== Some(target_state.clone())
	{
		return Ok(());
	}

	let event: Value = serde_json::from_str(row.get(3)).map_err(|e| err!("invalid JSON: {e}"))?;
	let sender_str = event
		.get("sender")
		.and_then(Value::as_str)
		.ok_or_else(|| err!("missing sender"))?;
	let sender = UserId::parse(sender_str).map_err(|e| err!("invalid sender: {e}"))?;
	let content: RoomMemberEventContent = event
		.get("content")
		.cloned()
		.ok_or_else(|| err!("missing content"))
		.and_then(|c| serde_json::from_value(c).map_err(|e| err!("invalid member content: {e}")))?;

	let stripped = event
		.get("unsigned")
		.and_then(|u| u.get("invite_room_state").or_else(|| u.get("knock_room_state")))
		.and_then(|s| serde_json::from_value::<Vec<Raw<AnyStrippedStateEvent>>>(s.clone()).ok());

	let invite_via = (target_state == MembershipState::Invite)
		.then(|| vec![sender.server_name().to_owned()]);
	let count = services.globals.next_count();
	services
		.state_cache
		.update_imported_membership(MembershipUpdate {
			room_id: &room_id,
			user_id: &user_id,
			membership_event: content,
			sender: &sender,
			last_state: stripped,
			invite_via,
			update_joined_count: false,
			count: PduCount::Normal(*count),
		})
		.await
}

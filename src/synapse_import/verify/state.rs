//! A room's current state, entry by entry, `(type, state_key) -> event ID`;
//! that each state event is stored; and the membership lists tuwunel derives
//! from it (joined, invited, knocking).

use std::collections::{BTreeMap, BTreeSet};

use futures::StreamExt;
use ruma::EventId;
use tuwunel_core::Result;

use super::{super::rooms::soft_failed, Ctx, rooms::Room};

type State = BTreeMap<(String, String), String>;

/// Why the import couldn't place an event Synapse has, as rooms/state.rs
/// finds it: no state group, a cycle in `state_group_edges`, or a group whose
/// ancestry is missing.
pub(super) async fn unplaceable(ctx: &Ctx<'_>, event_id: &str) -> Result<Option<String>> {
	let rows = ctx
		.source
		.rows(
			"WITH RECURSIVE chain(id, path, cycle) AS (SELECT state_group, ARRAY[state_group], \
			 FALSE FROM event_to_state_groups WHERE event_id = $1 UNION ALL SELECT \
			 e.prev_state_group, chain.path || e.prev_state_group, e.prev_state_group = \
			 ANY(chain.path) FROM chain JOIN state_group_edges e ON e.state_group = chain.id \
			 WHERE NOT chain.cycle) SELECT id, cycle, EXISTS (SELECT 1 FROM state_groups g WHERE \
			 g.id = chain.id) FROM chain",
			&[&event_id],
		)
		.await?;

	if rows.is_empty() {
		return Ok(Some("Synapse doesn't record the state before it".to_owned()));
	}

	if let Some(row) = rows.iter().find(|row| row.get::<_, bool>(1)) {
		return Ok(Some(format!(
			"Synapse state_group_edges contains a cycle at group {}",
			row.get::<_, i64>(0)
		)));
	}

	Ok(rows
		.iter()
		.find(|row| !row.get::<_, bool>(2))
		.map(|row| {
			format!("Synapse state group {} is referenced but missing", row.get::<_, i64>(0))
		}))
}

pub(super) async fn verify(ctx: &mut Ctx<'_>, room: &Room<'_>) -> Result {
	let mut rows = ctx
		.source
		.rows(
			"SELECT c.type, c.state_key, c.event_id, m.membership FROM current_state_events c \
			 LEFT JOIN room_memberships m USING (event_id) WHERE c.room_id = $1",
			&[&room.id.as_str()],
		)
		.await?;

	let left_room = rows.is_empty();
	if left_room && let Some(group) = last_timeline_group(ctx, room).await? {
		rows = ctx
			.source
			.rows(
				"WITH RECURSIVE chain(id, depth, path) AS (SELECT $1::bigint, 0, \
				 ARRAY[$1::bigint] UNION ALL SELECT e.prev_state_group, chain.depth + 1, \
				 chain.path || e.prev_state_group FROM chain JOIN state_group_edges e ON \
				 e.state_group = chain.id WHERE NOT (e.prev_state_group = ANY(chain.path))) \
				 SELECT DISTINCT ON (s.type, s.state_key) s.type, s.state_key, s.event_id, \
				 m.membership FROM chain JOIN state_groups_state s ON s.state_group = chain.id \
				 LEFT JOIN room_memberships m USING (event_id) ORDER BY s.type, s.state_key, \
				 chain.depth ASC",
				&[&group],
			)
			.await?;
	}

	let mut want = State::new();
	let mut members: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
	for row in &rows {
		let (kind, state_key, event_id): (&str, &str, &str) =
			(row.get(0), row.get(1), row.get(2));
		let Ok(id) = <&EventId>::try_from(event_id) else {
			ctx.out.excuse(
				"state",
				format!("{} {kind} {state_key:?}", room.id),
				"invalid event ID",
			);
			continue;
		};

		if kind == "m.room.member" {
			let membership = match row.get::<_, Option<&str>>(3) {
				| Some(m) => m.to_owned(),
				| None => member_content_membership(ctx, event_id).await?,
			};
			if matches!(membership.as_str(), "join" | "invite" | "knock") {
				if ruma::UserId::parse(state_key).is_err() {
					ctx.out.excuse(
						"memberships",
						format!("{} {state_key:?}", room.id),
						"invalid member state_key",
					);
				} else if ctx.services.timeline.get_pdu_json(id).await.is_ok() {
					members
						.entry(membership)
						.or_default()
						.insert(state_key.to_owned());
				} else {
					ctx.out.excuse(
						"memberships",
						format!("{} {state_key}", room.id),
						"member event could not be imported",
					);
				}
			}
		}

		want.insert((kind.to_owned(), state_key.to_owned()), event_id.to_owned());
	}

	if left_room {
		overlay_pending_local_invites(ctx, room, &mut members).await?;
	}

	let have = current_state(ctx, room).await;
	compare(ctx, room, &want, &have);

	for event_id in want.values() {
		let Ok(id) = <&EventId>::try_from(event_id.as_str()) else {
			continue;
		};

		if ctx
			.services
			.timeline
			.get_pdu_json(id)
			.await
			.is_ok()
		{
			ctx.out.matched("state_events");
		} else {
			room.lacks(ctx, "state_events", event_id, "the state event isn't stored")
				.await?;
		}
	}

	memberships(ctx, room, &members).await;
	Ok(())
}

/// Synapse empties `current_state_events` once no local user is joined to a
/// room; Tuwunel keeps the state after the room's last appended timeline event.
async fn last_timeline_group(ctx: &Ctx<'_>, room: &Room<'_>) -> Result<Option<i64>> {
	let rows = ctx
		.source
		.rows(
			"SELECT e.event_id, j.internal_metadata, g.state_group FROM events e JOIN event_json \
			 j USING (event_id) JOIN event_to_state_groups g USING (event_id) LEFT JOIN \
			 rejections r USING (event_id) WHERE e.room_id = $1 AND NOT e.outlier AND \
			 e.rejection_reason IS NULL AND r.event_id IS NULL ORDER BY e.topological_ordering \
			 DESC, e.stream_ordering DESC LIMIT 32",
			&[&room.id.as_str()],
		)
		.await?;

	for row in rows {
		if soft_failed(row.get(1)) {
			continue;
		}

		let Ok(id) = <&EventId>::try_from(row.get::<_, &str>(0)) else {
			continue;
		};

		if ctx.services.timeline.get_pdu_id(id).await.is_ok() {
			return Ok(Some(row.get(2)));
		}
	}

	Ok(None)
}

async fn member_content_membership(ctx: &Ctx<'_>, event_id: &str) -> Result<String> {
	let rows = ctx
		.source
		.rows("SELECT json FROM event_json WHERE event_id = $1", &[&event_id])
		.await?;

	Ok(rows
		.first()
		.and_then(|row| serde_json::from_str::<serde_json::Value>(row.get(0)).ok())
		.and_then(|val| {
			val.get("content")?
				.get("membership")?
				.as_str()
				.map(ToOwned::to_owned)
		})
		.unwrap_or_default())
}

async fn overlay_pending_local_invites(
	ctx: &Ctx<'_>,
	room: &Room<'_>,
	members: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result {
	if !ctx.source.has_table("local_current_membership").await? {
		return Ok(());
	}

	let rows = ctx
		.source
		.rows(
			"SELECT user_id, membership FROM local_current_membership WHERE room_id = $1 AND \
			 membership IN ('invite', 'knock')",
			&[&room.id.as_str()],
		)
		.await?;

	for row in rows {
		let (user_str, membership): (&str, &str) = (row.get(0), row.get(1));
		if !ctx.active.iter().any(|u| u.as_str() == user_str) {
			continue;
		}

		for set in members.values_mut() {
			set.remove(user_str);
		}
		members
			.entry(membership.to_owned())
			.or_default()
			.insert(user_str.to_owned());
	}

	Ok(())
}

/// Entries whose short IDs don't resolve come back under a placeholder type,
/// so they show up as a mismatch rather than vanish.
async fn current_state(ctx: &Ctx<'_>, room: &Room<'_>) -> State {
	let services = ctx.services;
	let Ok(shortstatehash) = services
		.state
		.get_room_shortstatehash(room.id)
		.await
	else {
		return State::new();
	};

	let entries: Vec<_> = services
		.state_accessor
		.state_full_ids(shortstatehash)
		.collect()
		.await;

	let mut state = State::new();
	for (shortstatekey, event_id) in entries {
		let key = match services
			.short
			.get_statekey_from_short(shortstatekey)
			.await
		{
			| Ok((kind, state_key)) => (kind.to_string(), state_key.to_string()),
			| Err(_) =>
				(format!("<unresolvable short state key {shortstatekey}>"), String::new()),
		};

		state.insert(key, event_id.to_string());
	}

	state
}

fn compare(ctx: &mut Ctx<'_>, room: &Room<'_>, want: &State, have: &State) {
	for (key, event_id) in want {
		let subject = || format!("{} {} {:?}", room.id, key.0, key.1);
		match have.get(key) {
			| Some(have) if have == event_id => ctx.out.matched("state"),
			| Some(have) =>
				ctx.out
					.differs("state", subject(), format!("Synapse {event_id}, tuwunel {have}")),
			| None => ctx.out.missing(
				"state",
				subject(),
				format!("not in the current state (Synapse {event_id})"),
			),
		}
	}

	for (key, event_id) in have
		.iter()
		.filter(|(key, _)| !want.contains_key(*key))
	{
		ctx.out.extra(
			"state",
			format!("{} {} {:?}", room.id, key.0, key.1),
			format!("not in Synapse's state ({event_id})"),
		);
	}
}

async fn memberships(
	ctx: &mut Ctx<'_>,
	room: &Room<'_>,
	members: &BTreeMap<String, BTreeSet<String>>,
) {
	let cache = &ctx.services.state_cache;
	let lists = [
		(
			"join",
			cache
				.room_members(room.id)
				.map(ToString::to_string)
				.collect::<BTreeSet<_>>()
				.await,
		),
		(
			"invite",
			cache
				.room_members_invited(room.id)
				.map(ToString::to_string)
				.collect()
				.await,
		),
		(
			"knock",
			cache
				.room_members_knocked(room.id)
				.map(ToString::to_string)
				.collect()
				.await,
		),
	];

	let empty = BTreeSet::new();
	for (membership, have) in lists {
		let want = members.get(membership).unwrap_or(&empty);
		want.intersection(&have)
			.for_each(|_| ctx.out.matched("memberships"));
		for user in want.difference(&have) {
			ctx.out.missing(
				"memberships",
				format!("{} {user}", room.id),
				format!("not listed as {membership}"),
			);
		}

		for user in have.difference(want) {
			ctx.out.extra(
				"memberships",
				format!("{} {user}", room.id),
				format!("listed as {membership}, not in Synapse"),
			);
		}
	}
}

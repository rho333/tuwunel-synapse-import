//! How far each local user had read each room and thread, from their latest
//! read receipts, as tuwunel's live receipt path records it: a room is read
//! to its newest unthreaded or main-timeline receipt, and a thread to its own
//! receipt while that is newer than the room's unthreaded one, which clears
//! thread stamps live.

use std::collections::BTreeMap;

use ruma::{OwnedEventId, OwnedRoomId, OwnedUserId};
use tuwunel_core::Result;
use tuwunel_service::Services;

use super::{
	super::{
		receipts::event_count_in_room,
		source::{Source, user_id},
	},
	counts::thread_root,
};

/// Each local user's read receipts, resolved to imported timeline counts so an
/// unimported target or negative backfilled stream ordering cannot shadow an
/// earlier valid receipt.
const RECEIPTS: &str = "
SELECT user_id, room_id, COALESCE(thread_id, '') AS thread, event_id
FROM receipts_linearized
WHERE receipt_type IN ('m.read', 'm.read.private', 'org.matrix.msc2285.read.private')
	AND user_id IN (SELECT name FROM users)
ORDER BY user_id, room_id";

#[derive(Default)]
struct Latest {
	unthreaded: Option<u64>,
	main: Option<u64>,
	threads: BTreeMap<OwnedEventId, u64>,
}

pub(super) async fn import(services: &Services, source: &Source) -> Result {
	let mut latest: BTreeMap<(OwnedUserId, OwnedRoomId), Latest> = BTreeMap::new();

	// A row that doesn't parse or wasn't imported was reported by receipts.
	for row in source.rows(RECEIPTS, &[]).await? {
		let (Ok(user_id), Ok(room_id), Ok(event_id)) = (
			user_id(&row, 0),
			OwnedRoomId::try_from(row.get::<_, &str>(1)),
			OwnedEventId::try_from(row.get::<_, &str>(3)),
		) else {
			continue;
		};

		if !services.users.is_active(&user_id).await {
			continue;
		}

		let Ok(count) = event_count_in_room(services, &room_id, &event_id).await else {
			continue;
		};

		let entry = latest.entry((user_id, room_id)).or_default();
		match row.get::<_, &str>(2) {
			| "" => entry.unthreaded = Some(entry.unthreaded.map_or(count, |prev| prev.max(count))),
			| "main" => entry.main = Some(entry.main.map_or(count, |prev| prev.max(count))),
			| thread =>
				if let Ok(Some(root)) = thread_root(thread) {
					entry
						.threads
						.entry(root)
						.and_modify(|prev| *prev = (*prev).max(count))
						.or_insert(count);
				},
		}
	}

	for ((user_id, room_id), latest) in latest {
		let unthreaded_at = latest.unthreaded;
		let room = latest
			.unthreaded
			.into_iter()
			.chain(latest.main)
			.max();
		if let Some(count) = room {
			services
				.pusher
				.import_last_read(&user_id, &room_id, None, count);
		}

		for (root, count) in latest.threads {
			if unthreaded_at.is_none_or(|unthreaded| count > unthreaded) {
				services
					.pusher
					.import_last_read(&user_id, &room_id, Some(&root), count);
			}
		}
	}

	Ok(())
}

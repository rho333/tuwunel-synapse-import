//! Key backups: each live version, its number of keys, and the latest version.

use std::{collections::BTreeMap, pin::pin};

use futures::TryStreamExt;
use ruma::OwnedUserId;
use tuwunel_core::Result;

use super::{
	super::{
		backups::{parse_backup_version, session_key},
		source::user_id,
	},
	Ctx,
};

const VERSIONS: &str = "SELECT v.user_id, v.version, v.algorithm, v.auth_data FROM \
                        e2e_room_keys_versions v WHERE v.deleted = 0 ORDER BY v.user_id, \
                        v.version";

pub(super) async fn verify(ctx: &mut Ctx<'_>) -> Result {
	let services = ctx.services;
	let rows = ctx.source.rows(VERSIONS, &[]).await?;

	let mut latest: BTreeMap<OwnedUserId, String> = BTreeMap::new();
	for row in rows {
		let Ok(user_id) = user_id(&row, 0) else {
			continue;
		};

		if !ctx.active.contains(&user_id) {
			continue;
		}

		let raw_version: i64 = row.get(1);
		let version = raw_version.to_string();
		let subject = format!("{user_id} version {version}");
		if let Err(e) = parse_backup_version(raw_version, row.get(2), row.get(3)) {
			ctx.out.excuse("backups", subject, e);
			continue;
		}

		if services
			.key_backups
			.get_backup(&user_id, &version)
			.await
			.is_err()
		{
			ctx.out
				.missing("backups", subject, "no such backup version");
			continue;
		}

		ctx.out.matched("backups");
		let want = valid_room_keys(ctx, &user_id, raw_version).await?;
		let keys = services
			.key_backups
			.count_keys(&user_id, &version)
			.await;
		ctx.out.count(
			"room_keys",
			subject,
			want,
			keys.try_into().unwrap_or(u64::MAX),
		);
		latest.insert(user_id, version);
	}

	for (user_id, version) in latest {
		match services
			.key_backups
			.get_latest_backup_version(&user_id)
			.await
		{
			| Ok(have) if have == version => {},
			| have => ctx.out.differs(
				"backups",
				&user_id,
				format!("latest version: Synapse {version}, tuwunel {have:?}"),
			),
		}
	}

	Ok(())
}

async fn valid_room_keys(ctx: &mut Ctx<'_>, user_id: &OwnedUserId, version: i64) -> Result<u64> {
	let mut rows = pin!(
		ctx.source
			.stream(
				"SELECT room_id, session_id, coalesce(first_message_index, 0), \
				 coalesce(forwarded_count, 0), coalesce(is_verified, false), session_data FROM \
				 e2e_room_keys WHERE user_id = $1 AND version = $2",
				&[&user_id.as_str(), &version],
			)
			.await?
	);

	let mut valid: u64 = 0;
	while let Some(row) = rows.try_next().await? {
		match session_key(&row) {
			| Ok(_) => valid = valid.saturating_add(1),
			| Err(e) => ctx.out.excuse(
				"room_keys",
				format!("{user_id} version {version} {}", row.get::<_, &str>(1)),
				e,
			),
		}
	}

	Ok(valid)
}

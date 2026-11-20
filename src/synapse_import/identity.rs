//! Synapse's retired signing keys, so events they signed keep verifying, and
//! its cache of other servers' keys, so events those servers signed keep
//! verifying while a server is offline or no longer serves the key.
//!
//! The current key is not imported here: preflight requires this server to
//! already run on it, through `signing_key_path`.

use ruma::{
	MilliSecondsSinceUnixEpoch, OwnedServerName, OwnedServerSigningKeyId, UInt,
	api::federation::discovery::{OldVerifyKey, ServerSigningKeys, VerifyKey},
	serde::Base64,
};
use tuwunel_core::{Err, Result, err};
use tuwunel_service::Services;

use super::{Report, config::SynapseConfig, source::Source};

pub(super) async fn import(
	services: &Services,
	config: &SynapseConfig,
	source: &Source,
	report: &mut Report,
) -> Result {
	import_old_keys(services, config, report).await?;
	import_remote_keys(services, source, report).await
}

async fn import_old_keys(
	services: &Services,
	config: &SynapseConfig,
	report: &mut Report,
) -> Result {
	let server_name = services.globals.server_name().to_owned();
	let mut keys = ServerSigningKeys::new(server_name, MilliSecondsSinceUnixEpoch::now());

	for (key_id, old) in &config.old_signing_keys {
		let key_id: OwnedServerSigningKeyId = key_id
			.as_str()
			.try_into()
			.map_err(|e| err!("Synapse old signing key {key_id:?}: {e}"))?;

		let key =
			Base64::parse(&old.key).map_err(|e| err!("Synapse old signing key {key_id}: {e}"))?;

		let expired_ts = UInt::new(old.expired_ts)
			.map(MilliSecondsSinceUnixEpoch)
			.ok_or_else(|| err!("Synapse old signing key {key_id}: expired_ts out of range"))?;

		keys.old_verify_keys
			.insert(key_id, OldVerifyKey::new(expired_ts, key));
	}

	report.old_keys = keys.old_verify_keys.len();
	if report.old_keys > 0 {
		services.server_keys.add_signing_keys(keys).await;
	}

	Ok(())
}

/// Each row is a server's whole key document as Synapse fetched it, from the
/// server or a notary. Oldest first, so a later fetch of the same key wins.
async fn import_remote_keys(services: &Services, source: &Source, report: &mut Report) -> Result {
	let rows = source
		.rows(
			"SELECT server_name, key_id, key_json FROM server_keys_json ORDER BY ts_added_ms, \
			 server_name, key_id",
			&[],
		)
		.await?;

	for row in rows {
		let server: &str = row.get(0);
		let key_id: &str = row.get(1);
		let keys = parse_remote_keys(server, row.get(2));
		let Some(keys) = report.keep(format!("server_keys_json {server} {key_id}"), keys) else {
			continue;
		};

		if services.globals.server_is_ours(&keys.server_name) {
			continue;
		}

		services.server_keys.add_signing_keys(keys).await;
		Report::count(&mut report.server_keys);
	}

	if source.has_table("server_signature_keys").await? {
		import_signature_keys(services, source, report).await?;
	}

	Ok(())
}

/// Keys Synapse extracted into `server_signature_keys` (for instance from a
/// notary query) that may not have a matching `server_keys_json` document.
async fn import_signature_keys(
	services: &Services,
	source: &Source,
	report: &mut Report,
) -> Result {
	let rows = source
		.rows(
			"SELECT server_name, key_id, verify_key FROM server_signature_keys WHERE server_name \
			 IS NOT NULL AND key_id IS NOT NULL AND verify_key IS NOT NULL ORDER BY ts_added_ms, \
			 server_name, key_id",
			&[],
		)
		.await?;

	for row in rows {
		let server: &str = row.get(0);
		let key_id: &str = row.get(1);
		let parsed = parse_signature_key(server, key_id, row.get(2));
		let Some(keys) =
			report.keep(format_args!("server_signature_keys {server} {key_id}"), parsed)
		else {
			continue;
		};

		if services.globals.server_is_ours(&keys.server_name) {
			continue;
		}

		let already = services
			.server_keys
			.verify_keys_for(&keys.server_name)
			.await;
		if keys
			.verify_keys
			.keys()
			.all(|id| already.contains_key(id))
		{
			continue;
		}

		services.server_keys.add_signing_keys(keys).await;
		Report::count(&mut report.server_keys);
	}

	Ok(())
}

fn parse_signature_key(
	server: &str,
	key_id: &str,
	raw_key: Vec<u8>,
) -> Result<ServerSigningKeys> {
	let server_name: OwnedServerName = server
		.try_into()
		.map_err(|e| err!("invalid server name: {e}"))?;
	let key_id: OwnedServerSigningKeyId = key_id
		.try_into()
		.map_err(|e| err!("invalid key ID: {e}"))?;

	let mut keys = ServerSigningKeys::new(server_name, MilliSecondsSinceUnixEpoch::now());
	keys.verify_keys
		.insert(key_id, VerifyKey::new(Base64::new(raw_key)));

	Ok(keys)
}

fn parse_remote_keys(server: &str, json: &[u8]) -> Result<ServerSigningKeys> {
	let keys: ServerSigningKeys =
		serde_json::from_slice(json).map_err(|e| err!("invalid key document: {e}"))?;

	if keys.server_name != server {
		return Err!("key document is for {}", keys.server_name);
	}

	Ok(keys)
}

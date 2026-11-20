//! Appservice registrations (bridges), from the files homeserver.yaml lists, so
//! their tokens keep working. Their ghost users came across with the accounts.

use std::path::Path;

use ruma::api::appservice::Registration;
use tuwunel_core::{Result, err};
use tuwunel_service::Services;

use super::{Report, config::SynapseConfig};

pub(super) async fn import(
	services: &Services,
	config: &SynapseConfig,
	report: &mut Report,
) -> Result {
	for file in &config.app_service_config_files {
		match register(services, file).await {
			| Ok(()) => Report::count(&mut report.appservices),
			| Err(e) => report.skip(format!("appservice {}: {e}", file.display())),
		}
	}

	Ok(())
}

async fn register(services: &Services, file: &Path) -> Result {
	let contents = std::fs::read_to_string(file).map_err(|e| err!("cannot read it: {e}"))?;
	let registration = parse_registration(&contents)?;

	// Already registered by a run that stopped partway through this phase.
	if services
		.appservice
		.get_db_registration(&registration.id)
		.await
		.is_ok()
	{
		return Ok(());
	}

	services
		.appservice
		.register_appservice(registration)
		.await
}

/// Normalizes Synapse/Mautrix registration quirks before Ruma deserializes:
/// `de.sorunome.msc2409.push_ephemeral` becomes `receive_ephemeral`, and empty
/// YAML keys (`aliases:` -> `null`) become `[]`.
pub(super) fn parse_registration(yaml: &str) -> Result<Registration> {
	let mut value: serde_yaml::Value =
		serde_yaml::from_str(yaml).map_err(|e| err!("cannot parse it: {e}"))?;

	if let Some(map) = value.as_mapping_mut() {
		let legacy_ephemeral = serde_yaml::Value::String("de.sorunome.msc2409.push_ephemeral".into());
		let receive_ephemeral = serde_yaml::Value::String("receive_ephemeral".into());
		if !map.contains_key(&receive_ephemeral)
			&& let Some(val) = map.get(&legacy_ephemeral).cloned()
			&& val.is_bool()
		{
			map.insert(receive_ephemeral, val);
		}

		let protocols_key = serde_yaml::Value::String("protocols".into());
		if map
			.get(&protocols_key)
			.is_some_and(serde_yaml::Value::is_null)
		{
			map.remove(&protocols_key);
		}

		let namespaces_key = serde_yaml::Value::String("namespaces".into());
		if let Some(namespaces) = map
			.get_mut(&namespaces_key)
			.and_then(serde_yaml::Value::as_mapping_mut)
		{
			for kind in ["users", "aliases", "rooms"] {
				let key = serde_yaml::Value::String(kind.into());
				if namespaces
					.get(&key)
					.is_some_and(serde_yaml::Value::is_null)
				{
					namespaces.insert(key, serde_yaml::Value::Sequence(Vec::new()));
				}
			}
		}
	}

	serde_yaml::from_value(value).map_err(|e| err!("cannot parse it: {e}"))
}

#[cfg(test)]
mod tests {
	use super::parse_registration;

	#[test]
	fn mautrix_registration_normalizes_msc2409_ephemeral_and_null_namespaces() {
		let yaml = r"
id: whatsapp
url: http://localhost:29318
as_token: as_secret_token_1234567890
hs_token: hs_secret_token_1234567890
sender_localpart: whatsappbot
rate_limited: false
de.sorunome.msc2409.push_ephemeral: true
protocols:
namespaces:
  users:
    - exclusive: true
      regex: '^@whatsapp_.*:example\.com$'
  aliases:
  rooms:
";

		let reg = parse_registration(yaml).expect("valid Mautrix registration");
		assert_eq!(reg.id, "whatsapp");
		assert!(reg.receive_ephemeral, "MSC2409 push_ephemeral mapped to receive_ephemeral");
		assert_eq!(reg.namespaces.users.len(), 1);
		assert!(reg.namespaces.aliases.is_empty());
		assert!(reg.namespaces.rooms.is_empty());
		assert!(reg.protocols.is_none());
	}

	#[test]
	fn explicit_receive_ephemeral_takes_precedence_over_msc2409() {
		let yaml = "
id: signal
url: http://localhost:29328
as_token: as_secret_token_1234567890
hs_token: hs_secret_token_1234567890
sender_localpart: signalbot
receive_ephemeral: false
de.sorunome.msc2409.push_ephemeral: true
namespaces:
  users: []
  aliases: []
  rooms: []
";

		let reg = parse_registration(yaml).expect("valid registration");
		assert!(!reg.receive_ephemeral);
	}
}

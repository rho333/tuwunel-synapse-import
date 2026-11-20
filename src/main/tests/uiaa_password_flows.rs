#![cfg(test)]

use std::net::TcpListener;

use futures::future::join;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{Err, Result, ruma::UserId};
use tuwunel_service::{Services, oauth::sessions::Adoption, users::PASSWORD_SENTINEL};

use self::{client::wait_until_ready, uiaa_signing::exercise as exercise_signing};

#[expect(
	dead_code,
	reason = "Only listener readiness is shared with the client API harness."
)]
mod client;
mod uiaa_signing;

/// LDAP may supply password UIAA only for LDAP-origin accounts. A real local
/// password remains usable regardless of origin or whether LDAP is enabled.
#[test]
fn uiaa_password_flows_match_account_credentials() -> Result {
	for ldap_enabled in [false, true] {
		if ldap_enabled && !cfg!(feature = "ldap") {
			continue;
		}

		exercise_server(ldap_enabled)?;
	}

	Ok(())
}

fn exercise_server(ldap_enabled: bool) -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let args = Args::default_test(&["fresh", "cleanup"])
		.with_option("address=[\"127.0.0.1\"]")
		.with_option(format!("port={port}"))
		.with_option("listening=true")
		.with_option("log_global_default=false")
		.with_option(format!("ldap.enable={ldap_enabled}"))
		.with_option("jwt.enable=false")
		.with_option("identity_provider.test.client_id=\"uiaa-test-idp\"")
		.with_option("identity_provider.test.client_secret=\"test-secret\"")
		.with_option("identity_provider.test.brand=\"test\"")
		.with_option("identity_provider.test.issuer_url=\"https://idp.invalid\"");

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;

	runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");

		drop(listener);

		let exercise = async {
			let outcome = exercise(&services, &base, ldap_enabled).await;
			let shutdown = server.server.shutdown();

			outcome.and(shutdown)
		};

		let (run, outcome) = join(async_run(&server), exercise).await;

		drop(services);
		async_stop(&server).await?;
		run?;

		outcome
	})
}

async fn exercise(services: &Services, base: &str, ldap_enabled: bool) -> Result {
	wait_until_ready(services, base).await?;

	let password = json!([{"stages": ["m.login.password"]}]);
	let sso = json!([{"stages": ["m.login.sso"]}]);
	let password_and_sso = json!([
		{"stages": ["m.login.password"]},
		{"stages": ["m.login.sso"]},
	]);
	let no_flows = json!([]);
	let ldap = if ldap_enabled { &password } else { &no_flows };

	for (localpart, credential, origin, expected_flows) in [
		("local", "test-password", "password", &password),
		("ldap", PASSWORD_SENTINEL, "ldap", ldap),
		("sso", PASSWORD_SENTINEL, "sso", &sso),
		("sso-local", "test-password", "sso", &password_and_sso),
		("passwordless", PASSWORD_SENTINEL, "password", &no_flows),
	] {
		let user_id = UserId::parse_with_server_name(localpart, services.globals.server_name())?;
		services
			.users
			.create(&user_id, Some(credential), Some(origin))
			.await?;

		// Setting a real password changes the origin to password. Retain an
		// SSO origin here to cover accounts with both persisted credentials.
		if origin == "sso" && credential != PASSWORD_SENTINEL {
			services.db["userid_origin"].insert(&user_id, origin);
		}

		let token = format!("uiaa-password-flow-test-access-token-{localpart}");
		services
			.users
			.create_device(&user_id, None, (Some(&token), None), None, None, None)
			.await?;

		let response = services
			.client
			.clients
			.default
			.post(format!("{base}/_matrix/client/v3/account/deactivate"))
			.bearer_auth(&token)
			.json(&json!({}))
			.send()
			.await?;

		if response.status() != StatusCode::UNAUTHORIZED {
			return Err!("{localpart}: expected a UIAA challenge, got {}", response.status());
		}

		let challenge: Value = response.json().await?;
		if challenge["flows"] != *expected_flows {
			return Err!(
				"{localpart}, LDAP enabled={ldap_enabled}: expected flows {expected_flows}, got \
				 {}",
				challenge["flows"]
			);
		}

		if challenge["session"]
			.as_str()
			.is_none_or(str::is_empty)
		{
			return Err!("{localpart}: UIAA challenge omitted its session");
		}

		if origin == "sso"
			&& challenge["params"]["m.login.sso"]["identity_providers"]
				!= json!([{"id": "uiaa-test-idp"}])
		{
			return Err!("{localpart}: SSO challenge lost the configured provider binding");
		}
	}

	exercise_retention(services, base).await?;

	exercise_signing(services, base).await
}

#[tracing::instrument(level = "debug", skip_all)]
async fn exercise_retention(services: &Services, base: &str) -> Result {
	let user_id = UserId::parse_with_server_name("retention", services.globals.server_name())?;
	let password = "retention-password";
	let token = "uiaa-retention-test-access-token-valid";

	services
		.users
		.create(&user_id, Some(password), Some("password"))
		.await?;

	let sender = services
		.users
		.create_device(&user_id, None, (Some(token), None), None, None, None)
		.await?;

	let client = &services.client.clients.default;
	let url = format!("{base}/_matrix/client/v3/delete_devices");

	// An auth-only retry, a body too large to retain, and an emptied list.
	for (padding, emptied) in [(0, false), (8192, false), (0, true)] {
		let target = services
			.users
			.create_device(&user_id, None, (None, None), None, None, None)
			.await?;

		let initial = json!({"devices": [target], "padding": "x".repeat(padding)});
		let response = client
			.post(&url)
			.bearer_auth(token)
			.json(&initial)
			.send()
			.await?;

		assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

		let challenge: Value = response.json().await?;
		let session = challenge["session"]
			.as_str()
			.expect("UIAA session");

		let retained = services
			.uiaa
			.get_uiaa_request(&user_id, Some(&sender), session);

		assert_eq!(retained.is_some(), padding == 0);
		assert!(
			services
				.users
				.device_exists(&user_id, &target)
				.await
		);

		let auth = json!({
			"type": "m.login.password",
			"identifier": {"type": "m.id.user", "user": user_id},
			"password": password,
			"session": session
		});

		let retry = match (padding, emptied) {
			| (_, true) => json!({"devices": [], "auth": auth}),
			| (0, _) => json!({"auth": auth}),
			| _ => json!({"devices": [target], "auth": auth}),
		};

		let response = client
			.post(&url)
			.bearer_auth(token)
			.json(&retry)
			.send()
			.await?;

		assert_eq!(response.status(), StatusCode::OK);

		let exists = services
			.users
			.device_exists(&user_id, &target)
			.await;

		assert_eq!(exists, emptied);

		assert!(
			services
				.uiaa
				.get_uiaa_request(&user_id, Some(&sender), session)
				.is_none()
		);
	}

	Ok(())
}

/// On a multi-IdP server, SSO-origin accounts without a device-bound IdP fall
/// back to the user's adopted OAuth sessions when they point to one IdP.
#[test]
fn uiaa_multi_idp_sso_falls_back_to_unique_session_idp() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();
	let args = Args::default_test(&["fresh", "cleanup"])
		.with_option("address=[\"127.0.0.1\"]")
		.with_option(format!("port={port}"))
		.with_option("listening=true")
		.with_option("log_global_default=false")
		.with_option("jwt.enable=false")
		.with_option("identity_provider.one.client_id=\"idp-one\"")
		.with_option("identity_provider.one.client_secret=\"secret-one\"")
		.with_option("identity_provider.one.brand=\"one\"")
		.with_option("identity_provider.one.issuer_url=\"https://idp-one.invalid\"")
		.with_option("identity_provider.two.client_id=\"idp-two\"")
		.with_option("identity_provider.two.client_secret=\"secret-two\"")
		.with_option("identity_provider.two.brand=\"two\"")
		.with_option("identity_provider.two.issuer_url=\"https://idp-two.invalid\"");

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;

	runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");

		drop(listener);

		let exercise = async {
			let outcome = exercise_multi_idp(&services, &base).await;
			let shutdown = server.server.shutdown();

			outcome.and(shutdown)
		};

		let (run, outcome) = join(async_run(&server), exercise).await;

		drop(services);
		async_stop(&server).await?;
		run?;

		outcome
	})
}

async fn exercise_multi_idp(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;

	let one = services.oauth.providers.get_config("idp-one")?;
	let two = services.oauth.providers.get_config("idp-two")?;
	let sso = json!([{"stages": ["m.login.sso"]}]);
	let no_flows = json!([]);

	let challenge = async |token: &str| -> Result<Value> {
		let response = services
			.client
			.clients
			.default
			.post(format!("{base}/_matrix/client/v3/account/deactivate"))
			.bearer_auth(token)
			.json(&json!({}))
			.send()
			.await?;
		assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
		Ok(response.json().await?)
	};

	// 1. Single adopted session after set_origin("sso"), then a second session
	//    on the same IdP: both remain unambiguous and advertise idp-one.
	let unique = UserId::parse_with_server_name("multi-unique", services.globals.server_name())?;
	services
		.users
		.create(&unique, Some(PASSWORD_SENTINEL), Some("password"))
		.await?;
	services.users.set_origin(&unique, "sso");
	assert_eq!(services.users.origin(&unique).await?, "sso");
	assert_eq!(
		services
			.oauth
			.sessions
			.adopt_subject(&one, "sub-one-a", &unique)
			.await?,
		Adoption::Adopted
	);

	let unique_token = "uiaa-multi-idp-test-access-token-unique";
	services
		.users
		.create_device(&unique, None, (Some(unique_token), None), None, None, None)
		.await?;

	let body = challenge(unique_token).await?;
	assert_eq!(body["flows"], sso);
	assert_eq!(body["params"]["m.login.sso"]["identity_providers"], json!([{"id": "idp-one"}]));

	assert_eq!(
		services
			.oauth
			.sessions
			.adopt_subject(&one, "sub-one-b", &unique)
			.await?,
		Adoption::Adopted
	);
	let body = challenge(unique_token).await?;
	assert_eq!(body["flows"], sso);
	assert_eq!(body["params"]["m.login.sso"]["identity_providers"], json!([{"id": "idp-one"}]));

	// 2. Sessions across two different IdPs are ambiguous: advertise no SSO flow.
	let ambiguous =
		UserId::parse_with_server_name("multi-ambiguous", services.globals.server_name())?;
	services
		.users
		.create(&ambiguous, Some(PASSWORD_SENTINEL), Some("sso"))
		.await?;
	assert_eq!(
		services
			.oauth
			.sessions
			.adopt_subject(&one, "sub-ambig-1", &ambiguous)
			.await?,
		Adoption::Adopted
	);
	assert_eq!(
		services
			.oauth
			.sessions
			.adopt_subject(&two, "sub-ambig-2", &ambiguous)
			.await?,
		Adoption::Adopted
	);

	let ambig_token = "uiaa-multi-idp-test-access-token-ambiguous";
	services
		.users
		.create_device(&ambiguous, None, (Some(ambig_token), None), None, None, None)
		.await?;
	assert_eq!(challenge(ambig_token).await?["flows"], no_flows);

	// 3. SSO-origin account with no sessions on a multi-IdP server: no SSO flow.
	let none = UserId::parse_with_server_name("multi-none", services.globals.server_name())?;
	services
		.users
		.create(&none, Some(PASSWORD_SENTINEL), Some("sso"))
		.await?;
	let none_token = "uiaa-multi-idp-test-access-token-none";
	services
		.users
		.create_device(&none, None, (Some(none_token), None), None, None, None)
		.await?;
	assert_eq!(challenge(none_token).await?["flows"], no_flows);

	// 4. Non-SSO origin does not fall back to session IdPs.
	let non_sso = UserId::parse_with_server_name("multi-non-sso", services.globals.server_name())?;
	services
		.users
		.create(&non_sso, Some(PASSWORD_SENTINEL), Some("password"))
		.await?;
	assert_eq!(
		services
			.oauth
			.sessions
			.adopt_subject(&one, "sub-non-sso", &non_sso)
			.await?,
		Adoption::Adopted
	);
	let non_sso_token = "uiaa-multi-idp-test-access-token-non-sso";
	services
		.users
		.create_device(&non_sso, None, (Some(non_sso_token), None), None, None, None)
		.await?;
	assert_eq!(challenge(non_sso_token).await?["flows"], no_flows);

	Ok(())
}

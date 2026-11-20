//! Each user's auth state as MAS holds it, overriding Synapse's stale copy.

use std::collections::HashMap;

use ruma::OwnedUserId;
use tuwunel_core::{Result, utils::hash::mark_keyed_argon2_hash};
use tuwunel_service::Services;

use super::{
	super::{Report, source::user_id},
	Mas,
	config::Scheme,
};

pub(in super::super) struct MasUser {
	/// None when the user has no password, or one tuwunel can't verify.
	pub(in super::super) password_hash: Option<String>,
	pub(in super::super) admin: bool,
	pub(in super::super) locked: bool,
	pub(in super::super) deactivated: bool,
	pub(in super::super) guest: bool,
}

/// Each user with their newest password, which MAS treats as the current one.
const USERS: &str = "SELECT '@' || u.username || ':' || $1, u.can_request_admin IS TRUE, \
                     u.locked_at IS NOT NULL, u.deactivated_at IS NOT NULL, u.is_guest IS TRUE, \
                     p.hashed_password, p.version FROM users u LEFT JOIN LATERAL (SELECT \
                     hashed_password, version FROM user_passwords WHERE user_id = u.user_id \
                     ORDER BY created_at DESC LIMIT 1) p ON true ORDER BY u.username";

pub(in super::super) async fn users(
	mas: &Mas,
	services: &Services,
	report: &mut Report,
) -> Result<HashMap<OwnedUserId, MasUser>> {
	let server_name = services.globals.server_name().as_str();
	let rows = mas.source.rows(USERS, &[&server_name]).await?;

	let mut users = HashMap::with_capacity(rows.len());
	for row in rows {
		let user_id = user_id(&row, 0)?;
		let hash: Option<String> = row.get(5);
		let version: Option<i32> = row.get(6);

		let scheme = version.and_then(|version| mas.config.scheme(version));
		let usable = match scheme {
			| Some(scheme) => verifiable(scheme, services)?,
			| None => version.is_none(),
		};

		if !usable {
			report.skip(format!(
				"{user_id}: tuwunel can't verify their MAS password (scheme version {}); reset \
				 this password",
				version.unwrap_or_default()
			));
		}

		let password_hash = match hash.filter(|_| usable) {
			| Some(hash) if keyed(scheme)? => match mark_keyed_argon2_hash(&hash) {
				| Ok(marked) => Some(marked.into_string()),
				| Err(e) => {
					report.skip(format!("{user_id}: cannot mark MAS Argon2 hash: {e}"));
					None
				},
			},
			| hash => hash,
		};

		users.insert(user_id, MasUser {
			password_hash,
			admin: row.get(1),
			locked: row.get(2),
			deactivated: row.get(3),
			guest: row.get(4),
		});
	}

	Ok(users)
}

/// Refuses a scheme in use whose secret tuwunel hasn't been given. A secret set
/// to something else isn't refused, since two schemes can't share one setting:
/// that scheme's users are reported instead.
pub(super) async fn check_schemes(
	mas: &Mas,
	services: &Services,
	problem: &mut impl FnMut(String),
) -> Result {
	let rows = mas
		.source
		.rows(
			"SELECT DISTINCT p.version FROM users u JOIN LATERAL (SELECT version FROM \
			 user_passwords WHERE user_id = u.user_id ORDER BY created_at DESC LIMIT 1) p ON \
			 true",
			&[],
		)
		.await?;

	let versions: Vec<i32> = rows.iter().map(|row| row.get::<_, i32>(0)).collect();
	for problem_msg in scheme_problems(
		&versions,
		|v| mas.config.scheme(v),
		services.config.imported_bcrypt_pepper.as_deref(),
		services.config.imported_argon2_secret.as_deref(),
	)? {
		problem(problem_msg);
	}

	Ok(())
}

pub(super) fn scheme_problems<'a>(
	versions: &[i32],
	lookup: impl Fn(i32) -> Option<&'a Scheme>,
	our_bcrypt: Option<&str>,
	our_argon2: Option<&str>,
) -> Result<Vec<String>> {
	let mut problems = Vec::new();
	let mut seen_secrets: HashMap<&str, String> = HashMap::new();

	for &version in versions {
		let Some(scheme) = lookup(version) else {
			problems.push(format!("MAS's config has no password scheme version {version}"));
			continue;
		};

		let algo = scheme.algorithm.as_str();
		if !matches!(algo, "bcrypt" | "argon2id") {
			problems.push(format!(
				"MAS password scheme {version} uses {algo}, which tuwunel cannot verify (only \
				 bcrypt and argon2id are supported)"
			));
			continue;
		}

		if let Some(secret) = scheme.secret()? {
			if let Some(prev) = seen_secrets.get(algo)
				&& prev != &secret
			{
				problems.push(format!(
					"multiple MAS {algo} password schemes in use have different secrets, and \
					 tuwunel can hold only one"
				));
			} else {
				seen_secrets.insert(algo, secret.clone());
			}

			let unset = match algo {
				| "bcrypt" => our_bcrypt.is_none(),
				| "argon2id" => our_argon2.is_none(),
				| _ => false,
			};

			if unset {
				let setting = if algo == "bcrypt" {
					"imported_bcrypt_pepper"
				} else {
					"imported_argon2_secret"
				};

				problems.push(format!(
					"MAS password scheme {version} ({algo}) has a secret; set {setting} to it"
				));
			}
		}
	}

	Ok(problems)
}

fn verifiable(scheme: &Scheme, services: &Services) -> Result<bool> {
	let config = &services.config;
	let secret = scheme.secret()?;

	Ok(match scheme.algorithm.as_str() {
		| "bcrypt" => config.imported_bcrypt_pepper.as_deref() == secret.as_deref(),
		| "argon2id" => secret.is_none() || config.imported_argon2_secret == secret,
		| _ => false,
	})
}

/// Whether the scheme's hashes are Argon2id keyed with a secret, which login
/// tries only on hashes marked as keyed.
fn keyed(scheme: Option<&Scheme>) -> Result<bool> {
	let Some(scheme) = scheme.filter(|scheme| scheme.algorithm == "argon2id") else {
		return Ok(false);
	};

	Ok(scheme.secret()?.is_some())
}

#[cfg(test)]
mod tests {
	use super::{super::config::MasConfig, scheme_problems};

	fn parse_config(yaml: &str) -> MasConfig {
		serde_yaml::from_str(yaml).expect("valid MAS config")
	}

	#[test]
	fn scheme_problems_refuses_pbkdf2_missing_secrets_and_conflicting_secrets() {
		let cfg = parse_config(
			"
matrix:
  homeserver: example.com
passwords:
  schemes:
    - version: 1
      algorithm: pbkdf2
    - version: 2
      algorithm: bcrypt
      secret: pepper-a
    - version: 3
      algorithm: bcrypt
      secret: pepper-b
    - version: 4
      algorithm: argon2id
      secret: argon-secret
",
		);

		let problems = scheme_problems(&[1, 2, 3, 4], |v| cfg.scheme(v), None, None).unwrap();
		assert!(problems.iter().any(|p| p.contains("uses pbkdf2")), "{problems:?}");
		assert!(problems.iter().any(|p| p.contains("different secrets")), "{problems:?}");
		assert!(problems.iter().any(|p| p.contains("imported_bcrypt_pepper")), "{problems:?}");
		assert!(problems.iter().any(|p| p.contains("imported_argon2_secret")), "{problems:?}");
	}

	#[test]
	fn scheme_problems_allows_multiple_schemes_sharing_one_secret() {
		let cfg = parse_config(
			"
matrix:
  homeserver: example.com
passwords:
  schemes:
    - version: 1
      algorithm: bcrypt
      secret: same-pepper
    - version: 2
      algorithm: bcrypt
      secret: same-pepper
",
		);

		let problems =
			scheme_problems(&[1, 2], |v| cfg.scheme(v), Some("same-pepper"), None).unwrap();
		assert!(problems.is_empty(), "{problems:?}");
	}
}

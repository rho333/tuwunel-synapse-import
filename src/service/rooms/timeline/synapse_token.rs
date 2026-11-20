//! Pagination tokens a client kept from Synapse, for a server imported with
//! `--import-synapse`.
//!
//! A client's stored timeline (an SDK event cache, a saved sync) can hold gaps
//! whose `/messages` tokens Synapse issued, and no further token to paginate
//! with but those. The importer records each event's place in Synapse's
//! (topological, stream) order against its count here, and `/messages`
//! resolves a token it can't parse to the position after the last imported
//! event at or before it. A server that imported nothing has no rows, so its
//! answer to such a token is unchanged.

use std::pin::pin;

use futures::StreamExt;
use ruma::{RoomId, api::Direction};
use tuwunel_core::{
	implement,
	utils::stream::{ReadyExt, TryIgnore},
};
use tuwunel_matrix::pdu::PduCount;

use crate::rooms::short::ShortRoomId;

/// shortroomid, topological ordering, biased stream ordering.
type Key = (u64, u64, u64);
type Row = (Key, u64);

/// A Synapse room stream position.
///
/// `t<topological>-<stream>`, `s<stream>`, or `m<stream>~<instance>.<stream>...`
/// from a multi-writer deployment: the first `_`-separated part of a
/// `/messages` or `prev_batch` token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SynapsePosition {
	topological: Option<i64>,
	min_stream: i64,
	max_stream: i64,
}

/// Which `/messages` parameter a token came in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenBound {
	From,
	To,
}

impl SynapsePosition {
	#[must_use]
	pub fn parse(token: &str) -> Option<Self> {
		let room = token.split('_').next()?;
		let (kind, rest) = room.split_at_checked(1)?;
		match kind {
			| "t" => {
				let (topological, stream) = rest.split_once('-')?;
				let (min_stream, max_stream) = parse_stream_part(stream)?;
				Some(Self {
					topological: Some(topological.parse().ok()?),
					min_stream,
					max_stream,
				})
			},
			| "s" => {
				let stream = rest.parse().ok()?;
				Some(Self {
					topological: None,
					min_stream: stream,
					max_stream: stream,
				})
			},
			| "m" => {
				let (min_stream, max_stream) = parse_stream_part(rest)?;
				Some(Self {
					topological: None,
					min_stream,
					max_stream,
				})
			},
			| _ => None,
		}
	}

	/// The stream bound Synapse applies to a token without a topological part:
	/// the furthest writer's position paginating backwards, the slowest's
	/// forwards (`generate_pagination_bounds`).
	fn stream_bound(self, dir: Direction, bound: TokenBound) -> i64 {
		match (dir, bound) {
			| (Direction::Backward, TokenBound::From) | (Direction::Forward, TokenBound::To) =>
				self.max_stream,
			| _ => self.min_stream,
		}
	}
}

/// Records where Synapse ordered an imported event.
#[implement(super::Service)]
pub fn record_synapse_position(
	&self,
	shortroomid: ShortRoomId,
	topological: i64,
	stream: i64,
	count: PduCount,
) {
	let key = (shortroomid, topological.cast_unsigned(), bias(stream));
	self.db
		.roomsynapsepos_pducount
		.put(key, count.into_unsigned());
}

/// The count to page from for a token Synapse issued in this room, or `None`
/// when it isn't one or the room has no imported positions. Synapse's tokens
/// sit after the event they name: a backward `from` includes it, a forward one
/// starts after it, and `to` mirrors that (`generate_pagination_where_clause`).
#[implement(super::Service)]
pub async fn synapse_token_count(
	&self,
	room_id: &RoomId,
	token: &str,
	dir: Direction,
	bound: TokenBound,
) -> Option<PduCount> {
	let position = SynapsePosition::parse(token)?;
	let shortroomid = self
		.services
		.short
		.get_shortroomid(room_id)
		.await
		.ok()?;

	let map = &self.db.roomsynapsepos_pducount;
	let in_room = move |((room, ..), _): &Row| *room == shortroomid;
	let first = pin!(
		map.stream_from::<Key, u64, _>(&(shortroomid, 0_u64, 0_u64))
			.ignore_err()
	)
	.next()
	.await;

	if !first.as_ref().is_some_and(in_room) {
		return None;
	}

	let stream = bias(position.stream_bound(dir, bound));
	let upper = match position.topological {
		| Some(topological) => (shortroomid, topological.cast_unsigned(), stream),
		| None => (shortroomid, u64::MAX, u64::MAX),
	};

	// Without a topological part Synapse bounds the stream ordering alone.
	// Non-negative `s`/`m` tokens come from the live stream, so backfilled rows
	// (`stream_ordering < 0`, placed mid-timeline by depth) must not match them.
	let zero_stream = bias(0);
	let last = pin!(
		map.rev_stream_from::<Key, u64, _>(&upper)
			.ignore_err()
			.ready_take_while(in_room)
			.ready_filter(move |((_, _, row_stream), _)| {
				position.topological.is_some()
					|| (*row_stream <= stream && (stream < zero_stream || *row_stream >= zero_stream))
			})
	)
	.next()
	.await;

	let Some((_, count)) = last else {
		return Some(PduCount::min());
	};

	let after = PduCount::from_unsigned(count).into_signed();
	let inclusive = matches!(
		(dir, bound),
		(Direction::Backward, TokenBound::From) | (Direction::Forward, TokenBound::To)
	);

	Some(PduCount::from_signed(if inclusive { after.saturating_add(1) } else { after }))
}

fn parse_stream_part(rest: &str) -> Option<(i64, i64)> {
	let mut parts = rest.split('~');
	let min_stream: i64 = parts.next()?.parse().ok()?;
	let mut max_stream = min_stream;
	for part in parts.filter(|part| !part.is_empty()) {
		let (_, pos) = part.split_once('.')?;
		max_stream = max_stream.max(pos.parse().ok()?);
	}

	Some((min_stream, max_stream))
}

/// Offset binary, so negative (backfilled) stream orderings sort first.
fn bias(stream: i64) -> u64 { stream.wrapping_sub(i64::MIN).cast_unsigned() }

#[cfg(test)]
mod tests {
	use super::SynapsePosition;

	#[test]
	fn parses_synapse_room_positions() {
		let t = SynapsePosition::parse("t11-421_0_0_0_0_0_0_0_0_0").unwrap();
		assert_eq!(t, SynapsePosition {
			topological: Some(11),
			min_stream: 421,
			max_stream: 421
		});

		let s = SynapsePosition::parse("s422_17_0_3_8_1_1_5_0_1").unwrap();
		assert_eq!(s, SynapsePosition {
			topological: None,
			min_stream: 422,
			max_stream: 422
		});

		let m = SynapsePosition::parse("m400~1.410~2.405_9").unwrap();
		assert_eq!(m, SynapsePosition {
			topological: None,
			min_stream: 400,
			max_stream: 410
		});

		let tw = SynapsePosition::parse("t11-421~1.425~2.419_0_0").unwrap();
		assert_eq!(tw, SynapsePosition {
			topological: Some(11),
			min_stream: 421,
			max_stream: 425
		});

		assert!(SynapsePosition::parse("m5~").is_some());
	}

	#[test]
	fn refuses_tuwunel_and_garbage_tokens() {
		for token in ["", "123", "-7", "x1", "t11", "tA-1", "t-5-3", "s", "m1~x"] {
			assert_eq!(SynapsePosition::parse(token), None, "{token}");
		}
	}

	#[tokio::test]
	async fn stream_tokens_ignore_negative_backfill_and_exhaust_cleanly() -> tuwunel_core::Result {
		use ruma::{api::Direction, room_id};
		use tuwunel_core::config::Figment;
		use tuwunel_matrix::PduCount;

		use super::TokenBound;
		use crate::test_utils::fixture;

		let Some(fixture) = fixture(Figment::new()).await? else {
			return Ok(());
		};

		let services = &fixture.services;
		let room_id = room_id!("!synapsetok:localhost");
		let short = services.short.get_or_create_shortroomid(room_id).await;

		// Event 1 at stream 100, backfilled Event 2 mid-timeline at stream -50, Event 3 at stream 200.
		services.timeline.record_synapse_position(short, 1, 100, PduCount::Normal(10));
		services.timeline.record_synapse_position(short, 2, -50, PduCount::Normal(20));
		services.timeline.record_synapse_position(short, 3, 200, PduCount::Normal(30));

		// Forward From s150 must resolve after Event 1 (count 10), NOT jump to backfilled Event 2 (count 20)!
		let from_s150 = services
			.timeline
			.synapse_token_count(room_id, "s150_0", Direction::Forward, TokenBound::From)
			.await;
		assert_eq!(from_s150, Some(PduCount::Normal(10)));

		// Backward From before earliest event (t1-50) must return PduCount::min() so pagination exhausts.
		let exhausted = services
			.timeline
			.synapse_token_count(room_id, "t1-50", Direction::Backward, TokenBound::From)
			.await;
		assert_eq!(exhausted, Some(PduCount::min()));

		Ok(())
	}
}

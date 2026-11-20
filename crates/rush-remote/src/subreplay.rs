//! What a session's subagents said, in its replay: a stream's tail, a
//! history page and a full stream carry their messages, each tagged, as
//! the live stream tags it, with the call that started its run (Parent),
//! among the session's own frames by when each was written. So a chat
//! reopened shows the agents its subagents started, which only their own
//! transcripts hold. The fork's own: Go's replay carries the session's
//! transcript alone. See docs/remote.md.
//!
//! A session with no subagents, or whose agent can't read them, replays
//! as it always did.

use std::collections::HashSet;

use rush_core::adapter::{Adapter, Session as AgentSession, Timed};
use rush_core::event::{self, Event, PartKind};
use rush_core::time::Ms;

use crate::resume::{Place, pick};

/// A session's own history before a time, with when each event was
/// written, and its subagents' messages before it.
pub(crate) struct Replay {
    times: Vec<Ms>,
    evs: Vec<Event>,
    subs: Vec<Timed>,
}

/// s's replay with its subagents' messages; None when a can't read them
/// or s has none before before (0: ever).
pub(crate) fn read(a: &dyn Adapter, s: &AgentSession, before: Ms) -> Option<Replay> {
    let r = a.subagent_history_reader()?;
    let subs = r
        .subagent_history(s, before)
        .ok()
        .filter(|v| !v.is_empty())?;
    let (times, evs) = r.timed_history(s, before).ok()?.into_iter().unzip();
    Some(Replay { times, evs, subs })
}

/// session_tail's page of r: pick_turns' page of the session's own
/// events, which alone the cursor counts, with the subagent messages
/// written in its span: from its first event (from the start, for the
/// oldest page) to the first event the client has (to before, for the
/// newest). Pages split the session's events, and so its times, so no
/// message is sent on two.
pub(crate) fn tail(
    r: &Replay,
    at: Place,
    turns: usize,
    frames: usize,
    size: usize,
) -> (Vec<Vec<u8>>, Place, bool) {
    let p = pick(&r.evs, false, at, turns, frames, size);
    if p.frames.is_empty() {
        return (p.frames, p.next, p.more);
    }
    let from = if p.more { r.times[p.start] } else { Ms::MIN };
    let to = r.times.get(p.end).copied().unwrap_or(Ms::MAX);
    let used: usize = p.frames.iter().map(Vec::len).sum();
    let room = frames.saturating_sub(p.frames.len());
    let own: Vec<(Ms, Vec<u8>)> = p.src.iter().map(|&i| r.times[i]).zip(p.frames).collect();
    let out = place(own, &r.subs, from, to, room, size.saturating_sub(used));
    (out, p.next, p.more)
}

/// send_history's frames of r: the session's newest most events (the
/// bool says some were left out) with the subagent messages written from
/// the first of them on, within most frames in all.
pub(crate) fn newest(r: &Replay, most: usize) -> (Vec<Vec<u8>>, bool) {
    let cut = r.evs.len() > most;
    let first = r.evs.len().saturating_sub(most);
    let own: Vec<(Ms, Vec<u8>)> = (first..r.evs.len())
        .filter_map(|i| Some((r.times[i], event::marshal(&r.evs[i]).ok()?.into_bytes())))
        .collect();
    let from = if cut { r.times[first] } else { Ms::MIN };
    let room = most.saturating_sub(own.len());
    (place(own, &r.subs, from, Ms::MAX, room, usize::MAX), cut)
}

/// own, a page's frames with their times (oldest first), with the
/// messages of subs written in [from, to) put among them by time, each
/// after the page's frames written when it was or before.
///
/// The page's own frames fit its caps already; the messages get what's
/// left of them, room frames and bytes bytes. When that's not enough,
/// subagent messages are dropped, never the session's own (the cursor
/// counts those): first kept are those that start a run, so the agents
/// they start are known, then the newest. A message dropped is sent on
/// no other page either.
fn place(
    own: Vec<(Ms, Vec<u8>)>,
    subs: &[Timed],
    from: Ms,
    to: Ms,
    room: usize,
    bytes: usize,
) -> Vec<Vec<u8>> {
    let launches: HashSet<&str> = subs
        .iter()
        .filter_map(|(_, e)| match e {
            Event::Message(m) => Some(m.parent.as_str()),
            _ => None,
        })
        .collect();
    let picked: Vec<(Ms, Vec<u8>, bool)> = subs
        .iter()
        .filter(|(t, _)| *t >= from && *t < to)
        .filter_map(|(t, e)| {
            let b = event::marshal(e).ok()?.into_bytes();
            Some((*t, b, starts_run(e, &launches)))
        })
        .collect();
    // Who gets the room: runs' starts, then the newest.
    let mut order: Vec<usize> = (0..picked.len()).collect();
    order.sort_by_key(|&j| (!picked[j].2, std::cmp::Reverse(j)));
    let (mut keep, mut n, mut b) = (vec![false; picked.len()], room, bytes);
    for j in order {
        if n == 0 {
            break;
        }
        if picked[j].1.len() <= b {
            keep[j] = true;
            n -= 1;
            b -= picked[j].1.len();
        }
    }
    let mut subs = picked
        .into_iter()
        .zip(keep)
        .filter_map(|((t, b, _), k)| k.then_some((t, b)))
        .peekable();
    let mut out = Vec::with_capacity(own.len());
    for (t, f) in own {
        while let Some((st, _)) = subs.peek()
            && *st < t
        {
            out.extend(subs.next().map(|(_, b)| b));
        }
        out.push(f);
    }
    out.extend(subs.map(|(_, b)| b));
    out
}

/// Whether e calls one of the calls that start a run.
fn starts_run(e: &Event, launches: &HashSet<&str>) -> bool {
    let Event::Message(m) = e else {
        return false;
    };
    m.parts.iter().any(|p| {
        p.kind == PartKind::ToolCall
            && p.call
                .as_ref()
                .is_some_and(|c| launches.contains(c.id.as_str()))
    })
}

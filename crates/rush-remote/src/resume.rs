//! The tail of turns a session's stream starts with, and the history route
//! that pages back from it: when older turns exist, the tail's cut frame
//! carries a cursor, "<unix ms>.<turns>[.<events>]", which GET
//! /api/sessions/{id}/history?before= takes back for the turns before it.
//!
//! From the fork: internal/remote/resume.go (tailTurns, tailFrames and
//! tailBytes :26-28, sendFileTail :151, frame :166, cursor :181, place
//! :191, parseCursor :193, cutFrame :209, history :217, fileTail :253,
//! sessionTail :265, readTurns :275, isUserTurn :300, userTurns :316,
//! pickTurns :332).

use std::collections::BTreeMap;

use rush_core::adapter::{Adapter, Registry, Session as AgentSession, read_history};
use rush_core::event::{self, Event};
use rush_host::Info;
use rush_state::Time;
use serde::Serialize;

use crate::http::{Request, Response};
use crate::server::{agent_session, fail, sse, valid_id};

/// The tail: the newest TAIL_TURNS turns you started, within TAIL_FRAMES
/// frames and TAIL_BYTES bytes.
pub(crate) const TAIL_TURNS: usize = 10;
pub(crate) const TAIL_FRAMES: usize = 300;
pub(crate) const TAIL_BYTES: usize = 1 << 20;

/// What of the turns before a cursor's time the client has: the newest
/// skip turns, and the newest part events of the one before them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Place {
    pub skip: usize,
    pub part: usize,
}

/// Sends the tail of what a session's transcript holds from before before
/// (all of it when zero), with the cut first: a session whose host isn't
/// running, or one whose replay starts after its transcript's start.
pub(crate) fn send_file_tail(
    reg: &Registry,
    info: &Info,
    before: Time,
    emit: &mut dyn FnMut(Vec<u8>),
) {
    let before = if before.is_zero() {
        Time::now()
    } else {
        before
    };
    let (frames, next, more) = file_tail(
        reg,
        info,
        before,
        Place::default(),
        TAIL_TURNS,
        TAIL_FRAMES,
        TAIL_BYTES,
    );
    if more {
        emit(cut_frame(before, next));
    }
    for b in frames {
        emit(b);
    }
}

/// One of the stream's own frames, written as the app reads them: t, then
/// e.
#[derive(Serialize)]
struct Frame<'a> {
    t: &'a str,
    e: BTreeMap<&'a str, String>,
}

/// A cursor: the turns of a transcript written before a time, and how much
/// of the newest of them the client has, "<unix ms>.<turns>", and
/// ".<events>" when it has the newest events of the turn before those too.
/// The transcript only grows after the time, so what's before it holds
/// still.
pub(crate) fn cursor(before: Time, at: Place) -> String {
    let mut s = format!("{}.{}", before.to_ms(), at.skip);
    if at.part > 0 {
        s.push_str(&format!(".{}", at.part));
    }
    s
}

/// Reads a cursor back; None for anything cursor doesn't write (a sign is
/// taken, as Go's strconv takes one).
pub(crate) fn parse_cursor(s: &str) -> Option<(Time, Place)> {
    let (t, rest) = s.split_once('.')?;
    let (k, e) = match rest.split_once('.') {
        Some((k, e)) => (k, Some(e)),
        None => (rest, None),
    };
    let ms: i64 = t.parse().ok()?;
    let skip: i64 = k.parse().ok()?;
    let part: i64 = match e {
        Some(e) => e.parse().ok()?,
        None => 0,
    };
    if ms <= 0 || skip < 0 || part < 0 {
        return None;
    }
    Some((
        Time::from_ms(ms),
        Place {
            skip: usize::try_from(skip).ok()?,
            part: usize::try_from(part).ok()?,
        },
    ))
}

/// {"t":"cut","e":{"before":<cursor>}}: there's more before what follows.
pub(crate) fn cut_frame(before: Time, at: Place) -> Vec<u8> {
    let f = Frame {
        t: "cut",
        e: BTreeMap::from([("before", cursor(before, at))]),
    };
    serde_json::to_vec(&f).unwrap_or_default()
}

/// Answers GET /api/sessions/{id}/history?before=<cursor>: the TAIL_TURNS
/// turns before the cursor, oldest first, then a cut with the next cursor
/// when there are more. The response ends; it's not a stream.
pub(crate) fn history(reg: &Registry, r: &Request, id: &str) -> Response {
    if !valid_id(id) {
        return fail(404, "no such session");
    }
    let Some((before, at)) = parse_cursor(&r.query_get("before").unwrap_or_default()) else {
        return fail(400, "no such place in the history");
    };
    let info = match rush_host::read_info(id) {
        Ok(i) => i,
        Err(e) => return fail(404, &e.to_string()),
    };
    let (frames, next, more) =
        file_tail(reg, &info, before, at, TAIL_TURNS, TAIL_FRAMES, TAIL_BYTES);
    let mut body = Vec::new();
    for b in &frames {
        sse(&mut body, Some(b));
    }
    if more {
        sse(&mut body, Some(&cut_frame(before, next)));
    }
    let mut resp = Response::new(200, body);
    resp.set("Content-Type", "text/event-stream");
    resp.set("Cache-Control", "no-store");
    resp
}

/// The frames of the turns of info's transcript written before before,
/// leaving out what the client has (at): the newest turns of them, within
/// frames frames and size bytes, a bigger turn keeping its newest part.
/// It says what the client has then, and whether there's more. It reads
/// the transcript from its end where the agent can.
fn file_tail(
    reg: &Registry,
    info: &Info,
    before: Time,
    at: Place,
    turns: usize,
    frames: usize,
    size: usize,
) -> (Vec<Vec<u8>>, Place, bool) {
    match agent_session(reg, info) {
        Some((a, s)) => session_tail(&*a, &s, before, at, turns, frames, size),
        None => (Vec::new(), at, false),
    }
}

/// file_tail of s, a session of a.
pub(crate) fn session_tail(
    a: &dyn Adapter,
    s: &AgentSession,
    before: Time,
    at: Place,
    turns: usize,
    frames: usize,
    size: usize,
) -> (Vec<Vec<u8>>, Place, bool) {
    let (evs, cut) = read_turns(a, s, before, at.skip + turns);
    pick_turns(&evs, cut, at, turns, frames, size)
}

/// Reads the end of s's transcript before before, at least want user turns
/// of it and the start of the one before, where there are that many: a
/// tail reader's window grows until it holds them, however deep, so a page
/// never says it reached the start when it didn't. The bool is whether
/// there's more before what it read.
fn read_turns(a: &dyn Adapter, s: &AgentSession, before: Time, want: usize) -> (Vec<Event>, bool) {
    if let Some(tb) = a.history_tail_before_reader() {
        let mut most: i64 = 1 << 20;
        loop {
            let Ok((evs, cut)) = tb.history_tail_before(s, most, before.to_ms()) else {
                return (Vec::new(), false);
            };
            if !cut || user_turns(&evs) > want {
                return (evs, cut);
            }
            // Go's int64 would wrap here; no transcript gets that far.
            match most.checked_mul(4) {
                Some(m) => most = m,
                None => return (evs, cut),
            }
        }
    }
    if let Some(hr) = a.history_reader()
        && let Ok(evs) = read_history(hr, s, before.to_ms())
    {
        // Reads the whole transcript to keep its end.
        return (evs, false);
    }
    (Vec::new(), false)
}

/// A message that starts a turn: one you said, not one the agent put in as
/// yours. Claude's transcript reader marks every user text injected (the
/// host tells what you sent, live), so there it's what Claude Code's own
/// wrappers aren't.
pub(crate) fn is_user_turn(ev: &Event) -> bool {
    let Event::Message(m) = ev else {
        return false;
    };
    if m.role != "user" || !m.parent.is_empty() {
        return false;
    }
    m.parts.iter().any(|p| {
        p.kind == event::PartKind::Text
            && !p.text.trim().is_empty()
            && (!m.injected || rush_convo::transcript::injected(&p.text).is_none())
    })
}

fn user_turns(evs: &[Event]) -> usize {
    evs.iter().filter(|e| is_user_turn(e)).count()
}

/// file_tail's choice from evs, a transcript's end (cut: not all of it),
/// after what the client has (at). Turns go whole, newest first, while
/// they fit: next counts only those sent, so the next cursor reaches the
/// rest. Only a newest turn too big on its own is sent in part, its
/// newest, and next counts the events sent, so the next cursor reaches its
/// start.
pub(crate) fn pick_turns(
    evs: &[Event],
    cut: bool,
    at: Place,
    turns: usize,
    frames: usize,
    size: usize,
) -> (Vec<Vec<u8>>, Place, bool) {
    let starts: Vec<usize> = (0..evs.len()).filter(|&i| is_user_turn(&evs[i])).collect();
    if at.skip > starts.len() {
        return (Vec::new(), at, cut);
    }
    let whole = |n: usize| Place {
        skip: at.skip + n,
        part: 0,
    };
    let mut end = if at.skip > 0 {
        starts[starts.len() - at.skip]
    } else {
        evs.len()
    };
    end = end.saturating_sub(at.part);
    let (mut out, mut bytes, mut n): (Vec<Vec<u8>>, usize, usize) = (Vec::new(), 0, 0);
    // Turn k is evs[starts[k]..end]; k -1 is what comes before the first,
    // part of a turn whose start wasn't read when cut.
    let mut k = starts.len() as isize - 1 - at.skip as isize;
    while k >= -1 {
        if n == turns {
            return (out, whole(n), k >= 0 || cut || end > 0);
        }
        let lo = if k >= 0 {
            starts[k as usize].min(end)
        } else if cut {
            return (out, whole(n), true);
        } else {
            0
        };
        let (mut seg, mut src, mut sb) = (Vec::new(), Vec::new(), 0usize);
        for (j, ev) in evs[lo..end].iter().enumerate() {
            if let Ok(b) = event::marshal(ev) {
                sb += b.len();
                seg.push(b.into_bytes());
                src.push(lo + j); // the event each of seg is
            }
        }
        if out.len() + seg.len() > frames || bytes + sb > size {
            let Some(mut i) = seg.len().checked_sub(1).filter(|_| out.is_empty()) else {
                return (out, whole(n), true);
            };
            // One turn bigger than the tail: its newest part.
            sb = seg[i].len();
            while i > 0 && seg.len() - i < frames && sb + seg[i - 1].len() <= size {
                sb += seg[i - 1].len();
                i -= 1;
            }
            let had = if n == 0 { at.part } else { 0 };
            let next = Place {
                skip: at.skip + n,
                part: had + end - src[i],
            };
            return (seg.split_off(i), next, true);
        }
        seg.append(&mut out);
        out = seg;
        bytes += sb;
        end = lo;
        if k >= 0 {
            n += 1;
        }
        k -= 1;
    }
    (out, whole(n), false)
}

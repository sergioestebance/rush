//! A session's replay carries what its subagents said (subreplay): read
//! by the Claude adapter from a real session's transcripts, scrubbed
//! (rush-claude's testdata/nested), and from ones written here.

use rush_claude::Claude;
use rush_core::adapter::{Session as AgentSession, read_history};
use rush_state::Time;
use serde_json::Value;

use crate::resume::{Place, TAIL_BYTES, TAIL_FRAMES, TAIL_TURNS, pick_turns, session_tail};
use crate::server::FULL_MOST;
use crate::subreplay;

/// The session: its background agent, outer, started one of its own,
/// inner.
const NESTED: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../rush-claude/testdata/nested/83285775-c7df-411b-a3d9-f931d0a3adff.jsonl"
);
/// The calls that started outer and inner.
const OUTER: &str = "toolu_01Y53coUkU79eVYmqFCQby5g";
const INNER: &str = "toolu_0193Yh1rE4bhRGPd45DYp68h";

fn at(path: &str) -> AgentSession {
    AgentSession {
        transcript: path.into(),
        ..AgentSession::default()
    }
}

fn read(frames: &[Vec<u8>]) -> Vec<Value> {
    frames
        .iter()
        .map(|b| serde_json::from_slice(b).unwrap())
        .collect()
}

/// A message frame's Parent; "" for the session's own, or another frame.
fn parent(v: &Value) -> &str {
    if v["t"] != "message" {
        return "";
    }
    v["e"]["Parent"].as_str().unwrap_or("")
}

/// Whether a frame's message calls id.
fn calls(v: &Value, id: &str) -> bool {
    v["e"]["Parts"]
        .as_array()
        .is_some_and(|ps| ps.iter().any(|p| p["Call"]["ID"] == id))
}

fn text(v: &Value) -> String {
    v["e"]["Parts"][0]["Text"].as_str().unwrap_or("").into()
}

/// The tail without subagents: what the transcript alone gives.
fn alone(s: &AgentSession, frames: usize) -> (Vec<Vec<u8>>, Place, bool) {
    let evs = read_history(&Claude::new(), s, Time::now().to_ms()).unwrap();
    pick_turns(
        &evs,
        false,
        Place::default(),
        TAIL_TURNS,
        frames,
        TAIL_BYTES,
    )
}

/// A reopened chat's tail carries its subagents' messages, each Parent the
/// call that started its run, after that call: outer's Agent call, which
/// starts inner, is outer's; inner's messages are inner's.
#[test]
fn a_reopened_chat_carries_what_its_subagents_said() {
    let (frames, next, more) = session_tail(
        &Claude::new(),
        &at(NESTED),
        Time::now(),
        Place::default(),
        TAIL_TURNS,
        TAIL_FRAMES,
        TAIL_BYTES,
    );
    let vs = read(&frames);
    let pos = |f: &dyn Fn(&Value) -> bool| vs.iter().position(f).unwrap();
    let outer_starts = pos(&|v| parent(v).is_empty() && calls(v, OUTER));
    let outer_says = pos(&|v| parent(v) == OUTER);
    let inner_starts = pos(&|v| parent(v) == OUTER && calls(v, INNER));
    let inner_says = pos(&|v| parent(v) == INNER);
    assert!(
        outer_starts < outer_says && outer_says < inner_starts && inner_starts < inner_says,
        "{outer_starts} {outer_says} {inner_starts} {inner_says}"
    );
    let of = |call: &str| vs.iter().filter(|v| parent(v) == call).count();
    assert_eq!((of(OUTER), of(INNER)), (4, 7));
    assert!(!more, "{next:?}");
}

/// The session's own frames, and the cursor, are what they were without
/// its subagents; a session with none replays exactly as before.
#[test]
fn its_own_frames_are_as_they_were() {
    let d = tempfile::tempdir().unwrap();
    let lone = d.path().join("s.jsonl");
    std::fs::copy(NESTED, &lone).unwrap();
    let lone = at(&lone.to_string_lossy());
    let before = alone(&lone, TAIL_FRAMES);
    let tail = |s: &AgentSession| {
        session_tail(
            &Claude::new(),
            s,
            Time::now(),
            Place::default(),
            TAIL_TURNS,
            TAIL_FRAMES,
            TAIL_BYTES,
        )
    };
    assert_eq!(tail(&lone), before);
    let (frames, next, more) = tail(&at(NESTED));
    let own: Vec<Vec<u8>> = frames
        .into_iter()
        .filter(|b| parent(&serde_json::from_slice(b).unwrap()).is_empty())
        .collect();
    assert_eq!((own, next, more), before);
}

/// When the caps leave no room for all of them, subagent messages go and
/// the session's own stay; the one that starts a run is kept first.
#[test]
fn the_caps_drop_subagent_messages_first() {
    let own = alone(&at(NESTED), TAIL_FRAMES).0.len();
    let (frames, _, _) = session_tail(
        &Claude::new(),
        &at(NESTED),
        Time::now(),
        Place::default(),
        TAIL_TURNS,
        own + 1,
        TAIL_BYTES,
    );
    let vs = read(&frames);
    let subs: Vec<&Value> = vs.iter().filter(|v| !parent(v).is_empty()).collect();
    assert_eq!((vs.len(), subs.len()), (own + 1, 1));
    assert!(
        parent(subs[0]) == OUTER && calls(subs[0], INNER),
        "{}",
        subs[0]
    );
}

/// A full stream carries every one of them with the session's whole
/// history.
#[test]
fn a_full_stream_carries_them_all() {
    let r = subreplay::read(&Claude::new(), &at(NESTED), 0).unwrap();
    let (frames, cut) = subreplay::newest(&r, FULL_MOST);
    let vs = read(&frames);
    let of = |call: &str| vs.iter().filter(|v| parent(v) == call).count();
    assert_eq!((of(OUTER), of(INNER), cut), (4, 7, false));
    // None for a session without subagents.
    let d = tempfile::tempdir().unwrap();
    let lone = d.path().join("s.jsonl");
    std::fs::copy(NESTED, &lone).unwrap();
    assert!(subreplay::read(&Claude::new(), &at(&lone.to_string_lossy()), 0).is_none());
}

/// 2026-11-20T14:00:00Z and sec seconds.
fn ts(sec: u32) -> String {
    format!("2026-11-20T14:{:02}:{:02}.000Z", sec / 60, sec % 60)
}

/// Pages a turn at a time: each subagent message is sent once, on the page
/// whose span holds when it was written, a background agent's on the
/// later turns it wrote during.
#[test]
fn pages_send_each_subagent_message_once() {
    let d = tempfile::tempdir().unwrap();
    let main = d.path().join("s.jsonl");
    let mut t = String::new();
    for i in 0..6 {
        let s = 10 * i;
        t += &format!(
            r#"{{"type":"user","timestamp":"{}","message":{{"role":"user","content":"q{i}"}}}}"#,
            ts(s)
        );
        t.push('\n');
        if i == 1 {
            t += &format!(
                r#"{{"type":"assistant","timestamp":"{}","message":{{"id":"m{i}","role":"assistant","content":[{{"type":"tool_use","id":"toolu_bg","name":"Agent","input":{{"description":"bg","prompt":"go","run_in_background":true}}}}]}}}}"#,
                ts(s + 1)
            );
            t.push('\n');
            t += &format!(
                r#"{{"type":"user","timestamp":"{}","message":{{"role":"user","content":[{{"type":"tool_result","tool_use_id":"toolu_bg","content":"launched"}}]}}}}"#,
                ts(s + 2)
            );
            t.push('\n');
        }
        t += &format!(
            r#"{{"type":"assistant","timestamp":"{}","message":{{"id":"r{i}","role":"assistant","content":[{{"type":"text","text":"a{i}"}}]}}}}"#,
            ts(s + 3)
        );
        t.push('\n');
    }
    std::fs::write(&main, t).unwrap();
    let sub = d.path().join("s/subagents");
    std::fs::create_dir_all(&sub).unwrap();
    std::fs::write(
        sub.join("agent-a1.meta.json"),
        r#"{"agentType":"general-purpose","toolUseId":"toolu_bg","spawnDepth":1}"#,
    )
    .unwrap();
    let said = [14, 16, 25, 35, 45, 55, 65];
    let mut b = String::new();
    for (j, s) in said.iter().enumerate() {
        b += &format!(
            r#"{{"type":"assistant","isSidechain":true,"timestamp":"{}","message":{{"id":"s{j}","role":"assistant","content":[{{"type":"text","text":"s{j}"}}]}}}}"#,
            ts(*s)
        );
        b.push('\n');
    }
    std::fs::write(sub.join("agent-a1.jsonl"), b).unwrap();
    let s = at(&main.to_string_lossy());
    let (mut place, mut pages) = (Place::default(), Vec::new());
    loop {
        let (frames, next, more) = session_tail(
            &Claude::new(),
            &s,
            Time::now(),
            place,
            1,
            TAIL_FRAMES,
            TAIL_BYTES,
        );
        let vs = read(&frames);
        let subs: Vec<String> = vs
            .iter()
            .filter(|v| parent(v) == "toolu_bg")
            .map(text)
            .collect();
        let own: Vec<String> = vs
            .iter()
            .filter(|v| parent(v).is_empty() && v["e"]["Role"] == "user")
            .map(text)
            .filter(|t| t.starts_with('q'))
            .collect();
        pages.push((own, subs));
        if !more {
            break;
        }
        place = next;
    }
    let sv = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let want: Vec<(Vec<String>, Vec<String>)> = vec![
        (sv(&["q5"]), sv(&["s5", "s6"])),
        (sv(&["q4"]), sv(&["s4"])),
        (sv(&["q3"]), sv(&["s3"])),
        (sv(&["q2"]), sv(&["s2"])),
        (sv(&["q1"]), sv(&["s0", "s1"])),
        (sv(&["q0"]), sv(&[])),
    ];
    assert_eq!(pages, want);
}

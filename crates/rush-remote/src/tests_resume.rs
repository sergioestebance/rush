//! The scroll-back cursor, the tail of turns a stream starts with and the
//! history route that pages back from it. After the fork's
//! internal/remote/resume_test.go (TestOlderTurnsComeFromTheHistoryRoute,
//! TestTheRuntimeAnswersAPositionIsStopped, coldTail, TestABigTurnKeepsItsNewestPart,
//! TestOlderTurnsComeFromTheHistoryRouteInWholeTurns,
//! TestOlderTurnsComeFromTheHistoryRouteHoweverDeep,
//! TestABigTurnsStartComesFromTheHistoryRoute, TestABigTurnsPagesReachItsStart).

use std::io::Write;
use std::os::unix::net::UnixListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rush_core::adapter::{
    Adapter, Features, HistoryReader, HistoryTailBeforeReader, Kind, Level, Profile, Registry,
    Result as AdapterResult, Session as AgentSession, TranscriptLocator,
};
use rush_core::event::{Event, Message, Part, PartKind, Status};
use rush_core::time::Ms;
use rush_host::Info;
use rush_state::Time;
use serde_json::Value;

use crate::config::{Config, update_config};
use crate::http::{Body, Request, Response};
use crate::resume::{
    Place, TAIL_BYTES, TAIL_FRAMES, TAIL_TURNS, cursor, cut_frame, is_user_turn, parse_cursor,
    pick_turns, session_tail,
};
use crate::server::Server;
use crate::testutil::home;

const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// An agent whose transcript is lines of {"At","User","Text"}: a turn is a
/// user line and the answers after it (the fork's turnsAgent).
struct Turns;

impl Adapter for Turns {
    fn kind(&self) -> Kind {
        Kind::from("turns")
    }
    fn name(&self) -> String {
        "Turns".into()
    }
    fn features(&self) -> Features {
        Features::default()
    }
    fn level(&self) -> Level {
        Level::default()
    }
    fn profiles(&self) -> Vec<Profile> {
        Vec::new()
    }
    rush_core::has!(
        history_reader: HistoryReader,
        transcript_locator: TranscriptLocator
    );
}

impl TranscriptLocator for Turns {
    fn transcript_path(&self, _: &Profile, cwd: &str, sid: &str) -> String {
        format!("{cwd}/{sid}")
    }
}

impl HistoryReader for Turns {
    fn history(
        &self,
        s: &AgentSession,
        before: Ms,
        _: usize,
        each: &mut dyn FnMut(Vec<Event>) -> bool,
    ) -> AdapterResult<()> {
        let b = std::fs::read_to_string(&s.transcript)?;
        let mut out = Vec::new();
        for l in b.lines() {
            let Ok(v) = serde_json::from_str::<Value>(l) else {
                continue;
            };
            if before != 0 && v["At"].as_i64().unwrap_or(0) >= before {
                continue;
            }
            let user = v["User"].as_bool().unwrap_or(false);
            out.push(said(
                if user { "user" } else { "assistant" },
                v["Text"].as_str().unwrap_or(""),
            ));
        }
        each(out);
        Ok(())
    }
}

/// An agent whose transcript is 200 turns, of which a tail of most bytes
/// holds one per MiB; it notes each window it's asked for (the fork's
/// deepAgent).
#[derive(Default)]
struct Deep {
    asked: Arc<Mutex<Vec<i64>>>,
}

impl Adapter for Deep {
    fn kind(&self) -> Kind {
        Kind::from("deep")
    }
    fn name(&self) -> String {
        "Deep".into()
    }
    fn features(&self) -> Features {
        Features::default()
    }
    fn level(&self) -> Level {
        Level::default()
    }
    fn profiles(&self) -> Vec<Profile> {
        Vec::new()
    }
    rush_core::has!(history_tail_before_reader: HistoryTailBeforeReader);
}

impl HistoryTailBeforeReader for Deep {
    fn history_tail_before(
        &self,
        _: &AgentSession,
        most: i64,
        _: Ms,
    ) -> AdapterResult<(Vec<Event>, bool)> {
        self.asked.lock().unwrap().push(most);
        let n = 200.min((most >> 20) as usize);
        let evs = (200 - n..200)
            .map(|i| said("user", &format!("q{i}")))
            .collect();
        Ok((evs, n < 200))
    }
}

fn said(role: &str, text: &str) -> Event {
    Event::Message(Message {
        role: role.into(),
        parts: vec![Part {
            kind: PartKind::Text,
            text: text.into(),
            ..Part::default()
        }],
        ..Message::default()
    })
}

/// Messages of texts: a "q…" is something you said, the rest answers.
fn msgs(texts: &[&str]) -> Vec<Event> {
    texts
        .iter()
        .map(|t| {
            said(
                if t.starts_with('q') {
                    "user"
                } else {
                    "assistant"
                },
                t,
            )
        })
        .collect()
}

/// The texts of the message frames among frames.
fn texts<B: AsRef<[u8]>>(frames: &[B]) -> Vec<String> {
    frames
        .iter()
        .filter_map(|b| {
            let v: Value = serde_json::from_slice(b.as_ref()).ok()?;
            (v["t"] == "message").then(|| {
                v["e"]["Parts"][0]["Text"]
                    .as_str()
                    .unwrap_or("")
                    .to_string()
            })
        })
        .collect()
}

fn kind_of(frame: &str) -> String {
    let v: Value = serde_json::from_str(frame).unwrap_or(Value::Null);
    v["t"].as_str().unwrap_or("").to_string()
}

fn before_of(frame: &str) -> String {
    let v: Value = serde_json::from_str(frame).unwrap();
    v["e"]["before"].as_str().unwrap_or("").to_string()
}

fn server() -> Arc<Server> {
    update_config(|c| c.web = true).unwrap();
    let reg = Registry::new();
    reg.register(Turns);
    let mut s = Server::new(Config::default(), TOKEN.into(), reg);
    s.bad_guess = Duration::ZERO;
    Arc::new(s)
}

fn get(s: &Arc<Server>, method: &str, path: &str) -> Response {
    let r = Request::new(method, path)
        .with_header("Host", "rush.test")
        .with_header("Authorization", &format!("Bearer {TOKEN}"));
    s.handler()(&r)
}

/// Whether a stream's frames so far are enough.
type Stop = Box<dyn Fn(&[String]) -> bool>;

/// A writer that keeps each frame, and ends the stream (cancelling its
/// request) once stop says it has enough, or after 20 s.
struct Frames {
    got: Arc<Mutex<Vec<String>>>,
    stop: Stop,
    ctx: crate::ctx::Ctx,
    by: std::time::Instant,
}

impl Write for Frames {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        let mut got = self.got.lock().unwrap();
        if (self.stop)(&got) || std::time::Instant::now() >= self.by {
            self.ctx.cancel();
            return Err(std::io::Error::other("enough"));
        }
        if let Some(d) = std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.strip_prefix("data: "))
        {
            got.push(d.trim_end().to_string());
        }
        if (self.stop)(&got) {
            self.ctx.cancel();
        }
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The frames of path's stream, up to and with the first that stop says
/// is enough.
fn stream(s: &Arc<Server>, path: &str, stop: impl Fn(&[String]) -> bool + 'static) -> Vec<String> {
    let r = Request::new("GET", path)
        .with_header("Host", "rush.test")
        .with_header("Authorization", &format!("Bearer {TOKEN}"));
    let ctx = r.ctx.clone();
    let resp = s.handler()(&r);
    assert_eq!(resp.status, 200, "{path}");
    let got: Arc<Mutex<Vec<String>>> = Arc::default();
    if let Body::Stream(f) = resp.body {
        f(&mut Frames {
            got: got.clone(),
            stop: Box::new(stop),
            ctx,
            by: std::time::Instant::now() + Duration::from_secs(20),
        });
    }
    got.lock().unwrap().clone()
}

fn through_synced(s: &Arc<Server>, path: &str) -> Vec<String> {
    stream(s, path, |got| {
        got.last().is_some_and(|f| kind_of(f) == "synced")
    })
}

/// One page of the history route: its frames, the response ended.
fn page(s: &Arc<Server>, id: &str, cur: &str) -> Vec<String> {
    let r = get(
        s,
        "GET",
        &format!("/api/sessions/{id}/history?before={cur}"),
    );
    assert_eq!(r.status, 200, "{id} {cur}");
    assert_eq!(r.get("Content-Type"), "text/event-stream");
    assert_eq!(r.get("Cache-Control"), "no-store");
    let Body::Bytes(b) = r.body else {
        panic!("the history route streams: it should end");
    };
    String::from_utf8(b)
        .unwrap()
        .split("\n\n")
        .filter_map(|f| f.strip_prefix("data: ").map(str::to_string))
        .collect()
}

/// A session of the turns agent, its transcript lines (at ms, user, text)
/// written an hour ago.
fn session(id: &str, lines: &[(i64, bool, String)], host_pid: i64, replay_from: Time) -> Info {
    let dir = rush_host::dir(id);
    std::fs::create_dir_all(&dir).unwrap();
    let cwd = dir.join("cwd");
    std::fs::create_dir_all(&cwd).unwrap();
    let mut t = String::new();
    for (at, user, text) in lines {
        t.push_str(&serde_json::json!({"At": at, "User": user, "Text": text}).to_string());
        t.push('\n');
    }
    std::fs::write(cwd.join("sid"), t).unwrap();
    let info = Info {
        id: id.into(),
        session_id: "sid".into(),
        kind: "turns".into(),
        cwd: cwd.to_string_lossy().into_owned(),
        state: if host_pid == 0 { "stopped" } else { "idle" }.into(),
        host_pid,
        proto: rush_host::PROTO,
        replay_from,
        updated_at: Time::now(),
        ..Info::default()
    };
    std::fs::write(dir.join("info.json"), serde_json::to_vec(&info).unwrap()).unwrap();
    info
}

fn an_hour_ago() -> i64 {
    Time::now().to_ms() - 3_600_000
}

/// n turns, each a question "q<i>" and its answer "a<i>", a second apart.
fn turns(n: usize) -> Vec<(i64, bool, String)> {
    let at = an_hour_ago();
    (0..n)
        .flat_map(|i| {
            [
                (at + 2000 * i as i64, true, format!("q{i}")),
                (at + 2000 * i as i64 + 1000, false, format!("a{i}")),
            ]
        })
        .collect()
}

fn range(from: usize, to: usize) -> Vec<String> {
    (from..to)
        .flat_map(|i| [format!("q{i}"), format!("a{i}")])
        .collect()
}

#[test]
fn a_cursor_reads_back_as_it_was_written() {
    let at = Time::from_ms(1_760_000_000_000);
    let cases = [
        (Place { skip: 0, part: 0 }, "1760000000000.0"),
        (Place { skip: 10, part: 0 }, "1760000000000.10"),
        (Place { skip: 3, part: 7 }, "1760000000000.3.7"),
        (Place { skip: 0, part: 300 }, "1760000000000.0.300"),
    ];
    for (place, want) in cases {
        assert_eq!(cursor(at, place), want);
        let (t, p) = parse_cursor(want).unwrap_or_else(|| panic!("{want} doesn't read"));
        assert_eq!((t.to_ms(), p), (at.to_ms(), place), "{want}");
    }
    // Go's strconv takes a sign and a zero part.
    for (s, place) in [
        ("1760000000000.1.0", Place { skip: 1, part: 0 }),
        ("+1760000000000.+2", Place { skip: 2, part: 0 }),
    ] {
        assert_eq!(parse_cursor(s).map(|(_, p)| p), Some(place), "{s}");
    }
}

#[test]
fn bad_cursors_name_no_place() {
    for s in [
        "",
        "nonsense",
        "1760000000000",
        "1760000000000.",
        ".3",
        "0.1",
        "-5.1",
        "1760000000000.-1",
        "1760000000000.1.-2",
        "1760000000000.1.2.3",
        "1760000000000.x",
        "1760000000000.1.",
        "17600x.1",
        "99999999999999999999.1",
    ] {
        assert!(parse_cursor(s).is_none(), "{s:?} read as a place");
    }
}

#[test]
fn the_cut_frame_says_t_then_e() {
    let at = Time::from_ms(1_760_000_000_000);
    assert_eq!(
        cut_frame(at, Place { skip: 10, part: 0 }),
        br#"{"t":"cut","e":{"before":"1760000000000.10"}}"#
    );
    assert_eq!(
        cut_frame(at, Place { skip: 1, part: 5 }),
        br#"{"t":"cut","e":{"before":"1760000000000.1.5"}}"#
    );
}

#[test]
fn a_turn_starts_with_what_you_said() {
    let msg = |role: &str, parent: &str, injected: bool, parts: Vec<Part>| {
        Event::Message(Message {
            role: role.into(),
            parent: parent.into(),
            injected,
            parts,
            ..Message::default()
        })
    };
    let text = |t: &str| Part {
        kind: PartKind::Text,
        text: t.into(),
        ..Part::default()
    };
    let note = "<task-notification><summary>done</summary></task-notification>";
    let cases = [
        (
            "you said it",
            msg("user", "", false, vec![text("hi")]),
            true,
        ),
        (
            "an answer",
            msg("assistant", "", false, vec![text("hi")]),
            false,
        ),
        (
            "a subagent's",
            msg("user", "call1", false, vec![text("hi")]),
            false,
        ),
        ("blank", msg("user", "", false, vec![text("  \n")]), false),
        (
            "no text",
            msg(
                "user",
                "",
                false,
                vec![Part {
                    kind: PartKind::ToolResult,
                    ..Part::default()
                }],
            ),
            false,
        ),
        // Claude's reader marks every user text injected: plain words are yours.
        (
            "marked, plain",
            msg("user", "", true, vec![text("hi")]),
            true,
        ),
        (
            "marked, wrapped",
            msg("user", "", true, vec![text(note)]),
            false,
        ),
        (
            "wrapped, unmarked",
            msg("user", "", false, vec![text(note)]),
            true,
        ),
        (
            "wrapped then plain",
            msg("user", "", true, vec![text(note), text("and this")]),
            true,
        ),
        (
            "not a message",
            Event::Status(Status {
                busy: false,
                text: "hi".into(),
            }),
            false,
        ),
    ];
    for (name, ev, want) in cases {
        assert_eq!(is_user_turn(&ev), want, "{name}");
    }
}

/// A pick_turns case: its name, the transcript's end, whether that's cut,
/// the place asked after, the turns and frames asked for, then the texts
/// sent, the place given and whether there's more.
type PickCase<'a> = (
    &'a str,
    &'a [&'a str],
    bool,
    Place,
    usize,
    usize,
    &'a [&'a str],
    Place,
    bool,
);

#[test]
fn pick_turns_cases() {
    let p = |skip, part| Place { skip, part };
    let three = ["q0", "a0", "q1", "a1", "q2", "a2"];
    let big = ["q0", "r0", "r1", "r2", "r3", "r4"];
    #[rustfmt::skip]
    let cases: &[PickCase] = &[
        ("the newest turns", &three, false, p(0, 0), 2, 300, &["q1", "a1", "q2", "a2"], p(2, 0), true),
        ("the rest", &three, false, p(2, 0), 2, 300, &["q0", "a0"], p(3, 0), false),
        ("nothing older", &three, false, p(3, 0), 2, 300, &[], p(3, 0), false),
        ("past the start", &three, false, p(4, 0), 2, 300, &[], p(4, 0), false),
        ("past what was read", &three, true, p(4, 0), 2, 300, &[], p(4, 0), true),
        ("frames bound whole turns", &three, false, p(0, 0), 10, 3, &["q2", "a2"], p(1, 0), true),
        ("what's before the first turn", &["a", "q0", "a0"], false, p(0, 0), 10, 300, &["a", "q0", "a0"], p(1, 0), false),
        ("a turn whose start wasn't read", &["a", "q0", "a0"], true, p(0, 0), 10, 300, &["q0", "a0"], p(1, 0), true),
        ("a big turn's newest part", &big, false, p(0, 0), 10, 3, &["r2", "r3", "r4"], p(0, 3), true),
        ("the rest of it", &big, false, p(0, 3), 10, 3, &["q0", "r0", "r1"], p(1, 0), false),
        ("its next part", &big, false, p(0, 3), 10, 2, &["r0", "r1"], p(0, 5), true),
        ("a part after whole turns", &["q0", "a0", "q1", "r0", "r1", "r2"], false, p(0, 2), 10, 300, &["q0", "a0", "q1", "r0"], p(2, 0), false),
        ("a big turn under one sent", &["q0", "r0", "r1", "r2", "r3", "r4", "q1", "a1"], false, p(1, 0), 10, 3, &["r2", "r3", "r4"], p(1, 3), true),
    ];
    for (name, evs, cut, at, turns, frames, want, next, more) in cases {
        let (out, n, m) = pick_turns(&msgs(evs), *cut, *at, *turns, *frames, TAIL_BYTES);
        assert_eq!(
            (texts(&out), n, m),
            (want.iter().map(|s| s.to_string()).collect(), *next, *more),
            "{name}"
        );
    }
}

/// Pages of turns too big for one page each send whole turns, and the next
/// cursor goes on from the oldest sent: every turn comes once.
#[test]
fn older_turns_page_in_whole_turns() {
    let mut evs = Vec::new();
    for i in 0..30 {
        evs.push(said("user", &format!("q{i}")));
        for j in 0..39 {
            evs.push(said("assistant", &format!("{i}.{j}")));
        }
    }
    let mut seen: Vec<String> = Vec::new();
    let mut skip = 0;
    loop {
        let (frames, next, more) = pick_turns(
            &evs,
            false,
            Place { skip, part: 0 },
            TAIL_TURNS,
            TAIL_FRAMES,
            TAIL_BYTES,
        );
        let n = next.skip - skip;
        let qs: Vec<String> = texts(&frames)
            .into_iter()
            .filter(|t| t.starts_with('q'))
            .collect();
        assert_eq!((qs.len(), frames.len()), (n, 40 * n), "at {skip}");
        seen.splice(0..0, qs);
        skip += n;
        if !more {
            break;
        }
    }
    let want: Vec<String> = (0..30).map(|i| format!("q{i}")).collect();
    assert_eq!(seen, want);
}

/// A turn bigger than the tail keeps its newest part.
#[test]
fn a_big_turn_keeps_its_newest_part() {
    let mut evs = vec![said("user", "go")];
    evs.extend((0..500).map(|i| said("assistant", &i.to_string())));
    let (frames, next, more) = pick_turns(
        &evs,
        false,
        Place::default(),
        TAIL_TURNS,
        TAIL_FRAMES,
        TAIL_BYTES,
    );
    assert_eq!(frames.len(), TAIL_FRAMES);
    assert_eq!(
        next,
        Place {
            skip: 0,
            part: TAIL_FRAMES
        }
    );
    assert!(more);
    assert!(String::from_utf8_lossy(frames.last().unwrap()).contains(r#""499""#));
}

/// A big turn's pages go on from the oldest event sent, with its start
/// last.
#[test]
fn a_big_turns_pages_reach_its_start() {
    let mut evs = vec![said("user", "go")];
    evs.extend((0..700).map(|i| said("assistant", &i.to_string())));
    let (mut at, mut all) = (Place::default(), 0);
    for page in 0.. {
        let (frames, next, more) = pick_turns(&evs, false, at, TAIL_TURNS, TAIL_FRAMES, TAIL_BYTES);
        all += frames.len();
        if !more {
            assert_eq!((page, all, next.skip), (2, evs.len(), 1));
            assert!(String::from_utf8_lossy(&frames[0]).contains(r#""go""#));
            return;
        }
        assert_eq!(next.part, all, "page {page}");
        at = next;
    }
}

/// A page deeper than any window yet read keeps reading, its window four
/// times bigger each time: it never says it reached the start when it
/// didn't.
#[test]
fn a_deep_page_grows_its_window() {
    let deep = Deep::default();
    let s = AgentSession::default();
    let (frames, next, more) = session_tail(
        &deep,
        &s,
        Time::now(),
        Place { skip: 180, part: 0 },
        TAIL_TURNS,
        TAIL_FRAMES,
        TAIL_BYTES,
    );
    assert_eq!((next.skip, more), (190, true));
    assert!(String::from_utf8_lossy(&frames[0]).contains(r#""q10""#));
    let mib = 1i64 << 20;
    assert_eq!(
        *deep.asked.lock().unwrap(),
        vec![mib, 4 * mib, 16 * mib, 64 * mib, 256 * mib]
    );
    let (_, next, more) = session_tail(
        &deep,
        &s,
        Time::now(),
        Place { skip: 190, part: 0 },
        TAIL_TURNS,
        TAIL_FRAMES,
        TAIL_BYTES,
    );
    assert_eq!((next.skip, more), (200, false));
    // A shallow page stops growing once it holds more turns than it wants.
    let shallow = Deep::default();
    session_tail(
        &shallow,
        &s,
        Time::now(),
        Place::default(),
        TAIL_TURNS,
        TAIL_FRAMES,
        TAIL_BYTES,
    );
    assert_eq!(*shallow.asked.lock().unwrap(), vec![mib, 4 * mib, 16 * mib]);
}

/// A cold chat's tail is its newest ten turns, within the frames and bytes.
#[test]
fn a_cold_chat_gets_only_its_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.jsonl");
    let at = an_hour_ago();
    let mut t = String::new();
    for i in 0..200i64 {
        t.push_str(&format!(
            "{{\"At\":{},\"User\":true,\"Text\":\"q{i}\"}}\n",
            at + 2 * i
        ));
        t.push_str(&format!(
            "{{\"At\":{},\"Text\":\"a{i} {}\"}}\n",
            at + 2 * i + 1,
            "w".repeat(20 << 10)
        ));
    }
    std::fs::write(&path, t).unwrap();
    let s = AgentSession {
        transcript: path.to_string_lossy().into_owned(),
        ..AgentSession::default()
    };
    let (frames, next, more) = session_tail(
        &Turns,
        &s,
        Time::now(),
        Place::default(),
        TAIL_TURNS,
        TAIL_FRAMES,
        TAIL_BYTES,
    );
    assert_eq!(
        (next, more),
        (
            Place {
                skip: TAIL_TURNS,
                part: 0
            },
            true
        )
    );
    let size: usize = frames.iter().map(Vec::len).sum();
    assert!(
        frames.len() <= TAIL_FRAMES && size <= TAIL_BYTES,
        "{} frames, {size} bytes",
        frames.len()
    );
    let qs: Vec<String> = texts(&frames)
        .into_iter()
        .filter(|t| t.starts_with('q'))
        .collect();
    assert_eq!(qs, (190..200).map(|i| format!("q{i}")).collect::<Vec<_>>());
}

/// A stopped session's stream starts with cut, then its newest ten turns;
/// the history route pages back from cut's cursor, each page ending with
/// the next cursor, the oldest with none.
#[test]
fn older_turns_come_from_the_history_route() {
    let _h = home();
    let s = server();
    session("d2d2d2d2", &turns(30), 0, Time::default());
    let tail = through_synced(&s, "/api/sessions/d2d2d2d2/events");
    let kinds: Vec<String> = tail.iter().map(|f| kind_of(f)).collect();
    assert_eq!(&kinds[..3], ["reset", "info", "cut"], "{kinds:?}");
    assert_eq!(kinds.len(), 24, "{kinds:?}");
    assert!(
        tail[2].starts_with(r#"{"t":"cut","e":{"before":""#),
        "{}",
        tail[2]
    );
    assert!(before_of(&tail[2]).ends_with(".10"), "{}", tail[2]);
    assert_eq!(texts(&tail), range(20, 30));
    let mut cur = before_of(&tail[2]);
    for (n, want) in [range(10, 20), range(0, 10)].into_iter().enumerate() {
        let got = page(&s, "d2d2d2d2", &cur);
        assert_eq!(texts(&got), want, "page {n}");
        let last = got.last().unwrap();
        if n == 0 {
            assert_eq!(kind_of(last), "cut", "page {n}");
            assert!(before_of(last).ends_with(".20"), "{last}");
            cur = before_of(last);
        } else {
            assert_ne!(kind_of(last), "cut", "the oldest page has a cut");
        }
    }
}

/// A running host's replay starts where its ring does: the stream's tail
/// is the turns of the transcript before that, cut first.
#[test]
fn a_running_hosts_stream_starts_with_the_turns_before_its_replay() {
    let _h = home();
    let s = server();
    let lines = turns(30);
    let from = Time::from_ms(lines[50].0); // q25
    let info = session("e6e6e6e6", &lines, std::process::id() as i64, from);
    host_says(info);
    let got = through_synced(&s, "/api/sessions/e6e6e6e6/events");
    let kinds: Vec<String> = got.iter().map(|f| kind_of(f)).collect();
    assert_eq!(&kinds[..2], ["reset", "cut"], "{kinds:?}");
    assert_eq!(&kinds[kinds.len() - 2..], ["info", "synced"], "{kinds:?}");
    assert_eq!(before_of(&got[1]), format!("{}.10", from.to_ms()));
    assert_eq!(texts(&got), range(15, 25));
}

/// Writes info as its session's info file.
fn write_info(info: Info) -> Info {
    let p = rush_host::dir(&info.id).join("info.json");
    std::fs::write(p, serde_json::to_vec(&info).unwrap()).unwrap();
    info
}

/// A host on the socket of info's session that sends each client its info
/// and nothing else.
fn host_says(info: Info) {
    let ln = UnixListener::bind(rush_host::sock_path(&info.id)).unwrap();
    std::thread::spawn(move || {
        for c in ln.incoming() {
            let Ok(mut c) = c else { return };
            let mut l = br#"{"info":"#.to_vec();
            l.extend(serde_json::to_vec(&info).unwrap());
            l.extend_from_slice(b",\"type\":\"agtop_info\"}\n");
            let _ = c.write_all(&l);
            std::thread::spawn(move || {
                let mut sink = Vec::new();
                let _ = std::io::Read::read_to_end(&mut c, &mut sink);
            });
        }
    });
}

/// A host that resumed a transcript (a stopped chat woken by a send) has
/// only what came after its start in its ring: the turns before it come
/// from the transcript, as a cold chat's would, with the cut first. One
/// that started the conversation has nothing before its start to send.
#[test]
fn a_woken_hosts_stream_starts_with_the_turns_before_it_started() {
    let _h = home();
    let s = server();
    let lines = turns(30);
    let mut info = session(
        "e7e7e7e7",
        &lines,
        std::process::id() as i64,
        Time::default(),
    );
    info.started_at = Time::from_ms(lines[59].0 + 1000);
    host_says(write_info(info.clone()));
    let got = through_synced(&s, "/api/sessions/e7e7e7e7/events");
    let kinds: Vec<String> = got.iter().map(|f| kind_of(f)).collect();
    assert_eq!(&kinds[..2], ["reset", "cut"], "{kinds:?}");
    assert_eq!(&kinds[kinds.len() - 2..], ["info", "synced"], "{kinds:?}");
    assert_eq!(
        before_of(&got[1]),
        format!("{}.10", info.started_at.to_ms())
    );
    assert_eq!(texts(&got), range(20, 30));
    assert_eq!(page(&s, "e7e7e7e7", &before_of(&got[1]))[1..].len(), 20);

    let mut fresh = session(
        "e8e8e8e8",
        &lines,
        std::process::id() as i64,
        Time::default(),
    );
    fresh.started_at = Time::from_ms(lines[0].0 - 1000);
    host_says(write_info(fresh));
    let kinds: Vec<String> = through_synced(&s, "/api/sessions/e8e8e8e8/events")
        .iter()
        .map(|f| kind_of(f))
        .collect();
    assert_eq!(kinds, ["reset", "info", "synced"]);
}

/// A stream of all of it (?full=1) is as it was: every event, no cursor.
#[test]
fn a_full_stream_sends_all_of_it() {
    let _h = home();
    let s = server();
    session("d3d3d3d3", &turns(30), 0, Time::default());
    let got = stream(&s, "/api/sessions/d3d3d3d3/events?full=1", |got| {
        got.len() >= 62
    });
    let kinds: Vec<String> = got.iter().map(|f| kind_of(f)).collect();
    assert_eq!(&kinds[..2], ["reset", "info"]);
    assert!(!kinds.contains(&"cut".to_string()), "{kinds:?}");
    assert_eq!(texts(&got), range(0, 30));
}

/// A turn bigger than the tail sends its newest part, and the history
/// route gives back the rest of it, its prompt last, each once, though
/// that rest is bigger than a page.
#[test]
fn a_big_turns_start_comes_from_the_history_route() {
    let _h = home();
    let s = server();
    for (id, small) in [("d6d6d6d6", false), ("d7d7d7d7", true)] {
        let at = an_hour_ago();
        let mut lines = Vec::new();
        if small {
            lines.push((at, true, "hi".to_string()));
            lines.push((at + 1, false, "hello".to_string()));
        }
        lines.push((at + 999, true, "go".to_string()));
        for i in 0..720 {
            lines.push((at + 1000 + i, false, format!("answer {i}")));
        }
        let want: Vec<String> = lines.iter().map(|l| l.2.clone()).collect();
        session(id, &lines, 0, Time::default());
        let tail = through_synced(&s, &format!("/api/sessions/{id}/events"));
        let kinds: Vec<String> = tail.iter().map(|f| kind_of(f)).collect();
        assert_eq!(&kinds[..3], ["reset", "info", "cut"], "small {small}");
        assert!(before_of(&tail[2]).ends_with(".0.300"), "{}", tail[2]);
        let mut got = texts(&tail);
        assert_eq!(got.len(), TAIL_FRAMES);
        let mut cur = before_of(&tail[2]);
        for _ in 0..5 {
            let p = page(&s, id, &cur);
            got.splice(0..0, texts(&p));
            match p.last() {
                Some(l) if kind_of(l) == "cut" => cur = before_of(l),
                _ => {
                    cur.clear();
                    break;
                }
            }
        }
        assert!(cur.is_empty(), "small {small}: no end in sight");
        assert_eq!(got, want, "small {small}");
    }
}

/// What the history route refuses, as the rest of the API does.
#[test]
fn the_history_route_refuses() {
    let _h = home();
    let s = server();
    session("d8d8d8d8", &turns(2), 0, Time::default());
    std::fs::create_dir_all(rush_host::dir("d9d9d9d9")).unwrap(); // no info
    let cases = [
        (
            "GET",
            "/api/sessions/zzzzzzzz/history?before=1760000000000.0",
            404,
            Some("no such session"),
        ),
        (
            "GET",
            "/api/sessions/d8d8d8d8/history",
            400,
            Some("no such place in the history"),
        ),
        (
            "GET",
            "/api/sessions/d8d8d8d8/history?before=nonsense",
            400,
            Some("no such place in the history"),
        ),
        (
            "GET",
            "/api/sessions/d9d9d9d9/history?before=1760000000000.0",
            404,
            None,
        ),
        (
            "POST",
            "/api/sessions/d8d8d8d8/history?before=1760000000000.0",
            405,
            None,
        ),
    ];
    for (method, path, code, error) in cases {
        let r = get(&s, method, path);
        assert_eq!(r.status, code, "{method} {path}");
        if let Some(e) = error {
            assert_eq!(
                String::from_utf8_lossy(r.bytes()),
                format!("{{\"error\":\"{e}\"}}\n"),
                "{path}"
            );
        }
        if code == 405 {
            assert_eq!(r.get("Allow"), "GET, HEAD");
        }
    }
}

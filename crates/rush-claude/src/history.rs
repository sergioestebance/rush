//! A Claude Code transcript read back as rush's own events, a slice at a
//! time: its messages, from before a time when one is given. A subagent's
//! lines and Claude Code's own notes are left out, as the conversation
//! leaves them out. Port of internal/adapters/claude/history.go:31-71
//! (History), sliced; following it as it grows is this port's own
//! (Go's Claude adapter has no FollowHistory: its pane tails the
//! transcript itself), as is reading it with each line's time and its
//! subagents' transcripts beside it, for a replay to carry what they said
//! (subagent_history).

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use rush_core::adapter::{self, Follow, MessageDecoder, Session, Slice, Timed, WaitOn};
use rush_core::event::Event;
use rush_core::time::Ms;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::json::{Str, ms, nd};

/// What a history read needs of a transcript line.
#[derive(Deserialize, Default)]
#[serde(default)]
struct Line<'a> {
    #[serde(rename = "type", borrow)]
    typ: Str<'a>,
    #[serde(rename = "isMeta", deserialize_with = "nd")]
    is_meta: bool,
    #[serde(rename = "isSidechain", deserialize_with = "nd")]
    is_sidechain: bool,
    #[serde(borrow)]
    timestamp: Str<'a>,
    #[serde(borrow)]
    message: Option<&'a RawValue>,
    #[serde(rename = "toolUseResult", borrow)]
    tool_use_result: Option<&'a RawValue>,
    /// A queue-operation's: enqueue, remove, dequeue.
    #[serde(borrow)]
    operation: Str<'a>,
    /// What a queue-operation queued.
    #[serde(borrow)]
    content: Option<&'a RawValue>,
    /// Which line this is: Claude Code may write one twice.
    #[serde(borrow)]
    uuid: Str<'a>,
    /// An attachment line's: a prompt sent while the agent was busy is one.
    #[serde(borrow)]
    attachment: Option<&'a RawValue>,
}

/// The background tasks a read has told ended already, by id, call and
/// status: Claude Code queues a notice, takes it, and may write it as a
/// message too, and each is told once.
type Told = std::collections::HashSet<String>;

/// What one line comes to.
enum Took {
    Events(Vec<Event>),
    Skip,
    /// Written at or after the time asked for: the read ends.
    Past,
}

fn take(dec: &mut dyn MessageDecoder, b: &[u8], before: Ms, told: &mut Told) -> Took {
    let Some(l) = parse(b) else {
        return Took::Skip;
    };
    if l.is_meta || l.is_sidechain {
        return Took::Skip;
    }
    if before != 0 {
        let at = ms(&l.timestamp);
        if at != 0 && at >= before {
            return Took::Past;
        }
    }
    take_line(dec, &l, told)
}

/// take, and when the line was written (0 when it doesn't say). side
/// reads a subagent's own transcript, every line of which is a sidechain.
fn take_at(
    dec: &mut dyn MessageDecoder,
    b: &[u8],
    before: Ms,
    told: &mut Told,
    side: bool,
) -> (Ms, Took) {
    let Some(l) = parse(b) else {
        return (0, Took::Skip);
    };
    let at = ms(&l.timestamp);
    if l.is_meta || (l.is_sidechain && !side) {
        return (at, Took::Skip);
    }
    if before != 0 && at != 0 && at >= before {
        return (at, Took::Past);
    }
    (at, take_line(dec, &l, told))
}

/// A transcript line, its newline left off; None for a blank or broken
/// one.
fn parse(b: &[u8]) -> Option<Line<'_>> {
    let b = b.strip_suffix(b"\n").unwrap_or(b);
    if b.is_empty() {
        return None;
    }
    serde_json::from_slice::<Line>(b).ok()
}

/// What a line read in, before the time asked for, comes to.
fn take_line(dec: &mut dyn MessageDecoder, l: &Line, told: &mut Told) -> Took {
    if let Some(text) = notice(l) {
        let done: Vec<Event> = ended(&text)
            .into_iter()
            .filter(|d| told.insert(format!("{},{},{}", d.id, d.call_id, d.status)))
            .map(Event::TaskDone)
            .collect();
        return if done.is_empty() {
            Took::Skip
        } else {
            Took::Events(done)
        };
    }
    if &*l.typ == "attachment" {
        return match queued_prompt(l) {
            Some(m) => Took::Events(dec.message("user", &m, None)),
            None => Took::Skip,
        };
    }
    if &*l.typ != "user" && &*l.typ != "assistant" {
        return Took::Skip;
    }
    let Some(m) = l.message else {
        return Took::Skip;
    };
    // A line written again (its uuid read already) is the same message.
    if !l.uuid.is_empty() && !told.insert(format!("uuid {}", &*l.uuid)) {
        return Took::Skip;
    }
    Took::Events(dec.message(&l.typ, m, l.tool_use_result))
}

/// The message of a prompt sent while the agent was busy: Claude Code
/// writes no user line for it, only a queued_command attachment when the
/// agent takes it (a task notice queued the same way is told by notice).
fn queued_prompt(l: &Line) -> Option<Box<RawValue>> {
    #[derive(Deserialize)]
    struct Queued<'a> {
        #[serde(rename = "type", borrow)]
        typ: Str<'a>,
        #[serde(rename = "commandMode", borrow, default)]
        mode: Str<'a>,
        #[serde(borrow)]
        prompt: Option<&'a RawValue>,
    }
    let q: Queued = serde_json::from_str(l.attachment?.get()).ok()?;
    if &*q.typ != "queued_command" || &*q.mode != "prompt" {
        return None;
    }
    let m = format!(r#"{{"role":"user","content":{}}}"#, q.prompt?.get());
    RawValue::from_string(m).ok()
}

/// SubagentHistoryReader::timed_history of the transcript at path: what
/// read gives of it before `before`, each event with when its line was
/// written, a line that doesn't say (or says earlier) taking the time of
/// the one before. side reads a subagent's transcript.
pub(crate) fn read_timed(path: &Path, before: Ms, side: bool) -> adapter::Result<Vec<Timed>> {
    let mut r = BufReader::with_capacity(64 << 10, File::open(path)?);
    let mut dec = crate::messages();
    let mut told = Told::new();
    let (mut out, mut line, mut last) = (Vec::new(), Vec::new(), 0);
    loop {
        line.clear();
        if r.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        let (at, took) = take_at(dec.as_mut(), &line, before, &mut told, side);
        last = last.max(at);
        match took {
            Took::Events(evs) => out.extend(evs.into_iter().map(|e| (last, e))),
            Took::Skip => {}
            Took::Past => break,
        }
    }
    Ok(out)
}

/// SubagentHistoryReader::subagent_history: the messages of the runs
/// beside s's transcript (subagents/agent-<id>.jsonl, each named by its
/// meta's toolUseId) written before `before`, by when, each Parent the
/// call that started its run. A workflow's agents, a folder down, are
/// left out: no call of the session's started them.
pub(crate) fn subagents(s: &Session, before: Ms) -> adapter::Result<Vec<Timed>> {
    if s.transcript.is_empty() {
        return Err(Box::new(NoTranscript));
    }
    let dir = crate::transcript::subagents_dir(Path::new(&s.transcript));
    let mut out = Vec::new();
    for meta in crate::subruns::meta_files(&dir) {
        let Some(call) = crate::subruns::launch_call(&meta) else {
            continue;
        };
        let path = dir.join(format!("agent-{}.jsonl", crate::subruns::meta_id(&meta)));
        let Ok(evs) = read_timed(&path, before, true) else {
            continue;
        };
        out.extend(evs.into_iter().filter_map(|(at, e)| match e {
            Event::Message(mut m) => {
                m.parent = call.clone();
                Some((at, Event::Message(m)))
            }
            _ => None,
        }));
    }
    // Stable: a run's own lines keep their order.
    out.sort_by_key(|(at, _)| *at);
    Ok(out)
}

/// A line's word that background tasks ended: Claude Code queues it as it
/// comes, and only sometimes writes it as a user message too (the fork's
/// history.go:192, notice).
fn notice(l: &Line) -> Option<String> {
    let text = match &*l.typ {
        "queue-operation" if &*l.operation == "enqueue" => {
            serde_json::from_str::<String>(l.content?.get()).ok()?
        }
        "user" => {
            #[derive(Deserialize)]
            struct M {
                content: String,
            }
            serde_json::from_str::<M>(l.message?.get()).ok()?.content
        }
        _ => return None,
    };
    let text = text.trim();
    text.starts_with("<task-notification>")
        .then(|| text.to_string())
}

/// The tasks a notice says ended: the history keeps no task events, so
/// without them a background agent started before a restart runs forever
/// (the fork's history.go:215, ended).
fn ended(s: &str) -> Vec<rush_core::event::TaskDone> {
    s.split("<task-notification>")
        .skip(1)
        .filter_map(|seg| {
            let seg = seg.split("</task-notification>").next().unwrap_or_default();
            let status = match tag(seg, "status").as_str() {
                st @ ("completed" | "failed" | "stopped") => st.to_string(),
                "killed" => "stopped".to_string(),
                _ => return None,
            };
            Some(rush_core::event::TaskDone {
                id: tag(seg, "task-id"),
                call_id: tag(seg, "tool-use-id"),
                status,
                output_file: tag(seg, "output-file"),
                summary: tag(seg, "summary"),
            })
        })
        .collect()
}

/// What s's first <name> holds, trimmed; "" without one.
fn tag(s: &str, name: &str) -> String {
    let Some((_, rest)) = s.split_once(&format!("<{name}>")) else {
        return String::new();
    };
    let v = rest.split(&format!("</{name}>")).next().unwrap_or_default();
    v.trim().to_string()
}

/// A session with no transcript to read.
#[derive(Debug)]
struct NoTranscript;

impl std::fmt::Display for NoTranscript {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("claude: this session has no transcript")
    }
}

impl std::error::Error for NoTranscript {}

/// HistoryReader::history: s's messages written before `before`, handed
/// to each a slice at a time, each slice from at most `most` bytes.
pub(crate) fn read(
    s: &Session,
    before: Ms,
    most: usize,
    each: &mut dyn FnMut(Vec<Event>) -> bool,
) -> adapter::Result<()> {
    if s.transcript.is_empty() {
        return Err(Box::new(NoTranscript));
    }
    let mut r = BufReader::with_capacity(64 << 10, File::open(&s.transcript)?);
    let mut dec = crate::messages();
    let mut told = Told::new();
    let (mut out, mut taken, mut line) = (Vec::new(), 0usize, Vec::new());
    loop {
        line.clear();
        let n = r.read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        taken += n;
        match take(dec.as_mut(), &line, before, &mut told) {
            Took::Events(evs) => out.extend(evs),
            Took::Skip => {}
            Took::Past => break,
        }
        if taken >= most.max(1) {
            taken = 0;
            if !out.is_empty() && !each(std::mem::take(&mut out)) {
                return Ok(());
            }
        }
    }
    if !out.is_empty() {
        each(out);
    }
    Ok(())
}

/// TailReader::history_tail: s's messages from the first whole line of its
/// transcript's last most bytes, found from the end backwards
/// (rush_core::tailread); the bool is whether that left some out.
pub(crate) fn tail(s: &Session, most: i64) -> adapter::Result<(Vec<Event>, bool)> {
    if s.transcript.is_empty() {
        return Err(Box::new(NoTranscript));
    }
    let (mut f, span) =
        rush_core::tailread::file_tail(std::path::Path::new(&s.transcript), most.max(1) as u64)?;
    if !span.cut() {
        let mut out = Vec::new();
        read(s, 0, adapter::SLICE, &mut |evs| {
            out.extend(evs);
            true
        })?;
        return Ok((out, false));
    }
    f.seek(SeekFrom::Start(span.start))?;
    let mut r = BufReader::with_capacity(64 << 10, f.take(span.len()));
    let mut dec = crate::messages();
    let mut told = Told::new();
    let (mut out, mut line) = (Vec::new(), Vec::new());
    loop {
        line.clear();
        if r.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        if let Took::Events(evs) = take(dec.as_mut(), &line, 0, &mut told) {
            out.extend(evs);
        }
    }
    Ok((out, true))
}

/// HistoryFollower::follow_history: s's transcript from its start, then
/// what's added as it grows.
pub(crate) fn follow(s: &Session) -> adapter::Result<Box<dyn Follow>> {
    if s.transcript.is_empty() {
        return Err(Box::new(NoTranscript));
    }
    Ok(Box::new(Follower {
        path: PathBuf::from(&s.transcript),
        off: 0,
        seen: None,
        dec: crate::messages(),
        told: Told::new(),
    }))
}

/// A transcript followed: how far it's been read, and the decoder that
/// knows the calls still waiting for their results.
struct Follower {
    path: PathBuf,
    off: u64,
    /// The file read so far (adapter::file_id), to tell one renamed over it.
    seen: Option<(u64, u64)>,
    dec: Box<dyn MessageDecoder>,
    told: Told,
}

impl Follow for Follower {
    fn next(&mut self, most: usize) -> adapter::Result<Slice> {
        let mut f = File::open(&self.path)?;
        let meta = f.metadata()?;
        let size = meta.len();
        let mut out = Slice::default();
        if adapter::rewritten(&mut self.seen, self.off, &meta) {
            // Rewritten, or another file renamed over it: start over.
            self.off = 0;
            self.dec = crate::messages();
            self.told.clear();
            out.reset = true;
        }
        if size == self.off {
            return Ok(out);
        }
        f.seek(SeekFrom::Start(self.off))?;
        let want = (size - self.off).min(most.max(1) as u64);
        let mut buf = Vec::with_capacity(want as usize);
        (&mut f).take(want).read_to_end(&mut buf)?;
        let full = match memchr::memrchr(b'\n', &buf) {
            Some(i) => i + 1,
            // One line longer than most: it's a slice of its own, once
            // it's whole.
            None if (buf.len() as u64) < size - self.off => {
                let mut r = BufReader::new(f);
                r.read_until(b'\n', &mut buf)?;
                if buf.last() != Some(&b'\n') {
                    return Ok(out); // still being written
                }
                buf.len()
            }
            None => return Ok(out), // a line still being written waits
        };
        for l in buf[..full].split_inclusive(|&c| c == b'\n') {
            if let Took::Events(evs) = take(self.dec.as_mut(), l, 0, &mut self.told) {
                out.events.extend(evs);
            }
        }
        self.off += full as u64;
        out.more = self.off < size && want as usize == most.max(1);
        Ok(out)
    }

    fn wait_on(&self) -> WaitOn {
        WaitOn {
            paths: vec![self.path.clone()],
            ..WaitOn::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transcript::tests::TempDir;
    use rush_core::adapter::{HistoryFollower, HistoryReader, read_history};
    use rush_core::event::{Event, PartKind};

    fn user(text: &str, at: &str) -> String {
        format!(
            r#"{{"type":"user","timestamp":"{at}","message":{{"role":"user","content":[{{"type":"text","text":"{text}"}}]}}}}"#
        ) + "\n"
    }

    fn texts(evs: &[Event]) -> Vec<String> {
        evs.iter()
            .filter_map(|e| match e {
                Event::Message(m) => m
                    .parts
                    .iter()
                    .find(|p| p.kind == PartKind::Text)
                    .map(|p| p.text.clone()),
                _ => None,
            })
            .collect()
    }

    fn read_lines(name: &str, lines: &[&str]) -> Vec<Event> {
        let d = TempDir::new(name);
        let p = d.0.join("t.jsonl");
        std::fs::write(&p, lines.join("\n") + "\n").unwrap();
        let s = Session {
            transcript: p.to_string_lossy().into_owned(),
            ..Session::default()
        };
        read_history(&crate::Claude::new(), &s, 0).unwrap()
    }

    /// A prompt sent while the agent was busy has no user line: Claude
    /// Code writes it as a queued_command attachment, a prompt's text and
    /// image blocks or its string. A queued task notice is not a prompt.
    #[test]
    fn a_prompt_sent_while_busy_is_read_where_it_was_taken() {
        let evs = read_lines(
            "queued",
            &[
                &user("first", "2026-11-20T10:00:00Z"),
                r#"{"type":"attachment","timestamp":"2026-11-20T10:00:01Z","uuid":"q1","attachment":{"type":"queued_command","prompt":"u fix it?","commandMode":"prompt","origin":{"kind":"human"}}}"#,
                r#"{"type":"attachment","timestamp":"2026-11-20T10:00:02Z","uuid":"q2","attachment":{"type":"queued_command","prompt":[{"type":"text","text":"[thread neat-mole]\nand this"}],"commandMode":"prompt"}}"#,
                r#"{"type":"attachment","timestamp":"2026-11-20T10:00:03Z","uuid":"q3","attachment":{"type":"queued_command","prompt":"<task-notification><task-id>a1</task-id><status>completed</status></task-notification>","commandMode":"task-notification"}}"#,
                r#"{"type":"attachment","timestamp":"2026-11-20T10:00:04Z","uuid":"q4","attachment":{"type":"hook_success","content":"ok"}}"#,
            ],
        );
        assert_eq!(
            texts(&evs),
            ["first", "u fix it?", "[thread neat-mole]\nand this"]
        );
        for e in &evs {
            let Event::Message(m) = e else {
                panic!("not a message: {e:?}")
            };
            assert_eq!(m.role, "user");
        }
    }

    /// Claude Code may write a prompt's line twice, the same uuid both
    /// times, far apart: it is read once.
    #[test]
    fn a_line_written_twice_is_read_once() {
        let twice = r#"{"type":"user","timestamp":"2026-11-20T10:00:00Z","uuid":"e1bb0e0f","message":{"role":"user","content":"did it stay out?"}}"#;
        let evs = read_lines(
            "twice",
            &[
                twice,
                r#"{"type":"assistant","timestamp":"2026-11-20T10:00:01Z","uuid":"a1","message":{"id":"msg_1","role":"assistant","content":[{"type":"text","text":"yes"}]}}"#,
                twice,
                r#"{"type":"user","timestamp":"2026-11-20T10:00:02Z","message":{"role":"user","content":"no uuid"}}"#,
                r#"{"type":"user","timestamp":"2026-11-20T10:00:03Z","message":{"role":"user","content":"no uuid"}}"#,
            ],
        );
        assert_eq!(
            texts(&evs),
            ["did it stay out?", "yes", "no uuid", "no uuid"]
        );
    }

    #[test]
    fn reads_in_slices_up_to_a_time() {
        let d = TempDir::new("history");
        let p = d.0.join("t.jsonl");
        let mut body = String::new();
        for i in 0..6 {
            body += &user(&format!("m{i}"), &format!("2026-09-29T10:00:0{i}Z"));
        }
        // Claude Code's own notes and a subagent's lines are left out.
        body += r#"{"type":"user","isMeta":true,"timestamp":"2026-09-29T10:00:07Z","message":{"role":"user","content":"meta"}}"#;
        body += "\n";
        std::fs::write(&p, &body).unwrap();
        let s = Session {
            transcript: p.to_string_lossy().into_owned(),
            ..Session::default()
        };
        let c = crate::Claude::new();
        let all = read_history(&c, &s, 0).unwrap();
        assert_eq!(texts(&all), ["m0", "m1", "m2", "m3", "m4", "m5"]);
        // Two lines' worth a slice, stopped after the second.
        let one = user("m0", "2026-09-29T10:00:00Z").len();
        let mut slices = Vec::new();
        c.history(&s, 0, 2 * one, &mut |evs| {
            slices.push(texts(&evs));
            slices.len() < 2
        })
        .unwrap();
        assert_eq!(slices, [vec!["m0", "m1"], vec!["m2", "m3"]]);
        // Only those written before 10:00:03.
        let before = rush_core::time::parse_rfc3339("2026-09-29T10:00:03Z");
        let mut got = Vec::new();
        c.history(&s, before, 1 << 20, &mut |evs| {
            got.extend(texts(&evs));
            true
        })
        .unwrap();
        assert_eq!(got, ["m0", "m1", "m2"]);
    }

    // The fork's history_test.go:17 (TestHistoryEndsBackgroundTasks): a
    // background agent's end, which Claude Code only queues, reads back as
    // its task done, told once though it's queued, taken and written as a
    // message too; that message isn't the person's.
    #[test]
    fn background_tasks_end_in_history() {
        let d = TempDir::new("bgend");
        let p = d.0.join("t.jsonl");
        let notice = r#"<task-notification>\n<task-id>a66</task-id>\n<tool-use-id>toolu_1</tool-use-id>\n<output-file>/o</output-file>\n<status>completed</status>\n<summary>Agent \"look\" finished</summary>\n</task-notification>"#;
        let lines = [
            r#"{"type":"assistant","timestamp":"2026-09-29T10:00:00Z","uuid":"a1","message":{"id":"m1","role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"Agent","input":{"description":"look","prompt":"p","run_in_background":true}}]}}"#.to_string(),
            r#"{"type":"user","timestamp":"2026-09-29T10:00:01Z","uuid":"u1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"Async agent launched"}]},"toolUseResult":{"isAsync":true,"status":"async_launched","agentId":"a66"}}"#.to_string(),
            format!(r#"{{"type":"queue-operation","operation":"enqueue","timestamp":"2026-09-29T10:05:00Z","content":"{notice}"}}"#),
            format!(r#"{{"type":"queue-operation","operation":"remove","timestamp":"2026-09-29T10:05:01Z","content":"{notice}"}}"#),
            format!(r#"{{"type":"user","timestamp":"2026-09-29T10:05:02Z","uuid":"u2","message":{{"role":"user","content":"{notice}"}}}}"#),
        ];
        std::fs::write(&p, lines.join("\n") + "\n").unwrap();
        let s = Session {
            transcript: p.to_string_lossy().into_owned(),
            ..Session::default()
        };
        let want = rush_core::event::TaskDone {
            id: "a66".into(),
            call_id: "toolu_1".into(),
            status: "completed".into(),
            output_file: "/o".into(),
            summary: "Agent \"look\" finished".into(),
        };
        let c = crate::Claude::new();
        let (tailed, _) = tail(&s, 1 << 20).unwrap();
        for (how, evs) in [("read", read_history(&c, &s, 0).unwrap()), ("tail", tailed)] {
            let done: Vec<_> = evs
                .iter()
                .filter_map(|e| match e {
                    Event::TaskDone(d) => Some(d.clone()),
                    _ => None,
                })
                .collect();
            assert_eq!(done, std::slice::from_ref(&want), "{how}");
            assert!(
                !texts(&evs)
                    .iter()
                    .any(|t| t.contains("<task-notification>")),
                "{how}: the notice read as the person's message"
            );
        }
    }

    // The statuses a notice ends a task with: killed is stopped; one still
    // running isn't an end.
    #[test]
    fn notices_end_tasks() {
        let seg = |status: &str| {
            format!(
                "<task-notification><task-id>t</task-id><status>{status}</status></task-notification>"
            )
        };
        for (status, want) in [
            ("completed", Some("completed")),
            ("failed", Some("failed")),
            ("stopped", Some("stopped")),
            ("killed", Some("stopped")),
            ("running", None),
        ] {
            let got = ended(&seg(status));
            assert_eq!(got.first().map(|d| d.status.as_str()), want, "{status}");
        }
        let two = format!("{}{}", seg("completed"), seg("failed"));
        assert_eq!(ended(&two).len(), 2);
    }

    #[test]
    fn follows_as_it_grows() {
        let d = TempDir::new("follow");
        let p = d.0.join("t.jsonl");
        let a = user("a", "2026-09-29T10:00:00Z");
        std::fs::write(&p, a.repeat(3)).unwrap();
        let s = Session {
            transcript: p.to_string_lossy().into_owned(),
            ..Session::default()
        };
        let mut f = crate::Claude::new().follow_history(&s).unwrap();
        assert_eq!(f.wait_on().paths, std::slice::from_ref(&p));
        // A slice of two lines, then the rest.
        let sl = f.next(2 * a.len()).unwrap();
        assert_eq!(
            (texts(&sl.events).len(), sl.more, sl.reset),
            (2, true, false)
        );
        let sl = f.next(2 * a.len()).unwrap();
        assert_eq!((texts(&sl.events).len(), sl.more), (1, false));
        // A line still being written waits for its end.
        {
            use std::io::Write;
            let mut w = std::fs::OpenOptions::new().append(true).open(&p).unwrap();
            let b = user("b", "2026-09-29T10:00:01Z");
            w.write_all(&b.as_bytes()[..10]).unwrap();
            assert!(f.next(1 << 20).unwrap().events.is_empty());
            w.write_all(&b.as_bytes()[10..]).unwrap();
        }
        assert_eq!(texts(&f.next(1 << 20).unwrap().events), ["b"]);
        // Rewritten shorter: it starts over.
        std::fs::write(&p, &a).unwrap();
        let sl = f.next(1 << 20).unwrap();
        assert!(sl.reset);
        assert_eq!(texts(&sl.events), ["a"]);
        // Another file renamed over it, longer than what was read: it
        // starts over too.
        let new = d.0.join("t.new");
        std::fs::write(&new, a.repeat(2)).unwrap();
        std::fs::rename(&new, &p).unwrap();
        let sl = f.next(1 << 20).unwrap();
        assert!(sl.reset, "a file renamed over it wasn't a rewrite");
        assert_eq!(texts(&sl.events), ["a", "a"]);
    }

    /// The shared check every adapter's end reader is held to
    /// (adapter::conform).
    #[test]
    fn its_end_is_the_end_of_its_whole() {
        let d = TempDir::new("claude-end");
        let path = d.0.join("t.jsonl");
        let mut b = String::new();
        for i in 0..40 {
            b += &format!(
                r#"{{"type":"user","timestamp":"2026-09-29T10:{:02}:00Z","message":{{"role":"user","content":"ask {i} {}"}}}}"#,
                i % 60,
                "words ".repeat(40)
            );
            b.push('\n');
            b += &format!(
                r#"{{"type":"assistant","timestamp":"2026-09-29T10:{:02}:30Z","message":{{"id":"m{i}","role":"assistant","content":[{{"type":"text","text":"answer {i}"}}]}}}}"#,
                i % 60
            );
            b.push('\n');
        }
        std::fs::write(&path, &b).unwrap();
        let s = Session {
            transcript: path.to_string_lossy().into_owned(),
            ..Session::default()
        };
        rush_core::adapter::conform::check_tail(&crate::Claude::new(), &s, b.len() as i64 / 4);
    }

    /// A session (scrubbed from a real one) whose background agent, outer,
    /// started one of its own, inner: each transcript beside its parent's.
    const NESTED: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/testdata/nested/83285775-c7df-411b-a3d9-f931d0a3adff.jsonl"
    );
    const OUTER: &str = "toolu_01Y53coUkU79eVYmqFCQby5g";
    const INNER: &str = "toolu_0193Yh1rE4bhRGPd45DYp68h";

    fn calls(e: &Event) -> Vec<(String, String)> {
        let Event::Message(m) = e else {
            return Vec::new();
        };
        m.parts
            .iter()
            .filter_map(|p| p.call.as_ref())
            .map(|c| (c.name.clone(), c.id.clone()))
            .collect()
    }

    /// Each subagent's messages, oldest first, name the call that started
    /// its run: outer's own Agent call (inner's start) is outer's, inner's
    /// Bash calls inner's.
    #[test]
    fn subagent_messages_name_the_call_that_started_their_run() {
        let s = Session {
            transcript: NESTED.into(),
            ..Session::default()
        };
        let subs = subagents(&s, 0).unwrap();
        assert!(subs.windows(2).all(|w| w[0].0 <= w[1].0), "by time");
        let parent = |e: &Event| match e {
            Event::Message(m) => m.parent.clone(),
            _ => panic!("only messages: {e:?}"),
        };
        let of = |call: &str| subs.iter().filter(|(_, e)| parent(e) == call).count();
        assert_eq!((of(OUTER), of(INNER), subs.len()), (4, 7, 11));
        let launch = subs
            .iter()
            .find(|(_, e)| calls(e).iter().any(|(_, id)| id == INNER))
            .expect("outer's Agent call");
        assert_eq!(parent(&launch.1), OUTER);
        assert_eq!(calls(&launch.1), [("Agent".to_string(), INNER.to_string())]);
        assert!(
            subs.iter()
                .filter(|(_, e)| parent(e) == INNER)
                .any(|(_, e)| calls(e).iter().any(|(n, _)| n == "Bash"))
        );
        // Before inner began, only outer had said anything.
        let began = subs.iter().find(|(_, e)| parent(e) == INNER).unwrap().0;
        let early = subagents(&s, began).unwrap();
        assert!(!early.is_empty() && early.iter().all(|(_, e)| parent(e) == OUTER));
        // A session with no folder beside it has none.
        let d = TempDir::new("claude-nosubs");
        let p = d.0.join("t.jsonl");
        std::fs::write(&p, user("hi", "2026-09-29T10:00:00Z")).unwrap();
        let alone = Session {
            transcript: p.to_string_lossy().into_owned(),
            ..Session::default()
        };
        assert!(subagents(&alone, 0).unwrap().is_empty());
    }

    /// The timed read is the history read, each event with a time that
    /// never goes back, and it stops at the same time.
    #[test]
    fn timed_history_is_the_history_with_times() {
        let s = Session {
            transcript: NESTED.into(),
            ..Session::default()
        };
        for before in [0, rush_core::time::parse_rfc3339("2026-11-20T14:18:00Z")] {
            let plain = read_history(&crate::Claude::new(), &s, before).unwrap();
            let timed = read_timed(Path::new(NESTED), before, false).unwrap();
            assert!(!plain.is_empty());
            assert_eq!(
                timed.iter().map(|(_, e)| e.clone()).collect::<Vec<_>>(),
                plain
            );
            assert!(timed.windows(2).all(|w| w[0].0 <= w[1].0));
            assert!(
                timed
                    .iter()
                    .all(|(t, _)| *t > 0 && (before == 0 || *t < before))
            );
        }
    }
}

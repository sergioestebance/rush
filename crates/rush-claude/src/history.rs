//! A Claude Code transcript read back as rush's own events, a slice at a
//! time: its messages, from before a time when one is given. A subagent's
//! lines and Claude Code's own notes are left out, as the conversation
//! leaves them out. Port of internal/adapters/claude/history.go:31-71
//! (History), sliced; following it as it grows is this port's own
//! (Go's Claude adapter has no FollowHistory: its pane tails the
//! transcript itself).

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;

use rush_core::adapter::{self, Follow, MessageDecoder, Session, Slice, WaitOn};
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
    let b = b.strip_suffix(b"\n").unwrap_or(b);
    if b.is_empty() {
        return Took::Skip;
    }
    let Ok(l) = serde_json::from_slice::<Line>(b) else {
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
    if let Some(text) = notice(&l) {
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
    if &*l.typ != "user" && &*l.typ != "assistant" {
        return Took::Skip;
    }
    let Some(m) = l.message else {
        return Took::Skip;
    };
    Took::Events(dec.message(&l.typ, m, l.tool_use_result))
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
}

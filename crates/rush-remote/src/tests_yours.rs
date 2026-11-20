//! A chat read back from its transcript says which messages are yours
//! (yours, Baker's ADR 0048): a reopened chat, its older pages and a full
//! replay show what you typed, and none of Claude Code's or rush's notes
//! as yours.

use rush_claude::Claude;
use rush_core::adapter::Session as AgentSession;
use rush_core::event::{Event, Message, Part, PartKind};
use rush_state::Time;
use serde_json::Value;

use crate::resume::{Place, TAIL_BYTES, TAIL_FRAMES, TAIL_TURNS, is_user_turn, session_tail};
use crate::yours::mark;

fn text(t: &str) -> Part {
    Part {
        kind: PartKind::Text,
        text: t.into(),
        ..Part::default()
    }
}

fn user(parent: &str, injected: bool, parts: Vec<Part>) -> Event {
    Event::Message(Message {
        role: "user".into(),
        parent: parent.into(),
        injected,
        parts,
        ..Message::default()
    })
}

fn msg(e: &Event) -> &Message {
    match e {
        Event::Message(m) => m,
        e => panic!("not a message: {e:?}"),
    }
}

/// Each case: the message read back, then whether it is yours and the
/// texts it goes out with.
#[test]
fn a_message_read_back_is_yours_when_you_typed_it() {
    let image = Part {
        kind: PartKind::Image,
        ..Part::default()
    };
    let result = Part {
        kind: PartKind::ToolResult,
        ..Part::default()
    };
    let note =
        "<task-notification><task-id>a1</task-id><status>completed</status></task-notification>";
    let cases: Vec<(&str, Event, bool, Vec<&str>)> = vec![
        (
            "a prompt with rush's note",
            user(
                "",
                true,
                vec![text(
                    "wha's tatus?\n\nrush: stale views: prs. Set each again with what's current, or clear it.",
                )],
            ),
            false,
            vec!["wha's tatus?"],
        ),
        (
            "a slash command",
            user(
                "",
                true,
                vec![text(
                    "<command-name>/compact</command-name>\n<command-args></command-args>",
                )],
            ),
            false,
            vec!["/compact"],
        ),
        (
            "words around a picture",
            user(
                "",
                true,
                vec![text("[Image #1]"), image.clone(), text(" WHY NO REPLY")],
            ),
            false,
            vec!["[Image #1] WHY NO REPLY"],
        ),
        (
            "a reminder beside a prompt",
            user(
                "",
                true,
                vec![text("<system-reminder>x</system-reminder>"), text("do it")],
            ),
            false,
            vec!["do it"],
        ),
        (
            "a task notice",
            user("", true, vec![text(note)]),
            true,
            vec![note],
        ),
        (
            "a compaction summary",
            user(
                "",
                true,
                vec![text(
                    "This session is being continued from a previous conversation.",
                )],
            ),
            true,
            vec!["This session is being continued from a previous conversation."],
        ),
        (
            "a stop",
            user("", true, vec![text("[Request interrupted by user]")]),
            true,
            vec!["[Request interrupted by user]"],
        ),
        (
            "a tool result's note",
            user("", true, vec![result, text("note")]),
            true,
            vec!["note"],
        ),
        (
            "a subagent's prompt",
            user("toolu_1", true, vec![text("do the thing")]),
            true,
            vec!["do the thing"],
        ),
    ];
    for (name, ev, injected, want) in cases {
        let mut evs = vec![ev];
        mark(&mut evs);
        let m = msg(&evs[0]);
        assert_eq!(m.injected, injected, "{name}");
        let got: Vec<&str> = m
            .parts
            .iter()
            .filter(|p| p.kind == PartKind::Text)
            .map(|p| p.text.as_str())
            .collect();
        assert_eq!(got, want, "{name}");
        if name == "words around a picture" {
            assert!(
                m.parts.iter().any(|p| p.kind == PartKind::Image),
                "{name}: the picture stays"
            );
        }
        // A turn the runtime counts is a message a client shows.
        assert_eq!(is_user_turn(&evs[0]), !injected, "{name}: turn");
    }
}

/// A reopened chat's tail, read from a transcript shaped as a real one:
/// your prompts come back yours, in place; Claude Code's notes do not.
#[test]
fn a_reopened_chat_shows_what_you_typed() {
    let dir = std::env::temp_dir().join(format!("rush-yours-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("s.jsonl");
    let lines = [
        r#"{"type":"user","timestamp":"2026-11-20T10:00:00Z","uuid":"u1","message":{"role":"user","content":"check the PRs"}}"#,
        r#"{"type":"assistant","timestamp":"2026-11-20T10:00:01Z","uuid":"a1","message":{"id":"msg_1","role":"assistant","content":[{"type":"text","text":"On it."}]}}"#,
        r#"{"type":"attachment","timestamp":"2026-11-20T10:00:02Z","uuid":"q1","attachment":{"type":"queued_command","prompt":"u fix it?","commandMode":"prompt"}}"#,
        r#"{"type":"user","timestamp":"2026-11-20T10:00:03Z","uuid":"u2","message":{"role":"user","content":"<task-notification><task-id>a1</task-id><status>completed</status></task-notification>"}}"#,
        r#"{"type":"assistant","timestamp":"2026-11-20T10:00:04Z","uuid":"a2","message":{"id":"msg_2","role":"assistant","content":[{"type":"text","text":"Done."}]}}"#,
        r#"{"type":"user","timestamp":"2026-11-20T10:00:05Z","uuid":"u3","message":{"role":"user","content":"wha's tatus?\n\nrush: stale views: prs. Set each again with what's current, or clear it."}}"#,
        r#"{"type":"assistant","timestamp":"2026-11-20T10:00:06Z","uuid":"a3","message":{"id":"msg_3","role":"assistant","content":[{"type":"text","text":"Green."}]}}"#,
        r#"{"type":"user","timestamp":"2026-11-20T10:00:05Z","uuid":"u3","message":{"role":"user","content":"wha's tatus?\n\nrush: stale views: prs. Set each again with what's current, or clear it."}}"#,
    ];
    std::fs::write(&p, lines.join("\n") + "\n").unwrap();
    let s = AgentSession {
        transcript: p.to_string_lossy().into_owned(),
        ..AgentSession::default()
    };
    let (frames, _, _) = session_tail(
        &Claude::new(),
        &s,
        Time::now(),
        Place::default(),
        TAIL_TURNS,
        TAIL_FRAMES,
        TAIL_BYTES,
    );
    let shown: Vec<String> = frames
        .iter()
        .map(|b| serde_json::from_slice::<Value>(b).unwrap())
        .filter(|v| v["t"] == "message")
        .filter(|v| v["e"]["Role"] == "assistant" || v["e"]["Injected"] != true)
        .map(|v| {
            let who = if v["e"]["Role"] == "user" {
                "you"
            } else {
                "agent"
            };
            format!(
                "{who}: {}",
                v["e"]["Parts"][0]["Text"].as_str().unwrap_or("")
            )
        })
        .collect();
    assert_eq!(
        shown,
        [
            "you: check the PRs",
            "agent: On it.",
            "you: u fix it?",
            "agent: Done.",
            "you: wha's tatus?",
            "agent: Green.",
        ]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

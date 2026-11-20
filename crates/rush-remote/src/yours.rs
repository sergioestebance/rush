//! A chat's history read back from its transcript, with what you said
//! marked yours (Baker's ADR 0048). Claude's reader marks every user text
//! injected: live, the host tells what was sent with its sent line. Read
//! back there is none, so this pass says which messages a client shows
//! as sent.

use rush_convo::transcript::{join_texts, said};
use rush_core::event::{Event, Message, Part, PartKind};

/// Marks, in evs read from a transcript, each message that is yours: not
/// injected, its text parts one, cleaned as a prompt shows.
pub(crate) fn mark(evs: &mut [Event]) {
    for ev in evs {
        if let Event::Message(m) = ev
            && let Some(text) = yours(m)
        {
            let mut parts = vec![Part {
                kind: PartKind::Text,
                text,
                ..Part::default()
            }];
            parts.extend(m.parts.drain(..).filter(|p| p.kind != PartKind::Text));
            m.parts = parts;
            m.injected = false;
        }
    }
}

/// What you said in m, an injected message of the session's own read back
/// from its transcript; None when it isn't yours (a subagent's, a tool's
/// result, Claude Code's or rush's notes) or isn't marked injected.
pub(crate) fn yours(m: &Message) -> Option<String> {
    if m.role != "user" || !m.injected || !m.parent.is_empty() {
        return None;
    }
    if m.parts.iter().any(|p| p.kind == PartKind::ToolResult) {
        return None;
    }
    // Each text part yours or not (a notice may share a message with what
    // you said), then the ones that are as one prompt: words split around
    // a picture join as they were.
    let texts: Vec<&str> = m
        .parts
        .iter()
        .filter(|p| p.kind == PartKind::Text && said(&p.text).is_some())
        .map(|p| p.text.as_str())
        .collect();
    if texts.is_empty() {
        return None;
    }
    said(&join_texts(&texts))
}

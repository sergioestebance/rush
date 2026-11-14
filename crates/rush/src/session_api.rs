//! `rush session api --protocol 1`: the versioned embedding interface's
//! envelope, answering only `probe` with the build it is. An embedder
//! checks that build (protocol, version, revision) before it runs a
//! runtime; every other method is refused as invalid input, so nothing
//! claims a capability this rush lacks.
//!
//! Ported from the fork's cmd/rush/session_api.go (sessionAPI :15) and
//! internal/host/session_api_transport.go (APIInputBytes :20, APIRequest
//! :23, APIResponse :30, APIError :38, WriteAPIResponse :66, apiBuild :82,
//! apiProbe :97, apiValidRequest :107), session_api_contract.go:651
//! (runtimeUUID).

use std::io::{Read, Write};

use serde_json::{Value, json};

/// The protocol's version as the build reports it.
pub const API_VERSION: &str = "session-api-v1";

/// The most input a request may be (APIInputBytes).
const INPUT_MOST: u64 = 8 << 20;

/// The full commit this rush was built from: RUSH_REVISION, which build.rs
/// sets from git unless the build sets it, else "unknown".
pub const REVISION: &str = match option_env!("RUSH_REVISION") {
    Some(r) => r,
    None => "unknown",
};

/// `rush session api <args>`: one request on stdin, one answer line on
/// stdout. Its exit code is 0 whenever an answer was written, as Go's.
pub fn session_api(args: &[String], stdin: &mut dyn Read, stdout: &mut dyn Write) -> i32 {
    let mut data = Vec::new();
    let read = stdin.take(INPUT_MOST + 1).read_to_end(&mut data);
    let input = match read {
        Ok(_) if data.len() as u64 <= INPUT_MOST => Some(data.as_slice()),
        _ => None,
    };
    let answer = answer(args, input, REVISION);
    match writeln!(stdout, "{answer}") {
        Ok(()) => 0,
        Err(_) => 1,
    }
}

/// The answer to one request: input is None when it couldn't be read whole
/// within its bound.
fn answer(args: &[String], input: Option<&[u8]>, revision: &str) -> Value {
    let req: Option<Value> = input.and_then(|b| serde_json::from_slice(b).ok());
    let id = req
        .as_ref()
        .and_then(|r| r.get("requestId"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let args_ok = matches!(args, [f, v] if f == "--protocol" && v == "1")
        || matches!(args, [f] if f == "--protocol=1");
    let Some(req) = req.filter(|r| args_ok && valid(r)) else {
        return failure(&id);
    };
    match req.get("method").and_then(Value::as_str) {
        Some("probe") => json!({
            "protocol": 1,
            "requestId": id,
            "ok": true,
            "result": {
                "protocol": 1,
                "build": {"protocol": 1, "version": API_VERSION, "revision": revision},
                "features": [],
                "providers": [],
            },
        }),
        _ => failure(&id),
    }
}

/// Whether the envelope is whole: protocol 1, a uuid request id, and
/// params that are JSON (apiValidRequest).
fn valid(r: &Value) -> bool {
    r.get("protocol").and_then(Value::as_i64) == Some(1)
        && r.get("requestId").and_then(Value::as_str).is_some_and(uuid)
        && r.get("params").is_some_and(|p| !p.is_null())
}

/// Whether s is a uuid, and not the nil one (runtimeUUID).
fn uuid(s: &str) -> bool {
    s.len() == 36
        && s != "00000000-0000-0000-0000-000000000000"
        && s.bytes().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == b'-',
            _ => c.is_ascii_hexdigit(),
        })
}

/// The refusal of a request that isn't one this rush answers (APIError
/// "invalid_input", "not_applied").
fn failure(id: &str) -> Value {
    json!({
        "protocol": 1,
        "requestId": id,
        "ok": false,
        "error": {
            "code": "invalid_input",
            "outcome": "not_applied",
            "retry": "none",
            "message": "Runtime request is invalid.",
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "11111111-1111-4111-8111-111111111111";

    fn args(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| s.to_string()).collect()
    }

    // session_api_test.go:10 (TestPublicSessionAPIProbeAndInvalidEnvelope),
    // plus what this rush refuses that Go's served: any other method.
    #[test]
    fn probe_answers_its_build_and_refuses_the_rest() {
        let p1 = args(&["--protocol", "1"]);
        let probe =
            format!(r#"{{"protocol":1,"requestId":"{ID}","method":"probe","params":{{}}}}"#);
        let a = answer(&p1, Some(probe.as_bytes()), "abc123");
        assert_eq!(
            a,
            json!({"protocol":1,"requestId":ID,"ok":true,"result":{"protocol":1,
                "build":{"protocol":1,"version":"session-api-v1","revision":"abc123"},
                "features":[],"providers":[]}})
        );
        let refused: &[(&str, Vec<String>, Option<String>)] = &[
            (
                "protocol 2",
                p1.clone(),
                Some(probe.replace(r#""protocol":1"#, r#""protocol":2"#)),
            ),
            (
                "request id not a uuid",
                p1.clone(),
                Some(probe.replace(ID, "wrong")),
            ),
            (
                "nil uuid",
                p1.clone(),
                Some(probe.replace(ID, "00000000-0000-0000-0000-000000000000")),
            ),
            ("trailing input", p1.clone(), Some(format!("{probe} {{}}"))),
            (
                "no params",
                p1.clone(),
                Some(probe.replace(r#","params":{}"#, "")),
            ),
            ("not json", p1.clone(), Some("probe".into())),
            ("too large", p1.clone(), None),
            ("no --protocol", vec![], Some(probe.clone())),
            (
                "--protocol 2",
                args(&["--protocol", "2"]),
                Some(probe.clone()),
            ),
            (
                "extra argument",
                args(&["--protocol", "1", "x"]),
                Some(probe.clone()),
            ),
            (
                "a method only Go served",
                p1.clone(),
                Some(probe.replace("probe", "initialize")),
            ),
            (
                "an unknown method",
                p1.clone(),
                Some(probe.replace("probe", "nope")),
            ),
        ];
        for (why, a, input) in refused {
            let got = answer(a, input.as_deref().map(str::as_bytes), "abc123");
            assert_eq!(got["ok"], json!(false), "{why}: {got}");
            assert_eq!(got["error"]["code"], json!("invalid_input"), "{why}: {got}");
            assert_eq!(got["protocol"], json!(1), "{why}: {got}");
        }
        // The request id is echoed when it could be read, as Go does.
        let other = answer(&p1, Some(probe.replace("probe", "nope").as_bytes()), "r");
        assert_eq!(other["requestId"], json!(ID));
    }

    /// The command writes one line and ends 0, the build's revision in it.
    #[test]
    fn session_api_writes_one_line() {
        let probe =
            format!(r#"{{"protocol":1,"requestId":"{ID}","method":"probe","params":{{}}}}"#);
        let mut out = Vec::new();
        let code = session_api(&args(&["--protocol", "1"]), &mut probe.as_bytes(), &mut out);
        let s = String::from_utf8(out).unwrap();
        assert_eq!((code, s.matches('\n').count()), (0, 1), "{s}");
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["result"]["build"]["revision"], json!(REVISION));
    }
}

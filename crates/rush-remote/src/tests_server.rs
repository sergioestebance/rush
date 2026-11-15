//! serve's HTTP surface, through its handler and on loopback only: who
//! gets in, what reaches a session's host, the hub's proxy, pairing,
//! devices and the streams. After Go's server_test.go, devices_test.go,
//! stream_test.go and fakehost_test.go.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixListener;
use std::sync::mpsc::{Receiver, channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rush_host::Info;
use rush_state::Time;
use serde_json::Value;

use crate::config::{Config, Peer, update_config};
use crate::ctx::Ctx;
use crate::http::{Body, Request, Response};
use crate::server::{Server, StartRequest};
use crate::testutil::home;

const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn server(peers: Vec<Peer>) -> Arc<Server> {
    update_config(|c| c.web = true).unwrap();
    let mut s = Server::new(
        Config {
            name: "hub".into(),
            peers,
            ..Config::default()
        },
        TOKEN.into(),
        rush_core::adapter::Registry::new(),
    );
    s.bad_guess = Duration::ZERO;
    Arc::new(s)
}

fn req(method: &str, path: &str, body: &str, hdr: &[(&str, &str)]) -> Request {
    let mut r = Request::new(method, path)
        .with_body(body)
        .with_header("Host", "rush.test");
    for (k, v) in hdr {
        r = r.with_header(k, v);
    }
    r
}

fn call(s: &Arc<Server>, method: &str, path: &str, body: &str, hdr: &[(&str, &str)]) -> Response {
    s.handler()(&req(method, path, body, hdr))
}

fn bearer(t: &str) -> String {
    format!("Bearer {t}")
}

fn cookie(t: &str) -> String {
    format!("rush_token={t}")
}

#[test]
fn auth_origin_and_ops() {
    let _h = home();
    let s = server(vec![]);
    let b = bearer(TOKEN);
    let c = cookie(TOKEN);
    let json = ("Content-Type", "application/json");
    let login = format!(r#"{{"token":"{TOKEN}"}}"#);
    type Case<'a> = (
        &'a str,
        &'a str,
        &'a str,
        &'a str,
        Vec<(&'a str, &'a str)>,
        u16,
    );
    let cases: Vec<Case> = vec![
        ("shell is public", "GET", "/", "", vec![], 200),
        ("api needs a token", "GET", "/api/sessions", "", vec![], 401),
        (
            "wrong bearer",
            "GET",
            "/api/sessions",
            "",
            vec![("Authorization", "Bearer nope")],
            401,
        ),
        (
            "bearer",
            "GET",
            "/api/sessions",
            "",
            vec![("Authorization", &b)],
            200,
        ),
        (
            "cookie",
            "GET",
            "/api/sessions",
            "",
            vec![("Cookie", &c), json],
            200,
        ),
        (
            "cookie post, same host",
            "POST",
            "/api/sessions/zz/op",
            r#"{"op":"stop"}"#,
            vec![("Cookie", &c), json, ("Origin", "http://rush.test")],
            404,
        ),
        (
            "cookie post, other origin",
            "POST",
            "/api/sessions/zz/op",
            r#"{"op":"stop"}"#,
            vec![("Cookie", &c), json, ("Origin", "https://evil.example")],
            403,
        ),
        (
            "cookie post, form",
            "POST",
            "/api/sessions/zz/op",
            "op=stop",
            vec![
                ("Cookie", &c),
                ("Content-Type", "application/x-www-form-urlencoded"),
            ],
            403,
        ),
        (
            "cross-site fetch without origin",
            "POST",
            "/api/sessions/zz/op",
            r#"{"op":"stop"}"#,
            vec![("Cookie", &c), json, ("Sec-Fetch-Site", "cross-site")],
            403,
        ),
        (
            "login from another origin",
            "POST",
            "/api/login",
            &login,
            vec![("Origin", "https://evil.example")],
            401,
        ),
        (
            "login",
            "POST",
            "/api/login",
            &login,
            vec![("Origin", "http://rush.test")],
            200,
        ),
        (
            "no such session",
            "GET",
            "/api/sessions/nope/events",
            "",
            vec![("Authorization", &b)],
            404,
        ),
        (
            "path trick in id",
            "GET",
            "/api/sessions/..%2f..%2fetc/events",
            "",
            vec![("Authorization", &b)],
            404,
        ),
        (
            "no such machine",
            "GET",
            "/m/ghost/api/sessions",
            "",
            vec![("Authorization", &b)],
            404,
        ),
        (
            "machine needs token",
            "GET",
            "/m/ghost/api/sessions",
            "",
            vec![],
            401,
        ),
    ];
    for (name, m, p, body, hdr, want) in cases {
        let r = call(&s, m, p, body, &hdr);
        assert_eq!(
            r.status,
            want,
            "{name}: {}",
            String::from_utf8_lossy(r.bytes())
        );
        assert_eq!(r.get("X-Content-Type-Options"), "nosniff", "{name}");
        assert!(
            r.get("Content-Security-Policy")
                .contains("frame-ancestors 'none'")
        );
    }
}

/// Serves h on loopback; its address.
fn on_loopback(h: crate::http::Handler) -> (String, Ctx) {
    let ln = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", ln.local_addr().unwrap());
    let ctx = Ctx::new();
    let c = ctx.clone();
    std::thread::spawn(move || crate::http::serve(&c, ln, h));
    (url, ctx)
}

#[test]
fn hub_proxies_with_the_peers_token() {
    let _h = home();
    let peer_tok = "b".repeat(64);
    let mut peer = Server::new(
        Config::default(),
        peer_tok.clone(),
        rush_core::adapter::Registry::new(),
    );
    peer.bad_guess = Duration::ZERO;
    let ph = Arc::new(peer).handler();
    let seen: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let s2 = seen.clone();
    let (url, stop) = on_loopback(Arc::new(move |r: &Request| {
        s2.lock()
            .unwrap()
            .push((r.header("authorization").into(), r.header("cookie").into()));
        ph(r)
    }));
    let hub = server(vec![Peer {
        name: "box".into(),
        url,
        token: peer_tok.clone(),
    }]);
    let r = call(
        &hub,
        "GET",
        "/m/box/api/sessions",
        "",
        &[("Cookie", &cookie(TOKEN))],
    );
    let mut body = Vec::new();
    if let Body::Stream(f) = r.body {
        f(&mut body);
    }
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(String::from_utf8_lossy(&body), "[]\n");
    let got = seen.lock().unwrap()[0].clone();
    assert_eq!(got, (bearer(&peer_tok), String::new()));
    // Only /api/ goes through, and only to a named peer.
    assert_eq!(
        call(
            &hub,
            "GET",
            "/m/box/",
            "",
            &[("Authorization", &bearer(TOKEN))]
        )
        .status,
        404
    );
    // The peer's token is no use at the hub.
    assert_eq!(
        call(
            &hub,
            "GET",
            "/api/sessions",
            "",
            &[("Authorization", &bearer(&peer_tok))]
        )
        .status,
        401
    );
    stop.cancel();
}

#[test]
fn remote_ops_allowlist() {
    let _h = home();
    let s = server(vec![]);
    std::fs::create_dir_all(rush_host::root().join("zz")).unwrap();
    for op in [
        "rewind",
        "restart",
        "ask",
        "relogin",
        "retitle",
        "compacted",
    ] {
        let r = call(
            &s,
            "POST",
            "/api/sessions/zz/op",
            &format!(r#"{{"op":"{op}"}}"#),
            &[
                ("Authorization", &bearer(TOKEN)),
                ("Content-Type", "application/json"),
            ],
        );
        assert_eq!(r.status, 403, "{op}");
    }
    // mcp is one a remote client may send (the fork's server.go:621): one
    // naming no servers is refused for that, not for its name.
    assert!(crate::server::REMOTE_OPS.contains(&"mcp"));
    let r = call(
        &s,
        "POST",
        "/api/sessions/zz/op",
        r#"{"op":"mcp"}"#,
        &[
            ("Authorization", &bearer(TOKEN)),
            ("Content-Type", "application/json"),
        ],
    );
    assert_eq!(r.status, 400, "mcp");
}

#[test]
fn allowed_cwd() {
    let _h = home();
    let s = server(vec![]);
    let ws = tempfile::tempdir().unwrap();
    let ws_s = ws.path().to_string_lossy().into_owned();
    assert!(
        s.allowed_cwd(&crate::config::dir().join("logins").to_string_lossy())
            .is_err()
    );
    assert!(s.allowed_cwd(&ws_s).is_ok());
    let mut c = s.config.clone();
    c.workspaces = vec![ws_s.clone()];
    let s = Server::new(c, TOKEN.into(), rush_core::adapter::Registry::new());
    let sub = ws.path().join("p");
    std::fs::create_dir_all(&sub).unwrap();
    assert!(s.allowed_cwd(&sub.to_string_lossy()).is_ok());
    assert!(s.allowed_cwd(&format!("{ws_s}/..")).is_err());
    let out = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(out.path(), ws.path().join("link")).unwrap();
    assert!(
        s.allowed_cwd(&ws.path().join("link").to_string_lossy())
            .is_err()
    );
}

/// A session's host stands in: alive (this process), answering on the
/// session's socket with its info, and handing over every op it's sent.
/// An mcp op with an id it answers as mcp says (the fork's
/// fakehost_test.go:105).
struct FakeHost {
    info: Info,
    ops: Receiver<Vec<u8>>,
    mcp: Arc<Mutex<McpAnswer>>,
}

/// How a fake host answers an mcp op.
#[derive(Clone, Default)]
enum McpAnswer {
    /// Takes it.
    #[default]
    Take,
    /// Says why it didn't.
    Refuse(&'static str),
    /// Never answers.
    Silent,
    /// Hangs up.
    Gone,
}

fn fake_host(id: &str, name: &str, cwd: &str) -> FakeHost {
    let dir = rush_host::dir(id);
    std::fs::create_dir_all(&dir).unwrap();
    let info = Info {
        id: id.into(),
        session_id: format!("{id}-0000"),
        name: name.into(),
        cwd: cwd.into(),
        host_pid: std::process::id() as i64,
        state: "idle".into(),
        proto: rush_host::PROTO,
        started_at: Time::now(),
        updated_at: Time::now(),
        ..Info::default()
    };
    std::fs::write(dir.join("info.json"), serde_json::to_vec(&info).unwrap()).unwrap();
    let ln = UnixListener::bind(rush_host::sock_path(id)).unwrap();
    let (tx, rx) = channel();
    let i2 = info.clone();
    let mcp: Arc<Mutex<McpAnswer>> = Arc::default();
    let m2 = mcp.clone();
    std::thread::spawn(move || {
        for c in ln.incoming() {
            let Ok(mut c) = c else { return };
            let mut l = br#"{"info":"#.to_vec();
            l.extend(serde_json::to_vec(&i2).unwrap());
            l.extend_from_slice(b",\"type\":\"agtop_info\"}\n");
            let _ = c.write_all(&l);
            let tx = tx.clone();
            let m2 = m2.clone();
            std::thread::spawn(move || {
                let mut w = c.try_clone().unwrap();
                for line in BufReader::new(c).split(b'\n').map_while(Result::ok) {
                    let v: Value = serde_json::from_slice(&line).unwrap_or(Value::Null);
                    if v["op"] == "hello" {
                        continue;
                    }
                    let _ = tx.send(line);
                    let Some(id) = v["id"].as_str().filter(|_| v["op"] == "mcp") else {
                        continue;
                    };
                    let answer = m2.lock().unwrap().clone();
                    let err = match answer {
                        McpAnswer::Take => "",
                        McpAnswer::Refuse(why) => why,
                        McpAnswer::Silent => continue,
                        McpAnswer::Gone => {
                            let _ = w.shutdown(std::net::Shutdown::Both);
                            return;
                        }
                    };
                    let mut l = rush_host::lines::reply_line(id, None, err);
                    l.push(b'\n');
                    let _ = w.write_all(&l);
                }
            });
        }
    });
    FakeHost { info, ops: rx, mcp }
}

impl FakeHost {
    /// Says nothing reached it: what serve refuses never dials.
    fn no_op(&self) {
        if let Ok(l) = self.ops.try_recv() {
            panic!("the host got {}", String::from_utf8_lossy(&l));
        }
    }

    /// Writes its info with f's changes, as serve reads it.
    fn set_info(&self, f: impl FnOnce(&mut Info)) {
        let mut i = rush_host::read_info(&self.info.id).unwrap();
        f(&mut i);
        std::fs::write(
            rush_host::dir(&self.info.id).join("info.json"),
            serde_json::to_vec(&i).unwrap(),
        )
        .unwrap();
    }

    fn op(&self) -> Value {
        let l = self
            .ops
            .recv_timeout(Duration::from_secs(5))
            .expect("no op reached the host");
        serde_json::from_slice(&l).unwrap()
    }
}

// The fork's server_test.go:278 (TestDeleteTakesASessionForGood): a delete
// stops a running session first, then takes its folder and scratch, so the
// list no longer has it; one already gone is a 404.
#[test]
fn delete_takes_a_session_for_good() {
    let _h = home();
    let f = fake_host("feedbeef", "job", "/work");
    let mut s = Server::new(
        Config {
            name: "box".into(),
            ..Config::default()
        },
        TOKEN.into(),
        rush_core::adapter::Registry::new(),
    );
    s.delete_wait = Duration::from_millis(50);
    let s = Arc::new(s);
    let b = bearer(TOKEN);
    let hdr = [("Authorization", b.as_str())];
    let del = || call(&s, "DELETE", "/api/sessions/feedbeef", "", &hdr);

    let r = del();
    assert_eq!(
        r.status,
        409,
        "a host that won't stop: {}",
        String::from_utf8_lossy(r.bytes())
    );
    assert_eq!(f.op()["op"], "stop", "the host wasn't asked to stop");

    let scratch = rush_host::dir("feedbeef").join("tmp");
    std::fs::create_dir_all(&scratch).unwrap();
    let mut info = f.info.clone();
    info.host_pid = 0; // stopped
    std::fs::write(
        rush_host::dir("feedbeef").join("info.json"),
        serde_json::to_vec(&info).unwrap(),
    )
    .unwrap();
    let r = del();
    assert_eq!(
        r.status,
        204,
        "delete: {}",
        String::from_utf8_lossy(r.bytes())
    );
    assert!(
        !rush_host::dir("feedbeef").exists(),
        "the folder outlived the delete"
    );
    let list = call(&s, "GET", "/api/sessions", "", &hdr);
    assert!(
        !String::from_utf8_lossy(list.bytes()).contains("feedbeef"),
        "a deleted session is listed"
    );
    assert_eq!(del().status, 404, "repeated delete");
    for bad in ["/api/sessions/..", "/api/sessions/a.b"] {
        assert_eq!(call(&s, "DELETE", bad, "", &hdr).status, 404, "{bad}");
    }
    let r = call(&s, "PUT", "/api/sessions/feedbeef", "", &hdr);
    assert_eq!(r.status, 405);
}

/// Every control the web app has, sent as the browser would: what reaches
/// the host is what the app asked for, and nothing carries a path.
#[test]
fn web_controls_reach_the_host() {
    let _h = home();
    let f = fake_host("feedbeef", "job", "/work");
    let started: Arc<Mutex<Option<StartRequest>>> = Arc::default();
    let st = started.clone();
    let info = f.info.clone();
    let mut s = Server::new(
        Config {
            name: "box".into(),
            ..Config::default()
        },
        TOKEN.into(),
        rush_core::adapter::Registry::new(),
    );
    s.start = Some(Arc::new(move |r: &StartRequest| {
        *st.lock().unwrap() = Some(r.clone());
        Ok(info.clone())
    }));
    let s = Arc::new(s);
    let c = cookie(TOKEN);
    let hdr = [
        ("Cookie", c.as_str()),
        ("Content-Type", "application/json"),
        ("Origin", "http://rush.test"),
    ];
    let post = |p: &str, b: &str| call(&s, "POST", p, b, &hdr);
    let r = post(
        "/api/sessions",
        r#"{"cwd":"/work","agent":"claude","prompt":"go","name":"n"}"#,
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(r.bytes()));
    let got = started.lock().unwrap().clone().unwrap();
    assert_eq!(
        (
            got.cwd.as_str(),
            got.agent.as_str(),
            got.prompt.as_str(),
            got.name.as_str()
        ),
        ("/work", "claude", "go", "n")
    );
    let cases: Vec<(&str, &str, Value)> = vec![
        (
            "steer a queued one in",
            r#"{"op":"queue_send","index":1,"was":"b","guide":true}"#,
            serde_json::json!({"op":"queue_send","index":1,"was":"b","guide":true}),
        ),
        (
            "send",
            r#"{"op":"send","text":"hello"}"#,
            serde_json::json!({"op":"send","text":"hello"}),
        ),
        (
            "steer",
            r#"{"op":"send","text":"turn left","guide":true}"#,
            serde_json::json!({"op":"send","text":"turn left","guide":true}),
        ),
        (
            "stop the turn",
            r#"{"op":"interrupt"}"#,
            serde_json::json!({"op":"interrupt"}),
        ),
        (
            "approve",
            r#"{"op":"allow","id":"ap1"}"#,
            serde_json::json!({"op":"allow","id":"ap1"}),
        ),
        (
            "approve always",
            r#"{"op":"allow","id":"ap1","always":true}"#,
            serde_json::json!({"op":"allow","id":"ap1","always":true}),
        ),
        (
            "deny",
            r#"{"op":"deny","id":"ap1","message":"no"}"#,
            serde_json::json!({"op":"deny","id":"ap1","message":"no"}),
        ),
        (
            "stop a task",
            r#"{"op":"stop_task","id":"t1"}"#,
            serde_json::json!({"op":"stop_task","id":"t1"}),
        ),
        (
            // Send now cuts in, on a harness that steers too (the fork's
            // host.go:1855); a client steers it in with guide.
            "queue send",
            r#"{"op":"queue_send","index":1,"was":"next"}"#,
            serde_json::json!({"op":"queue_send","index":1,"was":"next","now":true}),
        ),
        (
            "continue at the reset",
            r#"{"op":"limit","now":true}"#,
            serde_json::json!({"op":"limit","now":true}),
        ),
        (
            "tell a subagent",
            r#"{"op":"tell","id":"sub1","text":"hurry"}"#,
            serde_json::json!({"op":"tell","id":"sub1","text":"hurry"}),
        ),
    ];
    for (name, body, want) in cases {
        let r = post("/api/sessions/feedbeef/op", body);
        assert_eq!(
            r.status,
            202,
            "{name}: {}",
            String::from_utf8_lossy(r.bytes())
        );
        let got = f.op();
        for (k, v) in want.as_object().unwrap() {
            assert_eq!(&got[k], v, "{name}: {got}");
        }
    }
    // The answer's input arrives whole.
    post(
        "/api/sessions/feedbeef/op",
        r#"{"op":"allow","id":"q1","input":{"answers":{"Pick one":"B"}}}"#,
    );
    assert_eq!(f.op()["input"]["answers"]["Pick one"], "B");
    // A browser can't attach files of this machine, or sneak an exchange.
    post(
        "/api/sessions/feedbeef/op",
        r#"{"op":"send","text":"x","images":["/etc/passwd"],"exchange":{"id":"z"}}"#,
    );
    let got = f.op();
    assert!(
        got.get("images").is_none() && got.get("exchange").is_none(),
        "{got}"
    );
    assert_eq!(
        post(
            "/api/sessions/feedbeef/op",
            r#"{"op":"away","away":{"every":1000}}"#
        )
        .status,
        400
    );
    // An image arrives as bytes and reaches the host as a file in the
    // session's scratch folder.
    post(
        "/api/sessions/feedbeef/op",
        r#"{"op":"send","text":"see","pictures":[{"type":"image/png","data":"aGk="}]}"#,
    );
    let got = f.op();
    let p = got["images"][0].as_str().unwrap().to_string();
    assert!(
        p.starts_with(&*rush_host::tmp::temp_dir("feedbeef").to_string_lossy())
            && p.ends_with(".png"),
        "{p}"
    );
    assert_eq!(std::fs::read(&p).unwrap(), b"hi");
    for bad in [
        r#"{"op":"send","text":"x","pictures":[{"type":"text/html","data":"aGk="}]}"#,
        r#"{"op":"send","text":"x","pictures":[{"type":"image/png","data":""}]}"#,
        r#"{"op":"allow","id":"ap1","pictures":[{"type":"image/png","data":"aGk="}]}"#,
        &format!(
            r#"{{"op":"send","pictures":[{}{{"type":"image/png","data":"aGk="}}]}}"#,
            r#"{"type":"image/png","data":"aGk="},"#.repeat(8)
        ),
    ] {
        assert_eq!(post("/api/sessions/feedbeef/op", bad).status, 400, "{bad}");
    }
    assert_eq!(
        post("/api/sessions/feedbeef/op", r#"{"op":"rewind"}"#).status,
        403
    );
    assert!(f.ops.try_recv().is_err(), "an op reached the host");
}

#[test]
fn a_pairing_code_signs_in_once() {
    let _h = home();
    let mut c = Config {
        name: "hub".into(),
        origin: "https://rush.test".into(),
        ..Config::default()
    };
    c.web = true;
    let mut s = Server::new(c, TOKEN.into(), rush_core::adapter::Registry::new());
    s.bad_guess = Duration::ZERO;
    let s = Arc::new(s);
    let json = ("Content-Type", "application/json");
    assert_eq!(call(&s, "POST", "/api/pair", "{}", &[json]).status, 401);
    let r = call(
        &s,
        "POST",
        "/api/pair",
        "{}",
        &[("Authorization", &bearer(TOKEN)), json],
    );
    let p: crate::pair::Pairing = serde_json::from_slice(r.bytes()).unwrap();
    assert_eq!(p.url, format!("https://rush.test/#pair={}", p.code));
    assert!(!String::from_utf8_lossy(r.bytes()).contains(TOKEN));
    let login = |code: &str| {
        call(
            &s,
            "POST",
            "/api/login",
            &format!(r#"{{"code":"{code}"}}"#),
            &[("Origin", "https://rush.test"), json],
        )
    };
    let r = login(&p.code);
    assert_eq!(r.status, 200);
    assert!(r.get("Set-Cookie").starts_with("rush_token="));
    assert!(
        r.get("Set-Cookie")
            .contains("; Path=/; Max-Age=34560000; HttpOnly; SameSite=Strict")
    );
    assert_eq!(login(&p.code).status, 401, "the code worked twice");
}

#[test]
fn the_web_app_follows_its_switch() {
    let _h = home();
    let s = server(vec![]);
    update_config(|c| c.web = false).unwrap();
    let r = call(&s, "GET", "/", "", &[]);
    assert_eq!(r.status, 404);
    assert_eq!(
        String::from_utf8_lossy(r.bytes()),
        "The web app is off on this machine: Settings › Remote in rush turns it on.\n"
    );
    assert_eq!(
        call(
            &s,
            "GET",
            "/api/sessions",
            "",
            &[("Authorization", &bearer(TOKEN))]
        )
        .status,
        200
    );
    update_config(|c| c.web = true).unwrap();
    let r = call(&s, "GET", "/", "", &[]);
    assert_eq!(r.get("Content-Type"), "text/html; charset=utf-8");
    assert_eq!(r.get("Cache-Control"), "no-cache");
    assert!(String::from_utf8_lossy(r.bytes()).contains("<html"));
    assert_eq!(call(&s, "GET", "/nope.js", "", &[]).status, 404);
}

/// Each sign-in is a device of its own; revoking one locks only it out. A
/// cookie from before devices becomes one at the app's first request.
#[test]
fn devices_sign_in_and_revoke() {
    let _h = home();
    let s = server(vec![]);
    let sign_in = |ua: &str| -> String {
        let r = call(
            &s,
            "POST",
            "/api/login",
            &format!(r#"{{"token":"{TOKEN}"}}"#),
            &[
                ("Origin", "http://rush.test"),
                ("Content-Type", "application/json"),
                ("User-Agent", ua),
            ],
        );
        assert_eq!(r.status, 200);
        let set = r.get("Set-Cookie");
        let v = set
            .split(';')
            .next()
            .unwrap()
            .strip_prefix("rush_token=")
            .unwrap()
            .to_string();
        assert_ne!(v, TOKEN);
        v
    };
    let phone = sign_in(
        "Mozilla/5.0 (iPhone; CPU iPhone OS 19_0 like Mac OS X) Version/19.0 Mobile/15E148 Safari/604.1",
    );
    let laptop =
        sign_in("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/140.0 Safari/537.36");
    assert_eq!(
        call(
            &s,
            "GET",
            "/api/sessions",
            "",
            &[("Cookie", &cookie(&phone))]
        )
        .status,
        200
    );
    let b = bearer(TOKEN);
    let r = call(&s, "GET", "/api/devices", "", &[("Authorization", &b)]);
    let body = String::from_utf8_lossy(r.bytes()).into_owned();
    assert!(!body.contains("tokenHash"), "{body}");
    let list: Vec<crate::devices::Device> = serde_json::from_str(&body).unwrap();
    assert_eq!(list.len(), 2);
    let names: Vec<&str> = list.iter().map(|d| d.name.as_str()).collect();
    assert!(
        names.contains(&"iPhone · Safari") && names.contains(&"Mac · Chrome"),
        "{names:?}"
    );
    let id = list
        .iter()
        .find(|d| d.name.starts_with("iPhone"))
        .unwrap()
        .id
        .clone();
    let r = call(
        &s,
        "POST",
        &format!("/api/devices/{id}/revoke"),
        "{}",
        &[("Authorization", &b), ("Content-Type", "application/json")],
    );
    assert_eq!(r.status, 200);
    assert_eq!(
        call(
            &s,
            "GET",
            "/api/sessions",
            "",
            &[("Cookie", &cookie(&phone))]
        )
        .status,
        401
    );
    assert_eq!(
        call(
            &s,
            "GET",
            "/api/sessions",
            "",
            &[("Cookie", &cookie(&laptop))]
        )
        .status,
        200
    );
    let r = call(
        &s,
        "GET",
        "/api/machines",
        "",
        &[("Cookie", &cookie(TOKEN))],
    );
    assert_eq!(r.status, 200);
    assert!(r.get("Set-Cookie").starts_with("rush_token="));
    assert!(!r.get("Set-Cookie").contains(TOKEN));
    assert_eq!(s.devs.all().len(), 2);
    assert_eq!(
        String::from_utf8_lossy(r.bytes()),
        "[{\"name\":\"hub\",\"self\":true}]\n"
    );
}

/// A writer that ends its stream once it holds want events.
struct Until {
    out: Arc<Mutex<Vec<u8>>>,
    want: usize,
    /// A stream that never gets to want frames ends here, so its test
    /// fails rather than hangs.
    by: std::time::Instant,
}

impl Write for Until {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        let mut o = self.out.lock().unwrap();
        if String::from_utf8_lossy(&o).matches("data: ").count() >= self.want
            || std::time::Instant::now() >= self.by
        {
            return Err(std::io::Error::other("enough"));
        }
        o.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn stream(r: Response, want: usize) -> String {
    let out: Arc<Mutex<Vec<u8>>> = Arc::default();
    if let Body::Stream(f) = r.body {
        f(&mut Until {
            out: out.clone(),
            want,
            by: std::time::Instant::now() + Duration::from_secs(20),
        });
    }
    String::from_utf8_lossy(&out.lock().unwrap()).into_owned()
}

/// A session whose host isn't running gives its info, and the stream ends
/// when the client goes.
#[test]
fn events_of_a_stopped_session() {
    let _h = home();
    let s = server(vec![]);
    let dir = rush_host::dir("d4d4d4d4");
    std::fs::create_dir_all(&dir).unwrap();
    let info = Info {
        id: "d4d4d4d4".into(),
        name: "past".into(),
        state: "stopped".into(),
        host_pid: 0,
        updated_at: Time::now(),
        ..Info::default()
    };
    std::fs::write(dir.join("info.json"), serde_json::to_vec(&info).unwrap()).unwrap();
    let r = call(
        &s,
        "GET",
        "/api/sessions/d4d4d4d4/events",
        "",
        &[("Authorization", &bearer(TOKEN))],
    );
    assert_eq!(r.get("Content-Type"), "text/event-stream");
    // The writer refuses the keepalive after these three, which ends it.
    let got = stream(r, 3);
    assert!(
        got.starts_with("data: {\"e\":null,\"t\":\"reset\"}\n\n"),
        "{got}"
    );
    assert!(got.contains("\"t\":\"info\""), "{got}");
    assert!(got.contains("\"name\":\"past\""), "{got}");
    // The fork's server.go:918: a stopped host's catch-up ends with synced,
    // with no position, so a client holding frames back shows them.
    assert!(
        got.ends_with("data: {\"e\":{\"position\":\"\"},\"t\":\"synced\"}\n\n"),
        "{got}"
    );
}

// The fork's server.go:1016 (pump): a running host's replay ends with its
// info, and synced, with no position, follows it on each attachment.
#[test]
fn events_of_a_running_session_say_synced_after_its_info() {
    let _h = home();
    let s = server(vec![]);
    let _f = fake_host("e5e5e5e5", "live", "/work");
    let r = call(
        &s,
        "GET",
        "/api/sessions/e5e5e5e5/events",
        "",
        &[("Authorization", &bearer(TOKEN))],
    );
    let got = stream(r, 3);
    let frames: Vec<&str> = got
        .split("\n\n")
        .filter_map(|f| f.strip_prefix("data: "))
        .collect();
    assert_eq!(frames.len(), 3, "{got}");
    assert_eq!(frames[0], r#"{"e":null,"t":"reset"}"#);
    assert!(frames[1].contains(r#""t":"info""#), "{got}");
    assert_eq!(frames[2], r#"{"e":{"position":""},"t":"synced"}"#);
}

/// What a stream sends a stopped host's client as its info file changes:
/// the whole catch-up once, then its info alone, then nothing until it
/// changes again; a stream of all of it (?full=1) catches up each time,
/// as before.
#[test]
fn a_stopped_hosts_updates() {
    use crate::server::{Catchup, stopped_update};
    let (t1, t2) = (Time::from_unix(100, 0), Time::from_unix(200, 0));
    let cases = [
        (None, t1, false, Catchup::Whole),
        (Some(t1), t1, false, Catchup::Nothing),
        (Some(t1), t2, false, Catchup::Info),
        (None, t1, true, Catchup::Whole),
        (Some(t1), t1, true, Catchup::Nothing),
        (Some(t1), t2, true, Catchup::Whole),
    ];
    for (shown, updated, full, want) in cases {
        assert_eq!(
            stopped_update(shown, updated, full),
            want,
            "{shown:?} {updated:?} {full}"
        );
    }
}

#[test]
fn the_sessions_stream_starts_with_the_list() {
    let _h = home();
    let s = server(vec![]);
    let dir = rush_host::dir("c3c3c3c3");
    std::fs::create_dir_all(&dir).unwrap();
    let info = Info {
        id: "c3c3c3c3".into(),
        name: "first".into(),
        state: "idle".into(),
        updated_at: Time::now(),
        ..Info::default()
    };
    std::fs::write(dir.join("info.json"), serde_json::to_vec(&info).unwrap()).unwrap();
    let r = call(
        &s,
        "GET",
        "/api/sessions/stream",
        "",
        &[("Authorization", &bearer(TOKEN))],
    );
    assert_eq!(r.get("Content-Type"), "text/event-stream");
    let got = stream(r, 1);
    assert!(got.starts_with("data: {\"sessions\":["), "{got}");
    assert!(got.contains("\"name\":\"first\""), "{got}");
    assert!(got.contains("\"alive\":false"), "{got}");
}

// The fork's server.go:1062 and :910: a session's slash commands reach its
// stream, live from its host and, for a stopped host, from its saved list.
#[test]
fn slash_commands_reach_the_stream() {
    use crate::server::wire_line;
    assert_eq!(
        String::from_utf8(
            wire_line(br#"{"commands":[{"name":"review","description":"Review a PR","argumentHint":"<pr>"}],"type":"agtop_commands"}"#)
                .unwrap()
        )
        .unwrap(),
        // As Go's event.Marshal writes it.
        r#"{"t":"commands","e":{"List":[{"Name":"review","Description":"Review a PR","ArgumentHint":"<pr>","Aliases":null}]}}"#
    );
    let _h = home();
    let s = server(vec![]);
    let dir = rush_host::dir("f6f6f6f6");
    std::fs::create_dir_all(&dir).unwrap();
    let info = Info {
        id: "f6f6f6f6".into(),
        state: "stopped".into(),
        updated_at: Time::now(),
        ..Info::default()
    };
    std::fs::write(dir.join("info.json"), serde_json::to_vec(&info).unwrap()).unwrap();
    std::fs::write(
        dir.join("commands.json"),
        r#"[{"Name":"review","Description":"Review a PR","ArgumentHint":"<pr>","Aliases":null}]"#,
    )
    .unwrap();
    let r = call(
        &s,
        "GET",
        "/api/sessions/f6f6f6f6/events",
        "",
        &[("Authorization", &bearer(TOKEN))],
    );
    let got = stream(r, 4);
    let types: Vec<String> = got
        .split("\n\n")
        .filter_map(|f| f.strip_prefix("data: "))
        .map(|f| {
            serde_json::from_str::<Value>(f).unwrap()["t"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    assert_eq!(types, ["reset", "info", "commands", "synced"], "{got}");
    assert!(got.contains(r#""Name":"review""#), "{got}");
}

#[test]
fn a_hosts_lines_as_the_web_app_reads_them() {
    use crate::server::wire_line;
    assert_eq!(
        wire_line(br#"{"type":"agtop_answered","request_id":"r1"}"#).unwrap(),
        br#"{"e":{"ID":"r1"},"t":"answered"}"#
    );
    assert_eq!(
        wire_line(br#"{"type":"agtop_error","error":"no"}"#).unwrap(),
        br#"{"e":{"Error":"no"},"t":"error"}"#
    );
    assert!(wire_line(b"rubbish").is_none());
}

/// attachPeers on attaches each peer, with no terminal; off ends it.
#[test]
fn attach_peers_follows_its_switch() {
    let _h = home();
    let (url, stop) = on_loopback(Arc::new(|_: &Request| Response::new(200, "[]")));
    update_config(|c| c.attach_peers = true).unwrap();
    let ctx = Ctx::new();
    let c2 = ctx.clone();
    let cfg = Config {
        peers: vec![Peer {
            name: "box".into(),
            url,
            token: "t".into(),
        }],
        ..Config::default()
    };
    let log: crate::expose::Log = Arc::new(Mutex::new(Vec::<u8>::new()));
    let t = std::thread::spawn(move || crate::attach::attach_peers(&c2, &cfg, &log));
    let there = || crate::git::unreachable("box").exists();
    until("box attached", there);
    update_config(|c| c.attach_peers = false).unwrap();
    until("box let go", || !there());
    ctx.cancel();
    t.join().unwrap();
    stop.cancel();
}

/// Waits for ok, at most 15s: what it waits on is a thread of its own.
fn until(what: &str, ok: impl Fn() -> bool) {
    let end = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < end {
        if ok() {
            return;
        }
        std::thread::park_timeout(Duration::from_millis(10));
    }
    panic!("timed out: {what}");
}

/// A serve that records what a start asked for, with programs folders.
fn starting_server(
    programs: Vec<std::path::PathBuf>,
) -> (Arc<Server>, Arc<Mutex<Option<StartRequest>>>) {
    let started: Arc<Mutex<Option<StartRequest>>> = Arc::default();
    let st = started.clone();
    let mut s = Server::new(
        Config {
            name: "box".into(),
            ..Config::default()
        },
        TOKEN.into(),
        rush_core::adapter::Registry::new(),
    );
    s.programs = programs;
    s.start = Some(Arc::new(move |r: &StartRequest| {
        *st.lock().unwrap() = Some(r.clone());
        Ok(Info::default())
    }));
    (Arc::new(s), started)
}

/// An executable file at p.
fn program(p: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(p, "#!/bin/sh\n").unwrap();
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// What a client that runs agents in its own sandbox starts a session
/// with: its labels, a permission mode, a fork, its wrapper and the
/// wrapper's environment. Each reaches the start as asked.
#[test]
fn start_takes_labels_mode_fork_and_a_served_program() {
    let _h = home();
    let progs = tempfile::tempdir().unwrap();
    let wrapper = progs.path().join("wrapper");
    program(&wrapper);
    let (s, started) = starting_server(vec![progs.path().to_path_buf()]);
    let b = bearer(TOKEN);
    let hdr = [
        ("Authorization", b.as_str()),
        ("Content-Type", "application/json"),
    ];
    let body = serde_json::json!({
        "cwd": "/work", "agent": "claude",
        "meta": {"app.chat": "c1", "app.installation": "i1"},
        "mode": "bypassPermissions",
        "forkFrom": "0f8fad5b-d9cb-469f-a165-70867728950e",
        "binary": wrapper.to_string_lossy(),
        "env": {"TOOL_TOKEN": "t", "HTTPS_PROXY": "http://127.0.0.1:9"},
    });
    let r = call(&s, "POST", "/api/sessions", &body.to_string(), &hdr);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(r.bytes()));
    let got = started.lock().unwrap().clone().unwrap();
    assert_eq!(got.meta.get("app.chat").map(String::as_str), Some("c1"));
    assert_eq!(got.meta.len(), 2);
    assert_eq!(got.mode, "bypassPermissions");
    assert_eq!(got.fork_from, "0f8fad5b-d9cb-469f-a165-70867728950e");
    assert_eq!(got.binary, wrapper.to_string_lossy());
    assert_eq!(
        got.env_list(),
        vec![
            "HTTPS_PROXY=http://127.0.0.1:9".to_string(),
            "TOOL_TOKEN=t".into()
        ]
    );
}

/// What serve refuses in a start, before anything starts: labels that
/// aren't fit to store, a fork that isn't one conversation, and any
/// program or environment a serve without programs folders, or outside
/// them, would otherwise run.
#[test]
fn start_refuses_unfit_labels_forks_and_programs() {
    let _h = home();
    let progs = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let wrapper = progs.path().join("wrapper");
    program(&wrapper);
    let outside = other.path().join("claude");
    program(&outside);
    let plain = progs.path().join("notes.txt");
    std::fs::write(&plain, "").unwrap();
    let link = progs.path().join("link");
    std::os::unix::fs::symlink(&outside, &link).unwrap();
    let w = wrapper.to_string_lossy().into_owned();
    let many_labels: serde_json::Map<String, Value> = (0..17)
        .map(|i| (format!("k{i}"), Value::from("v")))
        .collect();
    let many_vars: serde_json::Map<String, Value> = (0..65)
        .map(|i| (format!("V{i}"), Value::from("v")))
        .collect();
    let uuid = "0f8fad5b-d9cb-469f-a165-70867728950e";
    let with_programs = true;
    let cases: Vec<(&str, bool, Value, u16, &str)> = vec![
        (
            "rush's own label",
            with_programs,
            serde_json::json!({"meta": {"spawnedBy": "x"}}),
            400,
            "label spawnedBy is rush's own",
        ),
        (
            "too many labels",
            with_programs,
            serde_json::json!({"meta": many_labels}),
            400,
            "at most 16 labels",
        ),
        (
            "long label name",
            with_programs,
            serde_json::json!({"meta": {"k".repeat(65): "v"}}),
            400,
            "a label's name must be 1 to 64 bytes and its value at most 512",
        ),
        (
            "empty label name",
            with_programs,
            serde_json::json!({"meta": {"": "v"}}),
            400,
            "a label's name must be 1 to 64 bytes and its value at most 512",
        ),
        (
            "long label value",
            with_programs,
            serde_json::json!({"meta": {"k": "v".repeat(513)}}),
            400,
            "a label's name must be 1 to 64 bytes and its value at most 512",
        ),
        (
            "fork of no uuid",
            with_programs,
            serde_json::json!({"forkFrom": "nope"}),
            400,
            "a fork names one conversation to copy, and resumes none",
        ),
        (
            "fork and resume",
            with_programs,
            serde_json::json!({"forkFrom": uuid, "resume": uuid}),
            400,
            "a fork names one conversation to copy, and resumes none",
        ),
        (
            "program, no folders",
            !with_programs,
            serde_json::json!({"binary": w}),
            403,
            "this serve runs only the agents' own programs, with their own environment",
        ),
        (
            "env, no folders",
            !with_programs,
            serde_json::json!({"env": {"A": "b"}}),
            403,
            "this serve runs only the agents' own programs, with their own environment",
        ),
        (
            "program outside",
            with_programs,
            serde_json::json!({"binary": outside.to_string_lossy()}),
            403,
            "that program isn't in one of this serve's program folders",
        ),
        (
            "link out of folder",
            with_programs,
            serde_json::json!({"binary": link.to_string_lossy()}),
            403,
            "that program isn't in one of this serve's program folders",
        ),
        (
            "relative program",
            with_programs,
            serde_json::json!({"binary": "wrapper"}),
            403,
            "the program must be a full path",
        ),
        (
            "not a program",
            with_programs,
            serde_json::json!({"binary": plain.to_string_lossy()}),
            403,
            &*Box::leak(
                format!(
                    "{} isn't a program on this machine",
                    plain.to_string_lossy()
                )
                .into_boxed_str(),
            ),
        ),
        (
            "rush's own variable",
            with_programs,
            serde_json::json!({"binary": w, "env": {"RUSH_HOME": "/x"}}),
            403,
            "RUSH_HOME is rush's own",
        ),
        (
            "bad variable name",
            with_programs,
            serde_json::json!({"binary": w, "env": {"A=B": "x"}}),
            403,
            "the environment variable \"A=B\" can't be set",
        ),
        (
            "too many variables",
            with_programs,
            serde_json::json!({"binary": w, "env": many_vars}),
            403,
            "at most 64 environment variables",
        ),
    ];
    for (name, programs, extra, want_status, want_err) in cases {
        let dirs = if programs {
            vec![progs.path().to_path_buf()]
        } else {
            vec![]
        };
        let (s, started) = starting_server(dirs);
        let b = bearer(TOKEN);
        let hdr = [
            ("Authorization", b.as_str()),
            ("Content-Type", "application/json"),
        ];
        let mut body = serde_json::json!({"cwd": "/work", "agent": "claude"});
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let r = call(&s, "POST", "/api/sessions", &body.to_string(), &hdr);
        let got: Value = serde_json::from_slice(r.bytes()).unwrap_or_default();
        assert_eq!(
            (r.status, got["error"].as_str().unwrap_or("")),
            (want_status, want_err),
            "{name}"
        );
        assert!(started.lock().unwrap().is_none(), "{name}: it started");
    }
}

/// MCP servers by name, each run by command, as a start or an mcp op
/// names them.
fn mcp_of(names: &[&str], command: &str) -> Value {
    names
        .iter()
        .map(|n| (n.to_string(), serde_json::json!({ "command": command })))
        .collect::<serde_json::Map<String, Value>>()
        .into()
}

/// What a start or an mcp op may not name, on a serve with programs
/// folders, and the 403's reason for each (the fork's server.go:462-484,
/// allowedMCP): a program outside them, a name of rush's own (its tools'
/// server, or a plugin's), one an agent can't put in its tools' names,
/// and an environment a program of the serve's own may not be given.
fn unfit_mcp(prog: &str, stray: &str) -> Vec<(&'static str, Value, String)> {
    let many: Vec<String> = (0..17).map(|i| format!("s{i}")).collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    let long = "n".repeat(65);
    vec![
        (
            "another folder",
            mcp_of(&["x"], stray),
            "MCP server x: that program isn't in one of this serve's program folders".into(),
        ),
        (
            "rush's own",
            mcp_of(&["rush"], prog),
            "an MCP server can't be named \"rush\"".into(),
        ),
        (
            "a plugin's",
            mcp_of(&["rush-notes"], prog),
            "an MCP server can't be named \"rush-notes\"".into(),
        ),
        (
            "a bad name",
            mcp_of(&["A B"], prog),
            "an MCP server can't be named \"A B\"".into(),
        ),
        (
            "a long name",
            mcp_of(&[&long], prog),
            format!("an MCP server can't be named \"{long}\""),
        ),
        (
            "no program",
            serde_json::json!({"x": {}}),
            "MCP server x: the program must be a full path".into(),
        ),
        (
            "a rush variable",
            serde_json::json!({"x": {"command": prog, "env": {"RUSH_SESSION": "y"}}}),
            "MCP server x: RUSH_SESSION is rush's own".into(),
        ),
        (
            "a bad variable",
            serde_json::json!({"x": {"command": prog, "env": {"A=B": "y"}}}),
            "MCP server x: the environment variable \"A=B\" can't be set".into(),
        ),
        (
            "too many",
            mcp_of(&many, prog),
            "at most 16 MCP servers".into(),
        ),
    ]
}

// The fork's programs_test.go:111 (TestStartMCPOnlyFromServeFolders): a
// start's MCP servers reach the host only from a serve told to take
// programs, each server's program from one of its folders, as a list in
// name order; one named without args or env (a client's own MCP server,
// as it names it) has none.
#[test]
fn start_takes_mcp_servers_only_from_serve_folders() {
    let _h = home();
    let progs = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let prog = progs.path().join("threads-mcp");
    let stray = other.path().join("threads-mcp");
    program(&prog);
    program(&stray);
    let (p, st) = (prog.to_string_lossy(), stray.to_string_lossy());
    let b = bearer(TOKEN);
    let hdr = [
        ("Authorization", b.as_str()),
        ("Content-Type", "application/json"),
    ];
    let start = |s: &Arc<Server>, mcp: Value| {
        let body = serde_json::json!({"cwd": "/work", "agent": "claude", "mcp": mcp});
        call(s, "POST", "/api/sessions", &body.to_string(), &hdr)
    };
    let good = serde_json::json!({
        "threads": {"command": p, "args": ["serve"]},
        "notes": {"command": p},
    });

    let (s, started) = starting_server(vec![]);
    let r = start(&s, good.clone());
    let got: Value = serde_json::from_slice(r.bytes()).unwrap_or_default();
    assert_eq!(
        (r.status, got["error"].as_str().unwrap_or("")),
        (
            403,
            "this serve runs only the agents' own programs, with their own environment"
        ),
        "no folders"
    );
    assert!(started.lock().unwrap().is_none(), "no folders: it started");

    let (s, started) = starting_server(vec![progs.path().to_path_buf()]);
    let r = start(&s, good);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(r.bytes()));
    let got = started.lock().unwrap().clone().unwrap().mcp_list();
    let server = |name: &str, args: &[&str]| rush_core::adapter::McpServer {
        name: name.into(),
        command: p.to_string(),
        args: args.iter().map(|a| a.to_string()).collect(),
        ..Default::default()
    };
    assert_eq!(
        got,
        vec![server("notes", &[]), server("threads", &["serve"])]
    );

    for (name, mcp, want) in unfit_mcp(&p, &st) {
        let (s, started) = starting_server(vec![progs.path().to_path_buf()]);
        let r = start(&s, mcp);
        let got: Value = serde_json::from_slice(r.bytes()).unwrap_or_default();
        assert_eq!(
            (r.status, got["error"].as_str().unwrap_or("")),
            (403, want.as_str()),
            "{name}"
        );
        assert!(started.lock().unwrap().is_none(), "{name}: it started");
    }
}

/// A serve on this machine with programs folders progs, that waits wait
/// for a host's answer to an mcp op.
fn mcp_server(progs: Vec<std::path::PathBuf>, wait: Duration) -> Server {
    let mut s = Server::new(
        Config {
            name: "box".into(),
            ..Config::default()
        },
        TOKEN.into(),
        rush_core::adapter::Registry::new(),
    );
    s.programs = progs;
    s.mcp_wait = wait;
    s
}

/// What an op answered: its status and its error, if any.
fn op_answer(s: &Arc<Server>, body: &Value) -> (u16, String) {
    let b = bearer(TOKEN);
    let hdr = [
        ("Authorization", b.as_str()),
        ("Content-Type", "application/json"),
    ];
    let r = call(
        s,
        "POST",
        "/api/sessions/feedbeef/op",
        &body.to_string(),
        &hdr,
    );
    let got: Value = serde_json::from_slice(r.bytes()).unwrap_or_default();
    (r.status, got["error"].as_str().unwrap_or("").to_string())
}

// The fork's programs_test.go:159 (TestOpMCPOnlyFromServeFolders): an mcp
// op adds MCP servers to a running session on the same terms as a start's.
// The host gets them as a list in name order, each with its own
// environment, under an id it answers: 202 once it took them, 502 with its
// reason when it didn't, 502 when it went away and 504 when it never said
// (server.go:548-570, awaitReply). An op that names no servers, or servers
// on another op, is a 400.
#[test]
fn an_mcp_op_adds_servers_only_from_serve_folders() {
    let _h = home();
    let f = fake_host("feedbeef", "job", "/work");
    let progs = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let prog = progs.path().join("threads-mcp");
    let stray = other.path().join("threads-mcp");
    program(&prog);
    program(&stray);
    let (p, st) = (prog.to_string_lossy(), stray.to_string_lossy());
    let good = serde_json::json!({"op": "mcp", "mcp": {
        "threads": {"command": p, "args": ["serve"], "env": {"TOOL_TOKEN": "tok"}},
    }});

    let s = Arc::new(mcp_server(vec![], Duration::from_secs(5)));
    assert_eq!(
        op_answer(&s, &good),
        (
            403,
            "this serve runs only the agents' own programs, with their own environment".into()
        ),
        "no folders"
    );
    f.no_op();

    let s = Arc::new(mcp_server(
        vec![progs.path().to_path_buf()],
        Duration::from_millis(300),
    ));
    assert_eq!(op_answer(&s, &good), (202, String::new()));
    let mut got = f.op();
    assert!(
        got["op"] == "mcp" && got["id"].as_str().is_some_and(|id| id.len() == 16),
        "the host got {got}, not an mcp op with an id to answer"
    );
    assert_eq!(
        got["mcp"].take(),
        serde_json::json!([{"name": "threads", "command": p, "args": ["serve"], "env": {"TOOL_TOKEN": "tok"}}])
    );
    // A client's own server, named without args: none reach the host.
    let bare =
        serde_json::json!({"op": "mcp", "mcp": {"threads": {"command": p, "env": {"K": "V"}}}});
    assert_eq!(op_answer(&s, &bare), (202, String::new()));
    assert_eq!(
        f.op()["mcp"],
        serde_json::json!([{"name": "threads", "command": p, "env": {"K": "V"}}])
    );

    for (answer, want) in [
        (
            McpAnswer::Refuse("the server did not start"),
            (502, "the server did not start"),
        ),
        (McpAnswer::Gone, (502, "the session's host went away")),
        (
            McpAnswer::Silent,
            (504, "the session's host did not answer"),
        ),
    ] {
        *f.mcp.lock().unwrap() = answer;
        let (code, err) = op_answer(&s, &good);
        assert_eq!((code, err.as_str()), want);
        assert_eq!(f.op()["op"], "mcp");
    }
    *f.mcp.lock().unwrap() = McpAnswer::Take;

    let mut cases: Vec<(&str, Value, u16, String)> = unfit_mcp(&p, &st)
        .into_iter()
        .map(|(name, mcp, why)| (name, serde_json::json!({"op": "mcp", "mcp": mcp}), 403, why))
        .collect();
    let xor = "an mcp op, and only one, names MCP servers".to_string();
    cases.extend([
        (
            "no server",
            serde_json::json!({"op": "mcp"}),
            400,
            xor.clone(),
        ),
        (
            "no servers",
            serde_json::json!({"op": "mcp", "mcp": {}}),
            400,
            xor.clone(),
        ),
        (
            "servers on a send",
            serde_json::json!({"op": "send", "text": "hi", "mcp": mcp_of(&["x"], &p)}),
            400,
            xor,
        ),
    ]);
    for (name, body, code, why) in cases {
        assert_eq!(op_answer(&s, &body), (code, why), "{name}");
        f.no_op();
    }
}

// The fork's programs_test.go:231 (TestOpMCPRestartsAHostFromBeforeMCPOps):
// a host from before mcp ops (Proto under 13) never answers one. Serve
// doesn't send it the op: it restarts the host on this rush with the
// servers, at once between turns (202), else once the turn has ended,
// answering 409 at once so the caller tries again with its next op. A
// host whose session runs every server named already is left as it is.
#[test]
fn an_mcp_op_restarts_a_host_from_before_mcp_ops() {
    let _h = home();
    let f = fake_host("feedbeef", "job", "/work");
    let progs = tempfile::tempdir().unwrap();
    let prog = progs.path().join("threads-mcp");
    program(&prog);
    let p = prog.to_string_lossy();
    let (tx, restarts) = channel::<String>();
    let tx = Mutex::new(tx);
    let mut s = mcp_server(vec![progs.path().to_path_buf()], Duration::from_secs(5));
    s.quiet_poll = Duration::from_millis(20);
    s.restart_mcp = Some(Arc::new(
        move |id: &str, servers: &[rush_core::adapter::McpServer]| {
            let _ = tx
                .lock()
                .unwrap()
                .send(format!("{id} {}", serde_json::to_string(servers).unwrap()));
            Ok(())
        },
    ));
    let s = Arc::new(s);
    let good = serde_json::json!({"op": "mcp", "mcp": {
        "threads": {"command": p, "env": {"TOOL_TOKEN": "tok"}},
    }});
    let want =
        format!(r#"feedbeef [{{"name":"threads","command":"{p}","env":{{"TOOL_TOKEN":"tok"}}}}]"#);
    let restarted = || {
        restarts
            .recv_timeout(Duration::from_secs(5))
            .expect("the host was not restarted")
    };
    let no_restart = |why: &str| {
        if let Ok(r) = restarts.try_recv() {
            panic!("{why}: restarted {r}");
        }
    };

    // between turns: restarted at once, with the servers
    f.set_info(|i| i.proto = rush_host::MCP_FROM - 1);
    assert_eq!(op_answer(&s, &good), (202, String::new()), "between turns");
    assert_eq!(restarted(), want);
    f.no_op();

    // mid-turn: 409 at once, restarted when the turn has ended
    f.set_info(|i| i.state = "working".into());
    assert_eq!(
        op_answer(&s, &good),
        (
            409,
            "the session's host is from before mcp ops: it is restarted with the servers when its turn has ended".into()
        ),
        "mid-turn"
    );
    no_restart("mid-turn");
    f.set_info(|i| i.state = "idle".into());
    assert_eq!(restarted(), want);
    f.no_op();

    // a session that runs the server already: 202 and nothing restarts,
    // between turns or mid-turn
    let cfg = rush_host::Config {
        id: "feedbeef".into(),
        mcp: vec![rush_core::adapter::McpServer {
            name: "threads".into(),
            command: p.to_string(),
            ..Default::default()
        }],
        ..rush_host::Config::default()
    };
    std::fs::write(
        rush_host::dir("feedbeef").join("config.json"),
        cfg.to_json_indent(),
    )
    .unwrap();
    for state in ["idle", "working"] {
        f.set_info(|i| i.state = state.into());
        assert_eq!(op_answer(&s, &good), (202, String::new()), "{state}");
        no_restart(state);
    }
    f.no_op();

    // a current host takes the op itself
    f.set_info(|i| {
        i.proto = rush_host::PROTO;
        i.state = "idle".into();
    });
    assert_eq!(op_answer(&s, &good), (202, String::new()), "a current host");
    assert_eq!(f.op()["op"], "mcp");
    no_restart("a current host");
}

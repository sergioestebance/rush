//! Restarting an agent restarts its host too, on the installed rush
//! (client::restart_agent): the conversation, its queue and its settings
//! carry over, and a start that fails says why and can be tried again.
//! Hosts here are fake ones in this process (crate::fake), started as
//! the installed rush would start them.

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use rush_core::adapter::McpServer;

use crate::client::{
    Client, Result, read_config, read_info, restart_agent_on, restart_with_mcp_on,
};
use crate::fake::mcp_line;
use crate::testing::Home;
use crate::wire::Config;

/// A stand-in installed rush that answers `rush host preflight` with
/// what it prints and its exit status.
fn installed(home: &Home, says: &str, status: i32) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let bin = home.path().join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let exe = bin.join("rush");
    std::fs::write(&exe, format!("#!/bin/sh\necho '{says}'\nexit {status}\n")).unwrap();
    std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
    exe
}

/// One that can run every session.
fn ok_rush(home: &Home) -> PathBuf {
    installed(home, crate::reload::PREFLIGHT_OK, 0)
}

/// A fake host for session id, mid-turn (its agent asks to run its
/// shell), with a model and effort set.
fn start(home: &Home, id: &str) -> Config {
    start_with(home, id, Vec::new())
}

/// start, the session running its caller's MCP servers mcp.
fn start_with(home: &Home, id: &str, mcp: Vec<McpServer>) -> Config {
    let acct = home.path().join("acct");
    std::fs::create_dir_all(&acct).unwrap();
    let mut cfg = Config {
        id: id.into(),
        session_id: format!("{id}-conversation"),
        resume: true,
        kind: "fake".into(),
        cwd: home.path().to_string_lossy().into_owned(),
        prompt: "carry on".into(),
        model: "fake-big".into(),
        effort: "high".into(),
        mcp,
        ..Config::default()
    };
    cfg.account.kind = "fake".into();
    cfg.account.dir = acct.to_string_lossy().into_owned();
    let cfg = crate::fake::spawn(crate::fake::registry(), cfg).unwrap();
    assert!(until(home, id, || read_info(id)
        .is_ok_and(|i| i.state == "blocked")));
    cfg
}

/// Waits, up to 10 s, for what an in-process host does on its own
/// threads to show in its folder or its account's: true once ok says.
fn until(home: &Home, id: &str, ok: impl Fn() -> bool) -> bool {
    let w = rush_sys::fswait::Watcher::new();
    w.watch(&[home.path().join("acct"), crate::paths::dir(id)]);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        crate::events::flush();
        if ok() {
            return true;
        }
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        w.wait(None, Some((deadline - now).min(Duration::from_millis(50))));
    }
}

/// The agent's starts, each with the conversation it resumed.
fn starts(home: &Home) -> Vec<String> {
    std::fs::read_to_string(home.path().join("acct").join("starts.log"))
        .unwrap_or_default()
        .lines()
        .map(String::from)
        .collect()
}

fn events(id: &str) -> String {
    crate::events::flush();
    std::fs::read_to_string(crate::paths::events_path(id)).unwrap_or_default()
}

fn stop(id: &str) {
    if let Ok(c) = Client::dial(id) {
        let _ = c.stop();
        c.close();
    }
}

/// Starts hosts as the rush at the path given would, noting each path.
struct Spawner(Mutex<Vec<PathBuf>>);

impl Spawner {
    fn new() -> Spawner {
        Spawner(Mutex::new(Vec::new()))
    }
    fn spawn(&self, cfg: Config, exe: &Path) -> Result<()> {
        self.0.lock().unwrap().push(exe.to_path_buf());
        crate::fake::spawn(crate::fake::registry(), cfg).map(|_| ())
    }
    fn exes(&self) -> Vec<PathBuf> {
        self.0.lock().unwrap().clone()
    }
}

// A restart stops the host with its agent and starts a new host from the
// installed rush, on the same conversation, folder, model and effort;
// the message it had queued goes to the new one's agent, which resumes.
#[test]
fn a_restart_starts_a_new_host_from_the_installed_rush() {
    let home = Home::new();
    let cfg = start(&home, "ra1");
    let exe = ok_rush(&home);
    let before = read_info("ra1").unwrap();
    {
        let c = Client::dial("ra1").unwrap();
        c.send("then this").unwrap();
        c.close();
    }
    let queued = || read_info("ra1").is_ok_and(|i| i.queue.iter().any(|q| q == "then this"));
    assert!(until(&home, "ra1", queued), "{}", events("ra1"));

    let sp = Spawner::new();
    restart_agent_on("ra1", "", &exe, &|c: Config, e: &Path| sp.spawn(c, e)).unwrap();
    assert_eq!(
        sp.exes(),
        std::slice::from_ref(&exe),
        "started from the installed rush"
    );

    // Nothing was sent with it: the agent starts again for the queue.
    assert!(
        until(&home, "ra1", || starts(&home).len() == 2),
        "{:?} {}",
        starts(&home),
        events("ra1")
    );
    let s = starts(&home);
    assert_eq!(s[1], format!("start {}", cfg.session_id), "resumed");

    let after = read_info("ra1").unwrap();
    assert!(after.generation > before.generation, "a new host");
    assert_eq!(after.session_id, cfg.session_id);
    let back = read_config("ra1").unwrap();
    assert_eq!(back.session_id, cfg.session_id);
    assert_eq!(back.cwd, cfg.cwd);
    assert_eq!(
        (back.model.as_str(), back.effort.as_str()),
        ("fake-big", "high")
    );
    assert!(back.resume && back.prompt.is_empty(), "{back:?}");
    let ev = events("ra1");
    assert!(ev.contains("\"agent-restart\""), "{ev}");
    assert!(ev.contains(&exe.to_string_lossy().into_owned()), "{ev}");
    stop("ra1");
}

// A new host that doesn't start says why; the session keeps its config
// and conversation, and a restart after starts it, sending what's given.
#[test]
fn a_failed_start_says_why_and_can_be_restarted() {
    let home = Home::new();
    let cfg = start(&home, "ra2");
    let exe = ok_rush(&home);

    let failing = |_: Config, e: &Path| -> Result<()> {
        Err(format!("no rush to start at {}", e.display()).into())
    };
    let err = restart_agent_on("ra2", "", &exe, &failing)
        .unwrap_err()
        .to_string();
    assert!(err.contains("no rush to start at"), "{err}");
    assert!(Client::dial("ra2").is_err(), "the old host stopped");
    let ev = events("ra2");
    assert!(
        ev.contains("\"agent-restart\"") && ev.contains("no rush to start at"),
        "{ev}"
    );
    let kept = read_config("ra2").unwrap();
    assert_eq!(kept.session_id, cfg.session_id);

    let sp = Spawner::new();
    restart_agent_on("ra2", "go on", &exe, &|c: Config, e: &Path| sp.spawn(c, e)).unwrap();
    assert_eq!(sp.exes(), [exe]);
    assert!(
        until(&home, "ra2", || starts(&home).len() == 2),
        "{:?} {}",
        starts(&home),
        events("ra2")
    );
    assert_eq!(starts(&home)[1], format!("start {}", cfg.session_id));
    stop("ra2");
}

// An installed rush that can't run the session leaves it as it is: the
// host and its agent carry on, and the error says why.
#[test]
fn an_installed_rush_that_cant_run_it_leaves_it_running() {
    let home = Home::new();
    start(&home, "ra3");
    let exe = installed(&home, "rush: its folder is gone", 1);
    let sp = Spawner::new();
    let err = restart_agent_on("ra3", "", &exe, &|c: Config, e: &Path| sp.spawn(c, e))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("can't run ra3") && err.contains("its folder is gone"),
        "{err}"
    );
    assert!(sp.exes().is_empty(), "nothing started");
    assert!(Client::dial("ra3").is_ok(), "its host carries on");
    assert_eq!(starts(&home).len(), 1, "its agent untouched");
    stop("ra3");
}

/// Whether another wake holds session id's startup lock: a lock of our
/// own on a fresh open of wake.lock is refused while one is held.
fn wake_locked(id: &str) -> bool {
    use std::os::fd::AsRawFd;
    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(crate::paths::dir(id).join("wake.lock"))
        .unwrap();
    // SAFETY: f's fd, open for this call.
    let got = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0;
    !got // closing f lets ours go
}

// A restart holds the startup lock a wake takes (ensure) from stopping the
// old host until the new one accepts connections, so a message that wakes
// the session meanwhile can't start a second host for it. Let go after.
#[test]
fn a_restart_holds_the_wake_lock_while_it_starts_the_new_host() {
    let home = Home::new();
    start(&home, "ra3");
    let exe = ok_rush(&home);
    assert!(!wake_locked("ra3"));
    let sp = Spawner::new();
    let held = Mutex::new(Vec::new());
    restart_agent_on("ra3", "", &exe, &|c: Config, e: &Path| {
        held.lock().unwrap().push(wake_locked("ra3"));
        sp.spawn(c, e)
    })
    .unwrap();
    assert_eq!(
        *held.lock().unwrap(),
        [true],
        "spawned without the wake lock"
    );
    // A process another test forks meanwhile shares the lock until it
    // execs, so let go is looked for, not assumed at once.
    assert!(
        until(&home, "ra3", || !wake_locked("ra3")),
        "the lock outlived the restart"
    );
    stop("ra3");
}

/// Servers named by name=command pairs.
fn servers(pairs: &[(&str, &str)]) -> Vec<McpServer> {
    pairs
        .iter()
        .map(|(n, c)| McpServer {
            name: (*n).into(),
            command: (*c).into(),
            ..McpServer::default()
        })
        .collect()
}

// The fork's mcp_test.go:197 (TestRestartWithMCPStartsTheHostAgainWithTheServers):
// a host from before mcp ops is restarted on the installed rush with the
// servers added to the session's own, by name, and its agent starts again
// with them; a name it runs already keeps its own server.
#[test]
fn a_restart_with_mcp_starts_the_host_again_with_the_servers() {
    let home = Home::new();
    start_with(&home, "rm1", servers(&[("notes", "/run/notes")]));
    let exe = ok_rush(&home);
    let sp = Spawner::new();
    let add = servers(&[("threads", "/run/threads"), ("notes", "/run/other")]);
    let before = read_info("rm1").unwrap();
    restart_with_mcp_on("rm1", &add, &exe, &|c: Config, e: &Path| sp.spawn(c, e)).unwrap();
    assert_eq!(sp.exes(), std::slice::from_ref(&exe), "not restarted");
    assert!(
        read_info("rm1").unwrap().generation > before.generation,
        "a new host"
    );
    assert_eq!(
        mcp_line(&read_config("rm1").unwrap().mcp),
        "notes=/run/notes threads=/run/threads"
    );
    // Its agent starts again for the next message, with them.
    let c = Client::dial("rm1").unwrap();
    c.send("go on").unwrap();
    c.close();
    assert!(
        until(&home, "rm1", || starts(&home).len() == 2),
        "{:?}",
        starts(&home)
    );
    assert!(
        starts(&home)[1].ends_with(" mcp=notes=/run/notes threads=/run/threads"),
        "{:?}",
        starts(&home)
    );
    stop("rm1");
}

// The fork's mcp_test.go:237 and :252: a host whose session runs every
// server named already is left as it is (nothing to add is no reason to
// stop what it does), and a restart for servers holds the wake lock while
// it starts the new host, as every restart does.
#[test]
fn a_restart_with_mcp_leaves_a_host_running_them_and_holds_the_wake_lock() {
    let home = Home::new();
    start_with(&home, "rm2", servers(&[("threads", "/run/threads")]));
    let exe = ok_rush(&home);
    let sp = Spawner::new();
    let held = Mutex::new(Vec::new());
    let spawn = |c: Config, e: &Path| {
        held.lock().unwrap().push(wake_locked("rm2"));
        sp.spawn(c, e)
    };
    restart_with_mcp_on("rm2", &servers(&[("threads", "/run/other")]), &exe, &spawn).unwrap();
    assert!(sp.exes().is_empty(), "a host running them restarted");
    assert_eq!(starts(&home).len(), 1, "its agent untouched");
    restart_with_mcp_on("rm2", &servers(&[("notes", "/run/notes")]), &exe, &spawn).unwrap();
    assert_eq!(
        *held.lock().unwrap(),
        [true],
        "spawned without the wake lock"
    );
    stop("rm2");
}

// The fork's stale.go:108-127 (RestartWithMCP): what a restart for servers
// refuses, leaving the session as it is: one past the 16 a session runs,
// and a stand-in's host, which ends with the program that started it.
#[test]
fn a_restart_with_mcp_refuses_too_many_and_a_stand_in() {
    let home = Home::new();
    let exe = ok_rush(&home);
    let sixteen: Vec<(String, String)> = (0..16)
        .map(|i| (format!("s{i:02}"), format!("/run/s{i}")))
        .collect();
    let sixteen: Vec<(&str, &str)> = sixteen
        .iter()
        .map(|(n, c)| (n.as_str(), c.as_str()))
        .collect();
    let cases = [
        (
            "rm3",
            Config {
                mcp: servers(&sixteen),
                ..Config::default()
            },
            "a session runs at most 16 MCP servers of its caller's",
        ),
        (
            "rm4",
            Config {
                owner: 4242,
                ..Config::default()
            },
            "a stand-in's host ends with the program that started it",
        ),
    ];
    for (id, mut cfg, want) in cases {
        cfg.id = id.into();
        cfg.kind = "fake".into();
        std::fs::create_dir_all(crate::paths::dir(id)).unwrap();
        std::fs::write(
            crate::paths::dir(id).join("config.json"),
            cfg.to_json_indent(),
        )
        .unwrap();
        let sp = Spawner::new();
        let e = restart_with_mcp_on(
            id,
            &servers(&[("threads", "/run/threads")]),
            &exe,
            &|c: Config, e: &Path| sp.spawn(c, e),
        )
        .expect_err(want);
        assert_eq!(e.to_string(), want, "{id}");
        assert!(sp.exes().is_empty(), "{id}: restarted");
    }
    let _ = &home;
}

//! A session's slash commands, kept in its folder (commands.json) as its
//! agent lists them, so a client of a session whose agent isn't running
//! still gets them, and a host started later has them before its agent
//! lists its own.
//!
//! Ported from the fork's internal/host/commands.go (commandsPath :14,
//! WriteCommands :17, ReadCommands :30, loadCommands :44).

use std::path::PathBuf;

use rush_core::event::Command;

use crate::paths::dir;

fn commands_path(id: &str) -> PathBuf {
    dir(id).join("commands.json")
}

/// Saves a session's slash commands, replacing the file whole.
pub fn write_commands(id: &str, list: &[Command]) -> std::io::Result<()> {
    let b = serde_json::to_vec(list).map_err(std::io::Error::other)?;
    crate::json::write_private(&commands_path(id), &b)
}

/// A session's saved slash commands, None when it has none.
pub fn read_commands(id: &str) -> Option<Vec<Command>> {
    let b = std::fs::read(commands_path(id)).ok()?;
    serde_json::from_slice(&b).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Home;

    /// What's written reads back; the file as Go writes it reads too; a
    /// session with none, or a file that isn't a list, has none.
    #[test]
    fn commands_files() {
        let _h = Home::new();
        std::fs::create_dir_all(dir("c1")).unwrap();
        assert_eq!(read_commands("c1"), None);
        let list = vec![Command {
            name: "review".into(),
            aliases: vec!["r".into()],
            ..Command::default()
        }];
        write_commands("c1", &list).unwrap();
        assert_eq!(read_commands("c1"), Some(list));
        std::fs::write(
            commands_path("c1"),
            r#"[{"Name":"plan","Description":"Plan it","ArgumentHint":"","Aliases":null}]"#,
        )
        .unwrap();
        assert_eq!(read_commands("c1").unwrap()[0].description, "Plan it");
        std::fs::write(commands_path("c1"), "{}").unwrap();
        assert_eq!(read_commands("c1"), None);
    }
}

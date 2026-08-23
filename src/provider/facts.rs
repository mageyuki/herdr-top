//! Provider-neutral facts extracted from append-only agent logs.

use std::path::PathBuf;

/// Identity and ownership of the session evidenced by a log record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionScope {
    /// A root Claude session identified by its session ID.
    ClaudeRoot(String),
    /// A Claude subagent identified alongside its owning root session.
    ClaudeSubagent {
        /// Owning root Claude session ID.
        parent: String,
        /// Claude subagent ID.
        agent_id: String,
    },
    /// A Codex session identified by its rollout ID.
    Codex {
        /// Codex rollout ID.
        rollout_id: String,
    },
}

/// Allowlisted evidence extracted from one provider log record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogFact {
    /// Evidence that a scoped log was appended at a provider timestamp.
    Append {
        /// Session that owns the appended record.
        scope: SessionScope,
        /// Provider timestamp in Unix epoch milliseconds.
        at_ms: i64,
    },
    /// Claude-generated title for a session.
    AiTitle {
        /// Claude session ID named by the title record.
        session_id: String,
        /// Generated session title.
        title: String,
    },
    /// Evidence that a Claude subagent appeared.
    SubagentAppeared {
        /// Owning root Claude session ID.
        parent: String,
        /// Claude subagent ID.
        agent_id: String,
        /// Allowlisted subagent role.
        agent_type: String,
        /// Allowlisted short subagent description.
        description: String,
    },
    /// Evidence that a Claude subagent ended.
    SubagentEnded {
        /// Owning root Claude session ID.
        parent: String,
        /// Claude subagent ID.
        agent_id: String,
        /// Whether the reported status was not completed.
        failed: bool,
    },
    /// Sanitized short activity evidenced by a tool-use block.
    Activity {
        /// Session that performed the activity.
        scope: SessionScope,
        /// Provider timestamp in Unix epoch milliseconds.
        at_ms: i64,
        /// Sanitized activity text of at most 60 characters.
        line: String,
    },
    /// One provider-reported output-token usage sample.
    Usage {
        /// Session charged for the sample.
        scope: SessionScope,
        /// Provider timestamp in Unix epoch milliseconds.
        at_ms: i64,
        /// Provider sample identity used for downstream deduplication.
        sample_id: String,
        /// Output tokens only.
        output_tokens: u64,
        /// Allowlisted provider model name, when present.
        model: Option<String>,
        /// Allowlisted provider effort setting, when present.
        effort: Option<String>,
    },
    /// Identifier found by the bounded raw-line evidence scan.
    EvidenceId {
        /// Session whose raw line contained the identifier.
        parent: SessionScope,
        /// Extracted identifier token.
        id: EvidenceId,
    },
}

/// Narrow identifier evidence that may be scanned from a raw log line.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EvidenceId {
    /// Lowercase hexadecimal UUID-shaped token.
    Uuid(String),
    /// Path assigned to `CLAUDE_CONFIG_DIR`.
    ConfigDir(PathBuf),
}

/// Scans a raw line for allowlisted identifier token patterns.
#[must_use]
pub fn scan_raw_ids(line: &str) -> Vec<EvidenceId> {
    const CONFIG_PREFIX: &[u8] = b"CLAUDE_CONFIG_DIR=";
    const UUID_LEN: usize = 36;

    let bytes = line.as_bytes();
    let mut found = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if index + UUID_LEN <= bytes.len() && uuid_at(bytes, index) {
            let before_is_hex = index
                .checked_sub(1)
                .is_some_and(|before| bytes[before].is_ascii_hexdigit());
            let after_is_hex = bytes
                .get(index + UUID_LEN)
                .is_some_and(u8::is_ascii_hexdigit);
            if !before_is_hex && !after_is_hex {
                let id = EvidenceId::Uuid(line[index..index + UUID_LEN].to_owned());
                push_unique(&mut found, id);
            }
        }

        if bytes[index..].starts_with(CONFIG_PREFIX)
            && index.checked_sub(1).is_none_or(|before| {
                !bytes[before].is_ascii_alphanumeric() && bytes[before] != b'_'
            })
        {
            let value_start = index + CONFIG_PREFIX.len();
            let value_end = line[value_start..]
                .char_indices()
                .find(|(_, ch)| ch.is_whitespace() || matches!(ch, '\'' | '"'))
                .map_or(line.len(), |(offset, _)| value_start + offset);
            if value_end > value_start {
                let id = EvidenceId::ConfigDir(PathBuf::from(&line[value_start..value_end]));
                push_unique(&mut found, id);
            }
        }

        index += 1;
    }
    found
}

/// Removes leading environment assignments and bounds a command to 60 characters.
#[must_use]
pub fn sanitize_command_script(script: &str) -> String {
    let mut remainder = script;
    while let Some(after_assignment) = strip_assignment_prefix(remainder) {
        remainder = after_assignment.trim_start_matches(char::is_whitespace);
    }
    truncate_60(remainder)
}

/// Renders a path relative to a cwd when it lies lexically beneath that cwd.
#[must_use]
pub fn repo_relative(path: &str, cwd: &str) -> String {
    let path = path.strip_prefix("file://").unwrap_or(path);
    let cwd = cwd.strip_prefix("file://").unwrap_or(cwd);
    if cwd.is_empty() {
        return path.to_owned();
    }

    std::path::Path::new(path)
        .strip_prefix(std::path::Path::new(cwd))
        .map_or_else(
            |_| path.to_owned(),
            |relative| relative.to_string_lossy().into_owned(),
        )
}

fn uuid_at(bytes: &[u8], start: usize) -> bool {
    const HYPHENS: [usize; 4] = [8, 13, 18, 23];
    bytes[start..start + 36]
        .iter()
        .enumerate()
        .all(|(offset, byte)| {
            if HYPHENS.contains(&offset) {
                *byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(byte)
            }
        })
}

fn push_unique(found: &mut Vec<EvidenceId>, id: EvidenceId) {
    if !found.contains(&id) {
        found.push(id);
    }
}

fn strip_assignment_prefix(script: &str) -> Option<&str> {
    let mut chars = script.char_indices();
    let (_, first) = chars.next()?;
    if !is_word(first) {
        return None;
    }

    let equals = chars.find_map(|(index, ch)| {
        (ch == '=')
            .then_some(index)
            .or((!is_word(ch)).then_some(usize::MAX))
    })?;
    if equals == usize::MAX {
        return None;
    }

    let value_start = equals + 1;
    let first_value = script[value_start..].chars().next()?;
    if first_value.is_whitespace() {
        return None;
    }
    let value_end = script[value_start..]
        .char_indices()
        .find(|(_, ch)| ch.is_whitespace())
        .map_or(script.len(), |(offset, _)| value_start + offset);
    Some(&script[value_end..])
}

fn is_word(ch: char) -> bool {
    ch == '_' || ch.is_alphanumeric()
}

fn truncate_60(value: &str) -> String {
    value.chars().take(60).collect()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn raw_id_scan_finds_uuids_and_config_dirs_without_json_parse() {
        let line = concat!(
            "not-json 6f9bdfa0-1502-4a37-97aa-c45591141130 ",
            "CLAUDE_CONFIG_DIR=/home/user/.claude-secondary ",
            "6f9bdfa0-1502-4a37-97aa-c45591141130 ",
            "CLAUDE_CONFIG_DIR=/home/user/.claude-secondary ",
            "a6f9bdfa0-1502-4a37-97aa-c45591141130b ",
            "745480a2-5bdc-483f-ab53-0b4fabc01781"
        );

        assert_eq!(
            scan_raw_ids(line),
            vec![
                EvidenceId::Uuid("6f9bdfa0-1502-4a37-97aa-c45591141130".to_owned()),
                EvidenceId::ConfigDir(PathBuf::from("/home/user/.claude-secondary")),
                EvidenceId::Uuid("745480a2-5bdc-483f-ab53-0b4fabc01781".to_owned()),
            ]
        );
    }

    #[test]
    fn command_sanitizer_strips_repeated_env_prefixes_and_truncates_safely() {
        let script = concat!(
            "CLAUDE_CONFIG_DIR=/home/user/.claude-secondary ",
            "RUST_LOG=debug codex exec resume ",
            "日本語日本語日本語日本語日本語日本語日本語日本語日本語日本語日本語日本語日本語日本語日本語"
        );

        let sanitized = sanitize_command_script(script);

        assert!(sanitized.starts_with("codex exec resume "));
        assert_eq!(sanitized.chars().count(), 60);
    }
}

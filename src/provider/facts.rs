//! Provider-neutral facts extracted from append-only agent logs.

use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};

use crate::model::TokenBreakdown;

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
    /// Claude root working directory used only for the subject basename fallback.
    ClaudeCwd {
        /// Claude root session ID that owns the working directory.
        session_id: String,
        /// Raw allowlisted working directory string.
        cwd: String,
    },
    /// Identity metadata reported by a Codex rollout.
    CodexMeta {
        /// Rollout ID of the artifact being read.
        rollout_id: String,
        /// Raw Codex working directory, which may be a `file://` URI.
        cwd: String,
        /// Codex process that originated the rollout.
        originator: String,
        /// Typed Codex internal-agent source, when recognized.
        internal: Option<CodexInternal>,
        /// Codex CLI version that created the rollout.
        cli_version: String,
    },
    /// Per-turn Codex execution context.
    CodexTurn {
        /// Rollout that owns the turn.
        rollout_id: String,
        /// Provider turn ID.
        turn_id: String,
        /// Model selected for the turn.
        model: String,
        /// Reasoning effort selected for the turn, when present.
        effort: Option<String>,
        /// Sandbox policy mode selected for the turn, when present.
        sandbox: Option<String>,
    },
    /// Evidence that a Codex turn started.
    CodexTurnStarted {
        /// Rollout that owns the turn.
        rollout_id: String,
        /// Provider timestamp in Unix epoch milliseconds.
        at_ms: i64,
    },
    /// Evidence that a Codex turn completed.
    CodexTurnComplete {
        /// Rollout that owns the turn.
        rollout_id: String,
        /// Provider timestamp in Unix epoch milliseconds.
        at_ms: i64,
    },
    /// Evidence that a Codex turn was aborted.
    CodexTurnAborted {
        /// Rollout that owns the turn.
        rollout_id: String,
        /// Provider timestamp in Unix epoch milliseconds.
        at_ms: i64,
    },
    /// Process ID reported by a Codex command execution.
    CodexPid {
        /// Rollout that owns the process.
        rollout_id: String,
        /// Decimal process ID parsed from the provider's JSON string.
        pid: u32,
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
    ///
    /// Duplicate facts are allowed; synthesis merges them with `failed: true` dominant.
    SubagentEnded {
        /// Owning root Claude session ID.
        parent: String,
        /// Claude subagent ID.
        agent_id: String,
        /// Whether the reported status was not completed.
        failed: bool,
    },
    /// Sanitized short activity evidenced by a provider event.
    Activity {
        /// Session that performed the activity.
        scope: SessionScope,
        /// Provider timestamp in Unix epoch milliseconds.
        at_ms: i64,
        /// Provider event family that supplied the activity.
        source: ActivitySource,
        /// Sanitized activity text of at most 60 characters.
        line: String,
    },
    /// One provider-reported output-token usage sample.
    ///
    /// Downstream deduplication uses `(scope, sample_id)`, never `sample_id`
    /// alone, because sample IDs are unique only within one session or artifact.
    Usage {
        /// Session charged for the sample.
        scope: SessionScope,
        /// Provider timestamp in Unix epoch milliseconds.
        at_ms: i64,
        /// Scope-local sample identity used for downstream deduplication.
        sample_id: String,
        /// Output tokens only.
        output_tokens: u64,
        /// Other allowlisted numeric token families reported with this sample.
        token_breakdown: TokenBreakdown,
        /// Allowlisted provider model name, when present.
        model: Option<String>,
        /// Allowlisted provider effort setting, when present.
        effort: Option<String>,
    },
    /// Identifier produced by one position in the closed lineage-evidence grammar.
    ///
    /// `lane::Admission::on_evidence` exact-matches UUIDs against `AdmissionIndex`;
    /// synthesis discards identifiers that do not name a discovered artifact.
    EvidenceId {
        /// Session whose typed evidence position contained the identifier.
        parent: SessionScope,
        /// Extracted identifier token.
        id: EvidenceId,
    },
}

/// Typed activity provenance for turn-scoped display policy.
///
/// The Task 5 consumer prefers the latest commentary in the current turn,
/// falling back to the latest command; this extractor only records provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActivitySource {
    /// A Codex commentary message.
    Commentary,
    /// A Codex command execution.
    Command,
    /// A Claude tool-use block.
    ToolUse,
}

/// Typed source of a Codex internal-agent rollout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodexInternal {
    /// A provider-defined named internal agent.
    Named {
        /// Provider-defined internal-agent name.
        name: String,
    },
    /// An internal agent created by spawning a child thread.
    ThreadSpawn {
        /// Parent Codex thread ID.
        parent_thread_id: String,
        /// Optional provider-assigned agent nickname.
        nickname: Option<String>,
        /// Optional provider-assigned agent role.
        role: Option<String>,
    },
}

/// Narrow identifier evidence produced by the closed lineage grammar.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
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
    let mut seen = HashSet::new();
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
                push_unique(&mut found, &mut seen, id);
            }
        }

        if bytes[index..].starts_with(CONFIG_PREFIX)
            && index.checked_sub(1).is_none_or(|before| {
                !bytes[before].is_ascii_alphanumeric() && bytes[before] != b'_'
            })
        {
            let value_start = index + CONFIG_PREFIX.len();
            if let Some((value_start, value_end)) = config_dir_value_bounds(line, value_start) {
                let id = EvidenceId::ConfigDir(PathBuf::from(&line[value_start..value_end]));
                push_unique(&mut found, &mut seen, id);
            }
        }

        index += 1;
    }
    found
}

/// Removes leading environment assignments and supported `env` wrappers, then bounds a command.
#[must_use]
pub fn sanitize_command_script(script: &str) -> String {
    let mut remainder = script;
    while let Some(after_prefix) =
        strip_assignment_prefix(remainder).or_else(|| strip_env_wrapper_prefix(remainder))
    {
        remainder = after_prefix.trim_start_matches(char::is_whitespace);
    }
    if let Some(redacted) = redact_unclassified_env_wrapper_assignments(remainder) {
        return truncate_60(&redacted);
    }
    truncate_60(remainder)
}

/// Renders a path relative to a cwd when it lies lexically beneath that cwd.
#[must_use]
pub fn repo_relative(path: &str, cwd: &str) -> String {
    let path = path.strip_prefix("file://").unwrap_or(path);
    let cwd = cwd.strip_prefix("file://").unwrap_or(cwd);
    let path = Path::new(path);
    match path.strip_prefix(Path::new(cwd)) {
        Ok(relative) if relative.as_os_str().is_empty() => ".".to_owned(),
        Ok(relative)
            if !relative.is_absolute()
                && !relative
                    .components()
                    .any(|component| component == Component::ParentDir) =>
        {
            relative.to_string_lossy().into_owned()
        }
        Ok(_) | Err(_) => path.file_name().map_or_else(
            || ".".to_owned(),
            |name| name.to_string_lossy().into_owned(),
        ),
    }
}

fn config_dir_value_bounds(line: &str, value_start: usize) -> Option<(usize, usize)> {
    let bytes = line.as_bytes();
    if bytes[value_start..].starts_with(b"\\\"") {
        let quoted_start = value_start + 2;
        let quoted_end = bytes[quoted_start..]
            .windows(2)
            .position(|window| window == b"\\\"")?
            + quoted_start;
        return (quoted_end > quoted_start).then_some((quoted_start, quoted_end));
    }

    if let Some(quote @ (b'\'' | b'"')) = bytes.get(value_start).copied() {
        let quoted_start = value_start + 1;
        let quoted_end = bytes[quoted_start..]
            .iter()
            .position(|byte| *byte == quote)?
            + quoted_start;
        return (quoted_end > quoted_start).then_some((quoted_start, quoted_end));
    }

    let value_end = line[value_start..]
        .char_indices()
        .find(|(_, ch)| ch.is_whitespace() || matches!(ch, '\'' | '"'))
        .map_or(line.len(), |(offset, _)| value_start + offset);
    (value_end > value_start).then_some((value_start, value_end))
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

pub(crate) fn is_uuid_token(value: &str) -> bool {
    value.len() == 36 && uuid_at(value.as_bytes(), 0)
}

fn push_unique(found: &mut Vec<EvidenceId>, seen: &mut HashSet<EvidenceId>, id: EvidenceId) {
    if seen.insert(id.clone()) {
        found.push(id);
    }
}

fn strip_assignment_prefix(script: &str) -> Option<&str> {
    let (assignment, remainder) = leading_token(script)?;
    is_assignment_token(assignment).then_some(remainder)
}

fn strip_env_wrapper_prefix(script: &str) -> Option<&str> {
    let (command, mut remainder) = leading_token(script)?;
    if command != "env" {
        return None;
    }
    remainder = remainder.trim_start_matches(char::is_whitespace);

    loop {
        let (option_or_command, after_token) = leading_token(remainder)?;
        match option_or_command {
            "-" | "-i" | "--ignore-environment" => {
                remainder = after_token.trim_start_matches(char::is_whitespace);
            }
            "-u" | "--unset" => {
                let (_, after_name) =
                    leading_token(after_token.trim_start_matches(char::is_whitespace))?;
                remainder = after_name.trim_start_matches(char::is_whitespace);
            }
            "--" => {
                let command = after_token.trim_start_matches(char::is_whitespace);
                return (!command.is_empty()).then_some(command);
            }
            option if option.starts_with("--unset=") && option.len() > "--unset=".len() => {
                remainder = after_token.trim_start_matches(char::is_whitespace);
            }
            option if option.starts_with('-') => return None,
            _ => return Some(remainder),
        }
    }
}

fn redact_unclassified_env_wrapper_assignments(script: &str) -> Option<String> {
    let (command, remainder) = leading_token(script)?;
    if command != "env" || strip_env_wrapper_prefix(script).is_some() {
        return None;
    }

    let mut removed_assignment = false;
    let mut redacted = String::new();
    for token in remainder.split_whitespace() {
        if is_assignment_token(token) {
            removed_assignment = true;
            continue;
        }
        if !redacted.is_empty() {
            redacted.push(' ');
        }
        redacted.push_str(token);
    }
    removed_assignment.then_some(redacted)
}

fn leading_token(value: &str) -> Option<(&str, &str)> {
    if value.is_empty() {
        return None;
    }
    let end = value
        .char_indices()
        .find(|(_, ch)| ch.is_whitespace())
        .map_or(value.len(), |(index, _)| index);
    Some((&value[..end], &value[end..]))
}

fn is_assignment_token(token: &str) -> bool {
    let token = strip_matched_quote_pair(token);
    token
        .split_once('=')
        .is_some_and(|(name, _)| !name.is_empty() && name.chars().all(is_word))
}

fn strip_matched_quote_pair(token: &str) -> &str {
    let bytes = token.as_bytes();
    if bytes.len() >= 2 && matches!(bytes[0], b'\'' | b'"') && bytes[0] == bytes[bytes.len() - 1] {
        &token[1..token.len() - 1]
    } else {
        token
    }
}

fn is_word(ch: char) -> bool {
    ch == '_' || ch.is_alphanumeric()
}

pub(crate) fn truncate_60(value: &str) -> String {
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
    fn raw_id_scan_reads_matched_quoted_config_dirs_only() {
        let line = concat!(
            "CLAUDE_CONFIG_DIR=\"/home/user/.claude-double\" ",
            "CLAUDE_CONFIG_DIR='/home/user/.claude-single' ",
            "CLAUDE_CONFIG_DIR=\"/home/user/.claude-unmatched"
        );

        assert_eq!(
            scan_raw_ids(line),
            vec![
                EvidenceId::ConfigDir(PathBuf::from("/home/user/.claude-double")),
                EvidenceId::ConfigDir(PathBuf::from("/home/user/.claude-single")),
            ]
        );
    }

    #[test]
    fn raw_id_scan_reads_json_escaped_double_quoted_config_dir() {
        let line = serde_json::json!({
            "type": "user",
            "command": "CLAUDE_CONFIG_DIR=\"/home/user/.claude-double\" claude -p hi"
        })
        .to_string();

        assert_eq!(
            scan_raw_ids(&line),
            vec![EvidenceId::ConfigDir(PathBuf::from(
                "/home/user/.claude-double"
            ))]
        );
    }

    #[test]
    fn repo_relative_reduces_parent_traversals_to_basename() {
        assert_eq!(
            repo_relative("/repo/public/../../private/secret", "/repo"),
            "secret"
        );
        assert_eq!(repo_relative("/repo/../etc/passwd", "/repo"), "passwd");
    }

    #[test]
    fn repo_relative_reduces_relative_input_to_basename() {
        // Claude tools emit absolute paths, so relative input is conservatively reduced.
        assert_eq!(repo_relative("src/provider/facts.rs", "/repo"), "facts.rs");
        assert_eq!(repo_relative("/repo/a/b.rs", ""), "b.rs");
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

    #[test]
    fn command_sanitizer_strips_supported_env_wrapper_forms() {
        for script in [
            "env API_TOKEN=secret curl",
            "env -i API_TOKEN=secret curl",
            "env - API_TOKEN=secret curl",
            "env --ignore-environment API_TOKEN=secret curl",
            "env -u API_TOKEN API_TOKEN=secret curl",
            "env --unset API_TOKEN API_TOKEN=secret curl",
            "env --unset=API_TOKEN API_TOKEN=secret curl",
            "env -- API_TOKEN=secret curl",
            "OUTER=value env -i INNER=secret env -- curl",
        ] {
            assert_eq!(sanitize_command_script(script), "curl", "script: {script}");
        }
    }

    #[test]
    fn command_sanitizer_redacts_secret_behind_unknown_env_option() {
        let sanitized =
            sanitize_command_script("env -C /tmp API_TOKEN=secret curl https://example.test");

        assert!(sanitized.contains("curl"));
        assert!(
            !sanitized.contains("secret"),
            "sanitized command leaked secret: {sanitized}"
        );
        assert!(
            !sanitized.contains("API_TOKEN=secret"),
            "sanitized command leaked assignment: {sanitized}"
        );
    }

    #[test]
    fn command_sanitizer_redacts_assignments_behind_unknown_env_options() {
        let script = "env -C /tmp API_TOKEN=secret curl";

        assert_eq!(sanitize_command_script(script), "-C /tmp curl");
        assert_eq!(
            sanitize_command_script("env -i -C /tmp OUTER=secret --unset OLD INNER=hidden curl"),
            "-i -C /tmp --unset OLD curl"
        );
        assert_eq!(
            sanitize_command_script("env -C /tmp EMPTY= curl"),
            "-C /tmp curl"
        );
    }

    #[test]
    fn command_sanitizer_preserves_assignment_arguments_without_env_wrapper() {
        let script = "make CC=clang target";

        assert_eq!(sanitize_command_script(script), script);
    }

    #[test]
    fn command_sanitizer_unknown_env_fallback_over_redacts_later_assignments() {
        assert_eq!(
            sanitize_command_script("env -C /tmp make CC=clang"),
            "-C /tmp make"
        );
    }
}

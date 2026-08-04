//! Session-name resolution and path-safe session-key encoding.

use std::fmt;

use sha2::{Digest, Sha256};
use thiserror::Error;

/// The input that supplied the resolved session name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolverSource {
    /// An explicit `--session` flag.
    ExplicitFlag,
    /// A non-empty `HERDR_SESSION` environment value.
    Environment,
    /// The reserved `default` name used inside a managed pane.
    ManagedPaneDefault,
}

impl fmt::Display for ResolverSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ExplicitFlag => "flag",
            Self::Environment => "environment",
            Self::ManagedPaneDefault => "default",
        })
    }
}

/// An exact session name and its path-safe encoded representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionKey {
    original_name: String,
    sanitized: String,
    hash16: String,
    encoded: String,
}

impl SessionKey {
    /// Returns the exact original session name.
    pub fn name(&self) -> &str {
        &self.original_name
    }

    /// Returns the sanitized, at-most-48-byte key component.
    pub fn sanitized(&self) -> &str {
        &self.sanitized
    }

    /// Returns the first 16 lowercase hex digits of the original name's SHA-256.
    pub fn hash16(&self) -> &str {
        &self.hash16
    }

    /// Returns the complete `<sanitized>-<hash16>` key.
    pub fn encoded(&self) -> &str {
        &self.encoded
    }
}

/// A session key together with the input source that resolved its name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSession {
    session_key: SessionKey,
    source: ResolverSource,
}

impl ResolvedSession {
    /// Returns the resolved and encoded session key.
    pub fn session_key(&self) -> &SessionKey {
        &self.session_key
    }

    /// Returns the input source that supplied the session name.
    pub fn source(&self) -> ResolverSource {
        self.source
    }
}

/// Errors from session-name resolution and encoding.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum SessionKeyError {
    /// No flag, non-empty environment value, or managed-pane default was available.
    #[error("session name could not be resolved")]
    UnresolvableSession,
    /// Empty names are not valid session names.
    #[error("session name cannot be empty")]
    EmptyName,
    /// Dot-only names could otherwise alias special filesystem path components.
    #[error("session name cannot consist only of dots")]
    DotOnlyName,
}

/// Resolves and encodes a session name using flag, environment, then pane precedence.
pub fn resolve(
    session_flag: Option<&str>,
    herdr_session: Option<&str>,
    inside_managed_pane: bool,
) -> Result<ResolvedSession, SessionKeyError> {
    let (name, source) = if let Some(name) = session_flag {
        (name, ResolverSource::ExplicitFlag)
    } else if let Some(name) = herdr_session.filter(|name| !name.is_empty()) {
        (name, ResolverSource::Environment)
    } else if inside_managed_pane {
        ("default", ResolverSource::ManagedPaneDefault)
    } else {
        return Err(SessionKeyError::UnresolvableSession);
    };

    Ok(ResolvedSession {
        session_key: encode(name)?,
        source,
    })
}

/// Encodes an exact session name as a path-safe key.
pub fn encode(name: &str) -> Result<SessionKey, SessionKeyError> {
    if name.is_empty() {
        return Err(SessionKeyError::EmptyName);
    }
    if name.as_bytes().iter().all(|byte| *byte == b'.') {
        return Err(SessionKeyError::DotOnlyName);
    }

    let mut sanitized = String::with_capacity(name.len().min(48));
    for (index, &byte) in name.as_bytes().iter().take(48).enumerate() {
        let byte = byte.to_ascii_lowercase();
        let byte = if (index == 0 && byte == b'.')
            || !matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-')
        {
            b'_'
        } else {
            byte
        };
        sanitized.push(char::from(byte));
    }

    let digest = Sha256::digest(name.as_bytes());
    let mut hash16 = String::with_capacity(16);
    const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
    for &byte in digest.iter().take(8) {
        hash16.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        hash16.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }

    let encoded = format!("{sanitized}-{hash16}");
    Ok(SessionKey {
        original_name: name.to_owned(),
        sanitized,
        hash16,
        encoded,
    })
}

#[cfg(test)]
mod tests {
    use super::{ResolverSource, SessionKeyError, encode, resolve};

    #[test]
    fn flag_beats_env() {
        let resolved = resolve(Some("Flag Name"), Some("environment"), true).unwrap();

        assert_eq!(resolved.session_key().name(), "Flag Name");
        assert_eq!(resolved.source(), ResolverSource::ExplicitFlag);
        assert!(matches!(
            resolve(Some(""), Some("environment"), true),
            Err(SessionKeyError::EmptyName)
        ));
    }

    #[test]
    fn empty_env_is_unset() {
        let inside = resolve(None, Some(""), true).unwrap();

        assert_eq!(inside.session_key().name(), "default");
        assert_eq!(inside.source(), ResolverSource::ManagedPaneDefault);
        assert!(matches!(
            resolve(None, Some(""), false),
            Err(SessionKeyError::UnresolvableSession)
        ));
    }

    #[test]
    fn default_only_inside_pane() {
        let resolved = resolve(None, None, true).unwrap();

        assert_eq!(resolved.session_key().name(), "default");
        assert_eq!(resolved.source(), ResolverSource::ManagedPaneDefault);
    }

    #[test]
    fn outside_unresolvable() {
        assert!(matches!(
            resolve(None, None, false),
            Err(SessionKeyError::UnresolvableSession)
        ));
    }

    #[test]
    fn encoding_pipeline_bytes_exact() {
        let key = encode("My Café/42").unwrap();

        assert_eq!(key.name(), "My Café/42");
        assert_eq!(key.encoded(), "my_caf___42-b521a40918aef3ff");
    }

    #[test]
    fn leading_dot_and_hostile_bytes() {
        let key = encode(".A/\\é\n").unwrap();

        assert_eq!(key.sanitized(), "_a_____");
    }

    #[test]
    fn foo_case_divergence() {
        let lower = encode("foo").unwrap();
        let upper = encode("Foo").unwrap();

        assert_eq!(lower.sanitized(), "foo");
        assert_eq!(upper.sanitized(), "foo");
        assert_eq!(lower.hash16(), "2c26b46b68ffc68f");
        assert_eq!(upper.hash16(), "1cbec737f863e492");
        assert_ne!(lower.hash16(), upper.hash16());
        assert_ne!(lower.encoded(), upper.encoded());
    }

    #[test]
    fn dot_only_rejected() {
        for name in [".", "..", "..."] {
            assert!(matches!(encode(name), Err(SessionKeyError::DotOnlyName)));
        }
        assert!(matches!(encode(""), Err(SessionKeyError::EmptyName)));
    }

    #[test]
    fn truncation_at_48_bytes_keeps_hash() {
        let name = "A".repeat(60);
        let key = encode(&name).unwrap();

        assert_eq!(
            key.sanitized(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(key.hash16(), "c5fb235befd875b9");
        assert_eq!(
            key.encoded(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-c5fb235befd875b9"
        );
    }
}

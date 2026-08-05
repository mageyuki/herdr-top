//! T4C identifiers: `Provider`, `RunId`, `RunKey`, and `DisplayOrdinal`.

use std::fmt;
use std::str::FromStr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum Provider {
    Claude,
    Codex,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RunId(ulid::Ulid);

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

impl RunId {
    #[must_use]
    pub fn new() -> Self {
        Self(ulid::Ulid::new())
    }

    pub fn parse(value: &str) -> Result<Self, ulid::DecodeError> {
        value.parse()
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for RunId {
    type Err = ulid::DecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        ulid::Ulid::from_string(value).map(Self)
    }
}

impl Serialize for RunId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for RunId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(D::Error::custom)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum RunKey {
    Controller(String),
    Native {
        provider: Provider,
        sid: String,
    },
    NativePath {
        provider: Provider,
        path: String,
    },
    Provisional {
        terminal_id: String,
        start_ms: i64,
        seq: u64,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct DisplayOrdinal(i64);

impl DisplayOrdinal {
    #[must_use]
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn run_id_display_parse_roundtrip() {
        let run_id = RunId::new();
        let encoded = run_id.to_string();

        assert_eq!(encoded.len(), 26);
        assert_eq!(encoded.parse::<RunId>().unwrap(), run_id);
    }

    #[test]
    fn run_id_serde_roundtrip() {
        let run_id = RunId::new();
        let encoded = serde_json::to_string(&run_id).unwrap();

        assert_eq!(serde_json::from_str::<RunId>(&encoded).unwrap(), run_id);
    }

    #[test]
    fn run_key_supports_hash_lookup() {
        let key = RunKey::Native {
            provider: Provider::Codex,
            sid: "session-1".to_owned(),
        };
        let mut values = HashMap::new();
        values.insert(key.clone(), 7);

        assert_eq!(values.get(&key), Some(&7));
        assert_eq!(
            serde_json::from_str::<RunKey>(&serde_json::to_string(&key).unwrap()).unwrap(),
            key
        );
    }
}

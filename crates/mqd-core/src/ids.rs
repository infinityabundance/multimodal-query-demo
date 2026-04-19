use serde::{Deserialize, Serialize};
use std::fmt;

use crate::errors::{CoreError, Result};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EntityPath(String);

impl EntityPath {
    pub fn new(path: impl Into<String>) -> Result<Self> {
        let s: String = path.into();
        if !s.starts_with('/') {
            return Err(CoreError::InvalidEntityPath {
                path: s,
                reason: "must start with '/'",
            });
        }
        if s.len() == 1 {
            return Err(CoreError::InvalidEntityPath {
                path: s,
                reason: "must contain at least one path segment",
            });
        }
        if s.contains("//") {
            return Err(CoreError::InvalidEntityPath {
                path: s,
                reason: "empty segments are not allowed",
            });
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EntityPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timeline(String);

impl Timeline {
    pub const WALL: &'static str = "wall_time";
    pub const SIM: &'static str = "sim_time";

    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn wall() -> Self {
        Self(Self::WALL.to_string())
    }

    pub fn sim() -> Self {
        Self(Self::SIM.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Timeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ComponentName(String);

impl ComponentName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ComponentName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_path_requires_leading_slash() {
        assert!(EntityPath::new("robot/base").is_err());
        assert!(EntityPath::new("/").is_err());
        assert!(EntityPath::new("/robot//base").is_err());
        assert!(EntityPath::new("/robot/base").is_ok());
    }

    #[test]
    fn entity_path_sorts_lexicographically() {
        let mut paths = [
            EntityPath::new("/robot/10").unwrap(),
            EntityPath::new("/robot/2").unwrap(),
            EntityPath::new("/agent/1").unwrap(),
        ];
        paths.sort();
        assert_eq!(paths[0].as_str(), "/agent/1");
        assert_eq!(paths[1].as_str(), "/robot/10");
        assert_eq!(paths[2].as_str(), "/robot/2");
    }
}

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::ids::{ComponentName, EntityPath, Timeline};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ComponentKind {
    Transform3D,
    Points3D,
    Scalar,
    ImageRef,
    Annotation,
}

impl ComponentKind {
    pub const ALL: [Self; 5] = [
        Self::Transform3D,
        Self::Points3D,
        Self::Scalar,
        Self::ImageRef,
        Self::Annotation,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Transform3D => "Transform3D",
            Self::Points3D => "Points3D",
            Self::Scalar => "Scalar",
            Self::ImageRef => "ImageRef",
            Self::Annotation => "Annotation",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "Transform3D" => Some(Self::Transform3D),
            "Points3D" => Some(Self::Points3D),
            "Scalar" => Some(Self::Scalar),
            "ImageRef" => Some(Self::ImageRef),
            "Annotation" => Some(Self::Annotation),
            _ => None,
        }
    }
}

impl fmt::Display for ComponentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Payload {
    Transform3D {
        tx: f64,
        ty: f64,
        tz: f64,
        qx: f64,
        qy: f64,
        qz: f64,
        qw: f64,
    },
    Points3D {
        points: Vec<[f64; 3]>,
    },
    Scalar {
        value: f64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        unit: Option<String>,
    },
    ImageRef {
        uri: String,
        width: i32,
        height: i32,
        format: String,
    },
    Annotation {
        label: String,
    },
}

impl Payload {
    pub fn kind(&self) -> ComponentKind {
        match self {
            Self::Transform3D { .. } => ComponentKind::Transform3D,
            Self::Points3D { .. } => ComponentKind::Points3D,
            Self::Scalar { .. } => ComponentKind::Scalar,
            Self::ImageRef { .. } => ComponentKind::ImageRef,
            Self::Annotation { .. } => ComponentKind::Annotation,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub entity_path: EntityPath,
    pub timeline: Timeline,
    pub t_ns: i64,
    pub component: ComponentName,
    pub payload: Payload,
}

impl Event {
    pub fn kind(&self) -> ComponentKind {
        self.payload.kind()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn component_kind_roundtrip() {
        for kind in ComponentKind::ALL {
            assert_eq!(ComponentKind::parse(kind.as_str()), Some(kind));
        }
        assert_eq!(ComponentKind::parse("Nope"), None);
    }

    #[test]
    fn event_json_roundtrip() {
        let event = Event {
            entity_path: EntityPath::new("/robot/base").unwrap(),
            timeline: Timeline::wall(),
            t_ns: 1_000_000_000,
            component: ComponentName::new("pose"),
            payload: Payload::Transform3D {
                tx: 1.0,
                ty: 2.0,
                tz: 3.0,
                qx: 0.0,
                qy: 0.0,
                qz: 0.0,
                qw: 1.0,
            },
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(event, back);
    }
}

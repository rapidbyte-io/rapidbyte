use serde::{Deserialize, Serialize};

use super::CursorValue;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointKind {
    Source,
    Dest,
    Transform,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Checkpoint {
    pub id: u64,
    pub kind: CheckpointKind,
    pub stream: String,
    pub cursor_field: Option<String>,
    pub cursor_value: Option<CursorValue>,
    pub records_processed: u64,
    pub bytes_processed: u64,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StateScope {
    Pipeline = 0,
    Stream = 1,
    ConnectorInstance = 2,
}

impl TryFrom<i32> for StateScope {
    type Error = i32;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Pipeline),
            1 => Ok(Self::Stream),
            2 => Ok(Self::ConnectorInstance),
            _ => Err(value),
        }
    }
}

impl StateScope {
    pub fn to_i32(self) -> i32 {
        self as i32
    }
}

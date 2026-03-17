//! ID generation interface.

use crate::domain::run::RunId;

pub trait IdGenerator: Send + Sync {
    fn new_run_id(&self) -> RunId;
}

//! UUID-backed ID generator.

#[derive(Debug, Default)]
pub struct UuidIdGenerator;

impl crate::ports::id_generator::IdGenerator for UuidIdGenerator {
    fn new_run_id(&self) -> crate::domain::run::RunId {
        crate::domain::run::RunId::new(uuid::Uuid::new_v4().to_string())
    }
}

//! In-memory event bus adapter.

#[derive(Debug, Default)]
pub struct InMemoryEventBus;

impl crate::ports::event_bus::EventBus for InMemoryEventBus {}

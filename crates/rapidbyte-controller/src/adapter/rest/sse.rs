use std::convert::Infallible;

use axum::response::sse::{Event, KeepAlive, Sse};
use futures::Stream;
use futures::StreamExt;
use serde::Serialize;

use crate::traits::EventStream;

/// Convert an application-layer `EventStream` into an axum SSE response.
///
/// `event_name` maps each item to an SSE event type (e.g., "progress", "complete").
/// The item is serialized as JSON in the `data` field.
pub fn to_sse_response<T, F>(
    stream: EventStream<T>,
    event_name: F,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>>
where
    T: Serialize + Send + 'static,
    F: Fn(&T) -> &'static str + Send + 'static,
{
    let sse_stream = stream.map(move |item| {
        let name = event_name(&item);
        let data = serde_json::to_string(&item).unwrap_or_default();
        Ok(Event::default().event(name).data(data))
    });
    Sse::new(sse_stream).keep_alive(KeepAlive::default())
}

use axum::response::sse::{Event, Sse};
use futures::Stream;
use rapidbyte_api::SseEvent;
use std::convert::Infallible;
use tokio::sync::broadcast;

/// Bridges a `broadcast::Receiver<SseEvent>` into an axum `Sse` response
/// stream. Terminal events (complete, failed, cancelled) close the stream.
/// Lagged subscribers log a warning and continue.
#[allow(dead_code)]
pub fn sse_from_broadcast(
    mut rx: broadcast::Receiver<SseEvent>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let sse_event = Event::default()
                        .event(event.event_type())
                        .json_data(&event)
                        .unwrap_or_else(|_| {
                            Event::default()
                                .event("error")
                                .data("serialization failed")
                        });
                    let is_terminal = event.is_terminal();
                    yield Ok(sse_event);
                    if is_terminal {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "SSE subscriber lagged");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Sse::new(stream)
}

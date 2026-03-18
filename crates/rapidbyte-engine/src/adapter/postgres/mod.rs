//! `PostgreSQL`-backed adapter implementing repository ports and `StateBackend`.
//!
//! [`PgBackend`] combines an async [`sqlx::PgPool`] for the hexagonal
//! repository traits and a sync [`ClientPool`] for the legacy
//! [`StateBackend`](rapidbyte_types::state_backend::StateBackend) trait.
//!
//! | Sub-module        | Trait                  |
//! |-------------------|------------------------|
//! | `cursor`          | `CursorRepository`     |
//! | `run_record`      | `RunRecordRepository`  |
//! | `dlq`             | `DlqRepository`        |
//! | `state_backend`   | `StateBackend`         |

mod cursor;
mod dlq;
mod run_record;
mod state_backend;

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Condvar, Mutex};

use postgres::{Client, NoTls};
use sqlx::PgPool;

use rapidbyte_types::state_backend::StateBackend;
use rapidbyte_types::state_error::StateError;

/// `PostgreSQL`-backed adapter combining async (sqlx) and sync (postgres)
/// connection pools.
///
/// Use [`PgBackend::connect`] to create an instance, then call
/// [`migrate`](PgBackend::migrate) to apply pending DDL migrations.
pub struct PgBackend {
    /// Async pool used by repository port implementations.
    pool: PgPool,
    /// Sync pool used by the legacy `StateBackend` trait.
    sync_pool: ClientPool,
}

impl PgBackend {
    /// Connect to a `PostgreSQL` database and set up both connection pools.
    ///
    /// # Errors
    ///
    /// Returns an error if either pool fails to connect.
    pub async fn connect(connstr: &str) -> Result<Self, anyhow::Error> {
        let pool = PgPool::connect(connstr).await?;
        let sync_pool = ClientPool::new(connstr, 4)?;
        Ok(Self { pool, sync_pool })
    }

    /// Run pending SQL migrations against the database.
    ///
    /// # Errors
    ///
    /// Returns an error if any migration fails.
    pub async fn migrate(&self) -> Result<(), anyhow::Error> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    /// Obtain an `Arc<dyn StateBackend>` view of this backend.
    #[must_use]
    pub fn as_state_backend(self: &Arc<Self>) -> Arc<dyn StateBackend> {
        Arc::clone(self) as Arc<dyn StateBackend>
    }
}

// ---------------------------------------------------------------------------
// Sync connection pool (ported from rapidbyte-state)
// ---------------------------------------------------------------------------

/// Small blocking connection pool backed by `postgres::Client`.
///
/// Uses a `Mutex<Vec<Client>>` with a `Condvar` for thread-safe checkout.
pub(crate) struct ClientPool {
    clients: Mutex<Vec<Client>>,
    available: Condvar,
}

/// RAII guard that returns a `Client` to the pool on drop.
pub(crate) struct PooledClient<'a> {
    pool: &'a ClientPool,
    client: Option<Client>,
}

impl ClientPool {
    /// Create a new pool with `size` connections to `connstr`.
    fn new(connstr: &str, size: usize) -> Result<Self, StateError> {
        let mut clients = Vec::with_capacity(size);
        for _ in 0..size {
            clients.push(Client::connect(connstr, NoTls).map_err(StateError::backend)?);
        }
        Ok(Self {
            clients: Mutex::new(clients),
            available: Condvar::new(),
        })
    }

    /// Check out a connection, blocking until one is available.
    fn checkout(&self) -> Result<PooledClient<'_>, StateError> {
        let mut clients = self.clients.lock().map_err(|_| StateError::LockPoisoned)?;
        loop {
            if let Some(client) = clients.pop() {
                return Ok(PooledClient {
                    pool: self,
                    client: Some(client),
                });
            }
            clients = self
                .available
                .wait(clients)
                .map_err(|_| StateError::LockPoisoned)?;
        }
    }
}

impl Deref for PooledClient<'_> {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref().expect("pooled client missing")
    }
}

impl DerefMut for PooledClient<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().expect("pooled client missing")
    }
}

impl Drop for PooledClient<'_> {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            if let Ok(mut clients) = self.pool.clients.lock() {
                clients.push(client);
                self.pool.available.notify_one();
            }
        }
    }
}

//! Agent worker for Rapidbyte — polls the controller, executes pipelines, reports results.
//!
//! # Crate structure
//!
//! | Module        | Responsibility                              |
//! |---------------|---------------------------------------------|
//! | `domain`      | Port traits, domain types, errors            |
//! | `application` | DI context, use-case orchestration, fakes    |
//! | `adapter`     | gRPC client, engine executor, metrics, clock |

#![warn(clippy::pedantic)]

pub mod adapter;
pub mod application;
pub mod domain;

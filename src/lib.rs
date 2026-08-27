#![deny(unsafe_code)]

//! Library components for the `herdr-top` session monitor.

pub mod activity;
pub mod diagnostics;
pub mod doctor;
pub mod herdr;
pub mod hook_adapter;
pub mod identity;
pub mod lockfile;
pub mod model;
pub mod operator;
pub mod performance;
pub mod provider;
pub mod reducer;
pub mod rendezvous;
pub mod session_key;
pub(crate) mod status;
pub mod store;
pub mod tui;

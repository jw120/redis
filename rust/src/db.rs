//! Database state

// Design borrowed from mini-redis example from tokio documentation
// `State` holds that key value store
// `Shared` holds guards stare with a mutex
// `Database` holds a handle (Arc) to the shared state

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

/// Underlying state holding the key-value store
#[derive(Debug)]
pub struct State {
    entries: HashMap<Bytes, Bytes>,
}

impl State {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

/// Guarding the underlying state with a mutex
#[derive(Debug)]
pub struct Shared {
    state: Mutex<State>,
}

impl Shared {
    fn new(state: State) -> Self {
        Self {
            state: Mutex::new(state),
        }
    }
}

/// Handle to our database
#[derive(Debug)]
pub struct Database {
    shared: Arc<Shared>,
}

impl Database {
    /// Set up a new empty database
    pub fn new() -> Self {
        let state = State::new();
        let shared = Arc::new(Shared::new(state));
        Self { shared }
    }

    /// Create a new handle to the database
    pub fn new_handle(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }

    /// Get a value from the database
    pub fn get(&self, key: Bytes) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();
        state.entries.get(&key).cloned()
    }

    /// Set a value into the database
    pub fn set(&self, key: Bytes, value: Bytes) {
        let mut state = self.shared.state.lock().unwrap();
        state.entries.insert(key, value);
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

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
struct Store {
    entries: HashMap<Bytes, Bytes>,
}

impl Store {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

/// Handle to our database
#[derive(Debug)]
pub struct Database {
    shared: Arc<Mutex<Store>>,
}

impl Database {
    /// Set up a new empty database
    pub fn initialise() -> Self {
        Self { shared: Arc::new(Mutex::new(Store::new())) }
    }

    /// Create a new handle to the database
    pub fn handle(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }

    /// Get a value from the database
    pub fn get(&self, key: Bytes) -> Option<Bytes> {
        let state = self.shared.lock().unwrap();
        state.entries.get(&key).cloned()
    }

    /// Set a value into the database
    pub fn set(&self, key: Bytes, value: Bytes) {
        let mut state = self.shared.lock().unwrap();
        state.entries.insert(key, value);
    }
}

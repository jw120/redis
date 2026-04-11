//! Database state

// Design borrowed from mini-redis example from tokio documentation
// `State` holds that key value store
// `Shared` holds guards stare with a mutex
// `Database` holds a handle (Arc) to the shared state

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use tracing::debug;

/// Objects that we keep in out store
#[derive(Clone, Debug)]
enum Object {
    String(Bytes),
    List(VecDeque<Bytes>),
}

/// Underlying state holding the key-value store
#[derive(Debug)]
struct Store {
    entries: HashMap<Bytes, Record>,
}

impl Store {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

/// Record we store in our key-value store
#[derive(Debug)]
struct Record {
    object: Object,
    expiry: Option<SystemTime>,
}

/// Handle to our database
#[derive(Debug)]
pub struct Database {
    shared: Arc<Mutex<Store>>,
}

/// Should push or pop operations apply to left or right of a list
#[derive(Clone, Copy, Debug)]
pub enum Direction {
    /// Apply to left-most element of the list
    Left,
    /// Apply to right-most element of the list
    Right,
}

impl Database {
    /// Set up a new empty database
    pub fn initialise() -> Self {
        Self {
            shared: Arc::new(Mutex::new(Store::new())),
        }
    }

    /// Create a new handle to the database
    pub fn handle(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }

    /// Get an object from the database
    fn get_object(&self, key: &Bytes) -> Option<Object> {
        let state = self.shared.lock().unwrap();
        match state.entries.get(key) {
            None => None,
            Some(Record {
                object,
                expiry: None,
            }) => Some(object.clone()),
            Some(Record {
                object,
                expiry: Some(t),
            }) => {
                if SystemTime::now() < *t {
                    Some(object.clone())
                } else {
                    None
                }
            }
        }
    }

    /// Insert an object into the database
    fn insert_object(&self, key: &Bytes, object: &Object, expiry: Option<SystemTime>) {
        let mut state = self.shared.lock().unwrap();
        state.entries.insert(
            key.clone(),
            Record {
                object: object.clone(),
                expiry,
            },
        );
    }

    // fn modify_list1<F>(&self, key: Bytes, f: F, default: Record)
    //      where F:  FnOnce(&mut Record) {
    //     let mut state = self.shared.lock().unwrap();
    //     state.entries.entry(key).and_modify(f).or_insert(default);
    // }

    /// Modifies a list entry if it exists
    fn modify_list<F>(&self, key: &Bytes, f: F)
    where
        F: FnOnce(&mut VecDeque<Bytes>),
    {
        let mut state = self.shared.lock().unwrap();
        state
            .entries
            .entry(key.clone())
            .and_modify(|Record { object, expiry: _ }| {
                if let Object::List(v) = object {
                    f(v)
                }
            });
    }

    /// Return length of a list (zero if a missing value)
    pub fn length(&self, key: &Bytes) -> Result<usize> {
        match self.get_object(key) {
            Some(Object::List(v)) => Ok(v.len()),
            Some(_) => Err(anyhow!("WRONGTYPE Expected a list")),
            None => Ok(0)
        }
    }

    /// GET command: Get a string value from the database
    pub fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        match self.get_object(key) {
            None => Ok(None),
            Some(Object::String(s)) => Ok(Some(s)),
            Some(_) => Err(anyhow!(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )),
        }
    }

    /// SET command: Set a string value in the database
    pub fn set(&self, key: &Bytes, value: &Bytes, expiry: Option<SystemTime>) {
        self.insert_object(key, &Object::String(value.clone()), expiry);
    }

    /// DEL command: Delete a key if present {
    pub fn del(&self, key: &Bytes) -> Result<()> {
        let mut state = self.shared.lock().unwrap();
        state.entries.remove(key);
        Ok(())
    }

    /// RPUSH command: append a string value to a list in the database (creating it if not existing)
    /// Returns the new length of the list
    pub fn push(&self, direction: Direction, key: &Bytes, values: &[Bytes]) -> Result<usize> {
        if values.is_empty() {
            bail!("ERR wrong number of arguments for push command");
        }

        for value in values {
            match self.get_object(key) {
                None => {
                    self.insert_object(key, &Object::List(VecDeque::from([value.clone()])), None);
                }
                Some(Object::List(_)) => match direction {
                    Direction::Left => {
                        self.modify_list(key, |w: &mut VecDeque<Bytes>| w.push_front(value.clone()))
                    }
                    Direction::Right => {
                        self.modify_list(key, |w: &mut VecDeque<Bytes>| w.push_back(value.clone()))
                    }
                },
                Some(_) => {
                    bail!("WRONGTYPE Operation against a key holding the wrong kind of value")
                }
            };
        }
        debug!("object is {:?}", self.get_object(key));
        self.length(key)
    }

    /// LPOP command: remove and return given number of elements from the left
    pub fn pop(&self, key: &Bytes, n: usize) -> Result<Vec<Bytes>> {
        let mut state = self.shared.lock().unwrap();
        let object = match state.entries.get_mut(key) {
            None => return Ok(Vec::new()),
            Some(Record {
                object,
                expiry: None,
            }) => object,
            Some(Record {
                object,
                expiry: Some(t),
            }) => {
                if SystemTime::now() < *t {
                    object
                } else {
                    return Ok(Vec::new())
                }
            }
        };
        let Object::List(v) = object else {
            bail!("WRONGTYPE Operation against a key holding the wrong kind of value");
        };
            
        let mut output: Vec<Bytes> = Vec::new();
        while output.len() < n && let Some(element) = v.pop_front() {
            output.push(element)
        }
        Ok(output)

    }
    
    /// LRANGE command: return array of elements start..=stop
    pub fn lrange(&self, key: &Bytes, start: i64, stop: i64) -> Result<Vec<Bytes>> {
        let Some(object) = self.get_object(key) else {
            return Ok(Vec::new());
        };
        let Object::List(v) = object else {
            bail!("WRONGTYPE Operation against a key holding the wrong kind of value");
        };

        let start: usize = if start < 0 {
            v.len().saturating_sub(-start as usize)
        } else {
            start as usize
        };
        let stop: usize = if stop < 0 {
            v.len().saturating_sub(-stop as usize)
        } else {
            stop as usize
        };
                    
        let mut output: Vec<Bytes> = Vec::new();
        for (i, s) in v.iter().enumerate() {
            if i >= start && i <= stop {
                output.push(s.clone());
            }
        }
        Ok(output)
    }
}

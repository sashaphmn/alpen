//! In-memory task store. Default for tests and dev.

use std::{collections::HashMap, sync::RwLock, time::SystemTime};

use super::traits::{TaskRecord, TaskStore};
use crate::{
    error::{ProverError, ProverResult},
    task::TaskStatus,
};

#[derive(Debug, Default)]
pub struct InMemoryTaskStore {
    records: RwLock<HashMap<String, TaskRecord>>,
}

impl InMemoryTaskStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl TaskStore for InMemoryTaskStore {
    fn get(&self, uuid: &str) -> Option<TaskRecord> {
        self.records.read().expect("lock").get(uuid).cloned()
    }

    fn insert(&self, record: TaskRecord) -> ProverResult<()> {
        let mut map = self.records.write().expect("lock");
        if map.contains_key(record.uuid()) {
            return Err(ProverError::TaskAlreadyExists(record.uuid().to_string()));
        }
        map.insert(record.uuid().to_string(), record);
        Ok(())
    }

    fn update_status(&self, uuid: &str, status: TaskStatus) -> ProverResult<()> {
        self.records
            .write()
            .expect("lock")
            .get_mut(uuid)
            .ok_or_else(|| ProverError::TaskNotFound(uuid.to_string()))?
            .update_status(status);
        Ok(())
    }

    fn set_retry_after(&self, uuid: &str, when: SystemTime) -> ProverResult<()> {
        self.records
            .write()
            .expect("lock")
            .get_mut(uuid)
            .ok_or_else(|| ProverError::TaskNotFound(uuid.to_string()))?
            .set_retry_after(Some(when));
        Ok(())
    }

    fn set_metadata(&self, uuid: &str, data: Vec<u8>) -> ProverResult<()> {
        self.records
            .write()
            .expect("lock")
            .get_mut(uuid)
            .ok_or_else(|| ProverError::TaskNotFound(uuid.to_string()))?
            .set_metadata(Some(data));
        Ok(())
    }

    fn list_retriable(&self, now: SystemTime) -> Vec<TaskRecord> {
        self.records
            .read()
            .expect("lock")
            .values()
            .filter(|r| r.status().is_retriable() && r.retry_after().is_some_and(|t| t <= now))
            .cloned()
            .collect()
    }

    fn list_in_progress(&self) -> Vec<TaskRecord> {
        self.records
            .read()
            .expect("lock")
            .values()
            .filter(|r| r.status().is_in_progress())
            .cloned()
            .collect()
    }

    fn count(&self) -> usize {
        self.records.read().expect("lock").len()
    }
}

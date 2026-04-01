//! Sled-backed persistent task store.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use borsh::{BorshDeserialize, BorshSerialize};

use crate::{
    error::{ProverError, ProverResult},
    task::TaskStatus,
};

use super::traits::{TaskRecord, TaskStore};

/// Serializable form of [`TaskRecord`] for sled storage.
#[derive(BorshSerialize, BorshDeserialize)]
struct StoredRecord {
    status: TaskStatus,
    /// Seconds since UNIX epoch.
    updated_at_secs: u64,
    /// Seconds since UNIX epoch, if set.
    retry_after_secs: Option<u64>,
    /// Opaque strategy metadata (e.g. remote ProofId).
    metadata: Option<Vec<u8>>,
}

impl StoredRecord {
    fn from_record(record: &TaskRecord) -> Self {
        Self {
            status: record.status().clone(),
            updated_at_secs: system_time_to_secs(SystemTime::now()),
            retry_after_secs: record.retry_after().map(system_time_to_secs),
            metadata: record.metadata().map(|m| m.to_vec()),
        }
    }

    fn to_record(self, uuid: String) -> TaskRecord {
        let mut r = TaskRecord::new(uuid, self.status);
        if let Some(secs) = self.retry_after_secs {
            r.set_retry_after(Some(secs_to_system_time(secs)));
        }
        if let Some(data) = self.metadata {
            r.set_metadata(Some(data));
        }
        r
    }
}

fn system_time_to_secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn secs_to_system_time(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

/// Persistent [`TaskStore`] backed by sled.
#[derive(Debug)]
pub struct SledTaskStore {
    tree: sled::Tree,
}

impl SledTaskStore {
    /// Open a task store using the given sled tree.
    pub fn new(tree: sled::Tree) -> Self {
        Self { tree }
    }

    /// Open a task store from a sled database, using "prover_tasks" as the tree name.
    pub fn open(db: &sled::Db) -> ProverResult<Self> {
        let tree = db
            .open_tree("prover_tasks")
            .map_err(|e| ProverError::Internal(e.into()))?;
        Ok(Self::new(tree))
    }

    fn get_stored(&self, uuid: &str) -> ProverResult<Option<StoredRecord>> {
        match self.tree.get(uuid.as_bytes()) {
            Ok(Some(bytes)) => {
                let record = borsh::from_slice(&bytes)
                    .map_err(|e| ProverError::Internal(anyhow::anyhow!("deserialize: {e}")))?;
                Ok(Some(record))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(ProverError::Internal(e.into())),
        }
    }

    fn put_stored(&self, uuid: &str, record: &StoredRecord) -> ProverResult<()> {
        let bytes = borsh::to_vec(record)
            .map_err(|e| ProverError::Internal(anyhow::anyhow!("serialize: {e}")))?;
        self.tree
            .insert(uuid.as_bytes(), bytes)
            .map_err(|e| ProverError::Internal(e.into()))?;
        Ok(())
    }

    fn modify<F>(&self, uuid: &str, f: F) -> ProverResult<()>
    where
        F: FnOnce(&mut StoredRecord),
    {
        let mut record = self
            .get_stored(uuid)?
            .ok_or_else(|| ProverError::TaskNotFound(uuid.to_string()))?;
        f(&mut record);
        record.updated_at_secs = system_time_to_secs(SystemTime::now());
        self.put_stored(uuid, &record)
    }
}

impl TaskStore for SledTaskStore {
    fn get(&self, uuid: &str) -> Option<TaskRecord> {
        self.get_stored(uuid)
            .ok()
            .flatten()
            .map(|r| r.to_record(uuid.to_string()))
    }

    fn insert(&self, record: TaskRecord) -> ProverResult<()> {
        if self.tree.contains_key(record.uuid().as_bytes()).unwrap_or(false) {
            return Err(ProverError::TaskAlreadyExists(record.uuid().to_string()));
        }
        let stored = StoredRecord::from_record(&record);
        self.put_stored(record.uuid(), &stored)
    }

    fn update_status(&self, uuid: &str, status: TaskStatus) -> ProverResult<()> {
        self.modify(uuid, |r| r.status = status)
    }

    fn set_retry_after(&self, uuid: &str, when: SystemTime) -> ProverResult<()> {
        self.modify(uuid, |r| {
            r.retry_after_secs = Some(system_time_to_secs(when));
        })
    }

    fn set_metadata(&self, uuid: &str, data: Vec<u8>) -> ProverResult<()> {
        self.modify(uuid, |r| r.metadata = Some(data))
    }

    fn list_retriable(&self, now: SystemTime) -> Vec<TaskRecord> {
        let now_secs = system_time_to_secs(now);
        self.tree
            .iter()
            .filter_map(|item| {
                let (key, val) = item.ok()?;
                let uuid = String::from_utf8(key.to_vec()).ok()?;
                let record: StoredRecord = borsh::from_slice(&val).ok()?;
                if record.status.is_retriable()
                    && record.retry_after_secs.is_some_and(|t| t <= now_secs)
                {
                    Some(record.to_record(uuid))
                } else {
                    None
                }
            })
            .collect()
    }

    fn list_in_progress(&self) -> Vec<TaskRecord> {
        self.tree
            .iter()
            .filter_map(|item| {
                let (key, val) = item.ok()?;
                let uuid = String::from_utf8(key.to_vec()).ok()?;
                let record: StoredRecord = borsh::from_slice(&val).ok()?;
                if record.status.is_in_progress() {
                    Some(record.to_record(uuid))
                } else {
                    None
                }
            })
            .collect()
    }

    fn count(&self) -> usize {
        self.tree.len()
    }
}

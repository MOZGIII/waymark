use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_backends_core::{BackendError, BackendResult};
use waymark_core_backend::{
    ActionDone, CoreBackend, GraphUpdate, InstanceDone, InstanceLockStatus, LockClaim,
    QueuedInstance, QueuedInstanceBatch,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RecordedLockClaim {
    pub lock_uuid: Uuid,
    pub lock_expires_at: DateTime<Utc>,
}

impl From<LockClaim> for RecordedLockClaim {
    fn from(value: LockClaim) -> Self {
        Self {
            lock_uuid: value.lock_uuid,
            lock_expires_at: value.lock_expires_at,
        }
    }
}

impl From<RecordedLockClaim> for LockClaim {
    fn from(value: RecordedLockClaim) -> Self {
        Self {
            lock_uuid: value.lock_uuid,
            lock_expires_at: value.lock_expires_at,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RecordedInstanceLockStatus {
    pub instance_id: Uuid,
    pub lock_uuid: Option<Uuid>,
    pub lock_expires_at: Option<DateTime<Utc>>,
}

impl From<InstanceLockStatus> for RecordedInstanceLockStatus {
    fn from(value: InstanceLockStatus) -> Self {
        Self {
            instance_id: value.instance_id,
            lock_uuid: value.lock_uuid,
            lock_expires_at: value.lock_expires_at,
        }
    }
}

impl From<RecordedInstanceLockStatus> for InstanceLockStatus {
    fn from(value: RecordedInstanceLockStatus) -> Self {
        Self {
            instance_id: value.instance_id,
            lock_uuid: value.lock_uuid,
            lock_expires_at: value.lock_expires_at,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RecordedQueuedInstanceBatch {
    pub instances: Vec<QueuedInstance>,
}

impl From<QueuedInstanceBatch> for RecordedQueuedInstanceBatch {
    fn from(value: QueuedInstanceBatch) -> Self {
        Self {
            instances: value.instances,
        }
    }
}

impl From<RecordedQueuedInstanceBatch> for QueuedInstanceBatch {
    fn from(value: RecordedQueuedInstanceBatch) -> Self {
        Self {
            instances: value.instances,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum RecordedResult<T> {
    Ok(T),
    Err(String),
}

impl<T> RecordedResult<T> {
    pub fn into_backend_result(self) -> BackendResult<T> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Err(message) => Err(BackendError::Message(message)),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum CoreBackendActivity {
    SaveGraphs {
        claim: RecordedLockClaim,
        graphs: Vec<GraphUpdate>,
        result: RecordedResult<Vec<RecordedInstanceLockStatus>>,
    },
    SaveActionsDone {
        actions: Vec<ActionDone>,
        result: RecordedResult<()>,
    },
    GetQueuedInstances {
        size: usize,
        claim: RecordedLockClaim,
        result: RecordedResult<RecordedQueuedInstanceBatch>,
    },
    RefreshInstanceLocks {
        claim: RecordedLockClaim,
        instance_ids: Vec<Uuid>,
        result: RecordedResult<Vec<RecordedInstanceLockStatus>>,
    },
    ReleaseInstanceLocks {
        lock_uuid: Uuid,
        instance_ids: Vec<Uuid>,
        result: RecordedResult<()>,
    },
    SaveInstancesDone {
        instances: Vec<InstanceDone>,
        result: RecordedResult<()>,
    },
    QueueInstances {
        instances: Vec<QueuedInstance>,
        result: RecordedResult<()>,
    },
}

#[derive(Clone)]
pub struct ActivityLoggingBackend {
    inner: Arc<dyn CoreBackend>,
    activity: Arc<Mutex<Vec<CoreBackendActivity>>>,
}

impl ActivityLoggingBackend {
    pub fn new(inner: Arc<dyn CoreBackend>) -> Self {
        Self {
            inner,
            activity: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn activity(&self) -> Vec<CoreBackendActivity> {
        self.activity.lock().expect("activity poisoned").clone()
    }

    pub fn drain_activity(&self) -> Vec<CoreBackendActivity> {
        let mut guard = self.activity.lock().expect("activity poisoned");
        std::mem::take(&mut *guard)
    }

    fn record(&self, entry: CoreBackendActivity) {
        self.activity.lock().expect("activity poisoned").push(entry);
    }
}

#[async_trait::async_trait]
impl CoreBackend for ActivityLoggingBackend {
    fn clone_box(&self) -> Box<dyn CoreBackend> {
        Box::new(self.clone())
    }

    async fn save_graphs(
        &self,
        claim: LockClaim,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        let result = self.inner.save_graphs(claim.clone(), graphs).await;
        let recorded_result = match &result {
            Ok(statuses) => RecordedResult::Ok(
                statuses
                    .iter()
                    .cloned()
                    .map(RecordedInstanceLockStatus::from)
                    .collect(),
            ),
            Err(error) => RecordedResult::Err(error.to_string()),
        };
        self.record(CoreBackendActivity::SaveGraphs {
            claim: claim.into(),
            graphs: graphs.to_vec(),
            result: recorded_result,
        });
        result
    }

    async fn save_actions_done(&self, actions: &[ActionDone]) -> BackendResult<()> {
        let result = self.inner.save_actions_done(actions).await;
        let recorded_result = match &result {
            Ok(()) => RecordedResult::Ok(()),
            Err(error) => RecordedResult::Err(error.to_string()),
        };
        self.record(CoreBackendActivity::SaveActionsDone {
            actions: actions.to_vec(),
            result: recorded_result,
        });
        result
    }

    async fn get_queued_instances(
        &self,
        size: usize,
        claim: LockClaim,
    ) -> BackendResult<QueuedInstanceBatch> {
        let result = self.inner.get_queued_instances(size, claim.clone()).await;
        let recorded_result = match &result {
            Ok(batch) => RecordedResult::Ok(batch.clone().into()),
            Err(error) => RecordedResult::Err(error.to_string()),
        };
        self.record(CoreBackendActivity::GetQueuedInstances {
            size,
            claim: claim.into(),
            result: recorded_result,
        });
        result
    }

    async fn refresh_instance_locks(
        &self,
        claim: LockClaim,
        instance_ids: &[Uuid],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        let result = self
            .inner
            .refresh_instance_locks(claim.clone(), instance_ids)
            .await;
        let recorded_result = match &result {
            Ok(statuses) => RecordedResult::Ok(
                statuses
                    .iter()
                    .cloned()
                    .map(RecordedInstanceLockStatus::from)
                    .collect(),
            ),
            Err(error) => RecordedResult::Err(error.to_string()),
        };
        self.record(CoreBackendActivity::RefreshInstanceLocks {
            claim: claim.into(),
            instance_ids: instance_ids.to_vec(),
            result: recorded_result,
        });
        result
    }

    async fn release_instance_locks(
        &self,
        lock_uuid: Uuid,
        instance_ids: &[Uuid],
    ) -> BackendResult<()> {
        let result = self
            .inner
            .release_instance_locks(lock_uuid, instance_ids)
            .await;
        let recorded_result = match &result {
            Ok(()) => RecordedResult::Ok(()),
            Err(error) => RecordedResult::Err(error.to_string()),
        };
        self.record(CoreBackendActivity::ReleaseInstanceLocks {
            lock_uuid,
            instance_ids: instance_ids.to_vec(),
            result: recorded_result,
        });
        result
    }

    async fn save_instances_done(&self, instances: &[InstanceDone]) -> BackendResult<()> {
        let result = self.inner.save_instances_done(instances).await;
        let recorded_result = match &result {
            Ok(()) => RecordedResult::Ok(()),
            Err(error) => RecordedResult::Err(error.to_string()),
        };
        self.record(CoreBackendActivity::SaveInstancesDone {
            instances: instances.to_vec(),
            result: recorded_result,
        });
        result
    }

    async fn queue_instances(&self, instances: &[QueuedInstance]) -> BackendResult<()> {
        let result = self.inner.queue_instances(instances).await;
        let recorded_result = match &result {
            Ok(()) => RecordedResult::Ok(()),
            Err(error) => RecordedResult::Err(error.to_string()),
        };
        self.record(CoreBackendActivity::QueueInstances {
            instances: instances.to_vec(),
            result: recorded_result,
        });
        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use chrono::{Duration, Utc};
    use uuid::Uuid;
    use waymark_backend_memory::MemoryBackend;
    use waymark_core_backend::{CoreBackend, LockClaim, QueuedInstance};

    use crate::{ActivityLoggingBackend, CoreBackendActivity, RecordedResult};

    fn sample_queued_instance() -> QueuedInstance {
        QueuedInstance {
            workflow_version_id: Uuid::new_v4(),
            schedule_id: None,
            dag: None,
            entry_node: Uuid::new_v4(),
            state: None,
            action_results: HashMap::new(),
            instance_id: Uuid::new_v4(),
            scheduled_at: None,
        }
    }

    fn sample_claim() -> LockClaim {
        LockClaim {
            lock_uuid: Uuid::new_v4(),
            lock_expires_at: Utc::now() + Duration::seconds(60),
        }
    }

    #[tokio::test]
    async fn test_logs_queue_and_poll_activity() {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let inner = MemoryBackend::with_queue(queue);
        let backend = ActivityLoggingBackend::new(Arc::new(inner));

        let instance = sample_queued_instance();
        backend
            .queue_instances(std::slice::from_ref(&instance))
            .await
            .expect("queue instance");

        let batch = backend
            .get_queued_instances(1, sample_claim())
            .await
            .expect("get queued instances");

        assert_eq!(batch.instances.len(), 1);
        assert_eq!(backend.activity().len(), 2);

        let activity = backend.activity();
        match &activity[0] {
            CoreBackendActivity::QueueInstances {
                result: RecordedResult::Ok(()),
                ..
            } => {}
            other => panic!("unexpected first activity: {other:?}"),
        }

        match &activity[1] {
            CoreBackendActivity::GetQueuedInstances {
                result: RecordedResult::Ok(recorded_batch),
                ..
            } => {
                assert_eq!(recorded_batch.instances.len(), 1);
            }
            other => panic!("unexpected second activity: {other:?}"),
        }
    }
}

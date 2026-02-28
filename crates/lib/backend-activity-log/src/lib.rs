use std::sync::{Arc, Mutex};

use uuid::Uuid;
use waymark_backends_core::BackendResult;
use waymark_core_backend::{
    ActionDone, CoreBackend, GraphUpdate, InstanceDone, InstanceLockStatus, LockClaim,
    QueuedInstance, QueuedInstanceBatch,
};
use waymark_replay_activity_core::{
    BackendActivity, CoreBackendActivity, RecordedInstanceLockStatus, RecordedResult,
    RecordedWorkflowVersion, WorkflowRegistryActivity,
};
use waymark_workflow_registry_backend::{
    WorkflowRegistration, WorkflowRegistryBackend, WorkflowVersion,
};

#[derive(Clone)]
pub struct ActivityLoggingBackend {
    core_inner: Arc<dyn CoreBackend>,
    registry_inner: Arc<dyn WorkflowRegistryBackend>,
    activity: Arc<Mutex<Vec<BackendActivity>>>,
}

impl ActivityLoggingBackend {
    pub fn new<B>(inner: Arc<B>) -> Self
    where
        B: CoreBackend + WorkflowRegistryBackend + 'static,
    {
        let core_inner: Arc<dyn CoreBackend> = inner.clone();
        let registry_inner: Arc<dyn WorkflowRegistryBackend> = inner;
        Self {
            core_inner,
            registry_inner,
            activity: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn activity(&self) -> Vec<BackendActivity> {
        self.activity.lock().expect("activity poisoned").clone()
    }

    pub fn drain_activity(&self) -> Vec<BackendActivity> {
        let mut guard = self.activity.lock().expect("activity poisoned");
        std::mem::take(&mut *guard)
    }

    fn record(&self, entry: BackendActivity) {
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
        let result = self.core_inner.save_graphs(claim.clone(), graphs).await;
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
        self.record(BackendActivity::Core(CoreBackendActivity::SaveGraphs {
            claim: claim.into(),
            graphs: graphs.to_vec(),
            result: recorded_result,
        }));
        result
    }

    async fn save_actions_done(&self, actions: &[ActionDone]) -> BackendResult<()> {
        let result = self.core_inner.save_actions_done(actions).await;
        let recorded_result = match &result {
            Ok(()) => RecordedResult::Ok(()),
            Err(error) => RecordedResult::Err(error.to_string()),
        };
        self.record(BackendActivity::Core(
            CoreBackendActivity::SaveActionsDone {
                actions: actions.to_vec(),
                result: recorded_result,
            },
        ));
        result
    }

    async fn get_queued_instances(
        &self,
        size: usize,
        claim: LockClaim,
    ) -> BackendResult<QueuedInstanceBatch> {
        let result = self
            .core_inner
            .get_queued_instances(size, claim.clone())
            .await;
        let recorded_result = match &result {
            Ok(batch) => RecordedResult::Ok(batch.clone().into()),
            Err(error) => RecordedResult::Err(error.to_string()),
        };
        self.record(BackendActivity::Core(
            CoreBackendActivity::GetQueuedInstances {
                size,
                claim: claim.into(),
                result: recorded_result,
            },
        ));
        result
    }

    async fn refresh_instance_locks(
        &self,
        claim: LockClaim,
        instance_ids: &[Uuid],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        let result = self
            .core_inner
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
        self.record(BackendActivity::Core(
            CoreBackendActivity::RefreshInstanceLocks {
                claim: claim.into(),
                instance_ids: instance_ids.to_vec(),
                result: recorded_result,
            },
        ));
        result
    }

    async fn release_instance_locks(
        &self,
        lock_uuid: Uuid,
        instance_ids: &[Uuid],
    ) -> BackendResult<()> {
        let result = self
            .core_inner
            .release_instance_locks(lock_uuid, instance_ids)
            .await;
        let recorded_result = match &result {
            Ok(()) => RecordedResult::Ok(()),
            Err(error) => RecordedResult::Err(error.to_string()),
        };
        self.record(BackendActivity::Core(
            CoreBackendActivity::ReleaseInstanceLocks {
                lock_uuid,
                instance_ids: instance_ids.to_vec(),
                result: recorded_result,
            },
        ));
        result
    }

    async fn save_instances_done(&self, instances: &[InstanceDone]) -> BackendResult<()> {
        let result = self.core_inner.save_instances_done(instances).await;
        let recorded_result = match &result {
            Ok(()) => RecordedResult::Ok(()),
            Err(error) => RecordedResult::Err(error.to_string()),
        };
        self.record(BackendActivity::Core(
            CoreBackendActivity::SaveInstancesDone {
                instances: instances.to_vec(),
                result: recorded_result,
            },
        ));
        result
    }

    async fn queue_instances(&self, instances: &[QueuedInstance]) -> BackendResult<()> {
        let result = self.core_inner.queue_instances(instances).await;
        let recorded_result = match &result {
            Ok(()) => RecordedResult::Ok(()),
            Err(error) => RecordedResult::Err(error.to_string()),
        };
        self.record(BackendActivity::Core(CoreBackendActivity::QueueInstances {
            instances: instances.to_vec(),
            result: recorded_result,
        }));
        result
    }
}

#[async_trait::async_trait]
impl WorkflowRegistryBackend for ActivityLoggingBackend {
    async fn upsert_workflow_version(
        &self,
        registration: &WorkflowRegistration,
    ) -> BackendResult<Uuid> {
        let result = self
            .registry_inner
            .upsert_workflow_version(registration)
            .await;
        let recorded_result = match &result {
            Ok(id) => RecordedResult::Ok(*id),
            Err(error) => RecordedResult::Err(error.to_string()),
        };
        self.record(BackendActivity::WorkflowRegistry(
            WorkflowRegistryActivity::UpsertWorkflowVersion {
                registration: registration.clone().into(),
                result: recorded_result,
            },
        ));
        result
    }

    async fn get_workflow_versions(&self, ids: &[Uuid]) -> BackendResult<Vec<WorkflowVersion>> {
        let result = self.registry_inner.get_workflow_versions(ids).await;
        let recorded_result = match &result {
            Ok(versions) => RecordedResult::Ok(
                versions
                    .iter()
                    .cloned()
                    .map(RecordedWorkflowVersion::from)
                    .collect(),
            ),
            Err(error) => RecordedResult::Err(error.to_string()),
        };
        self.record(BackendActivity::WorkflowRegistry(
            WorkflowRegistryActivity::GetWorkflowVersions {
                ids: ids.to_vec(),
                result: recorded_result,
            },
        ));
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
    use waymark_replay_activity_core::{BackendActivity, CoreBackendActivity, RecordedResult};

    use crate::ActivityLoggingBackend;

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
            BackendActivity::Core(CoreBackendActivity::QueueInstances {
                result: RecordedResult::Ok(()),
                ..
            }) => {}
            other => panic!("unexpected first activity: {other:?}"),
        }

        match &activity[1] {
            BackendActivity::Core(CoreBackendActivity::GetQueuedInstances {
                result: RecordedResult::Ok(recorded_batch),
                ..
            }) => {
                assert_eq!(recorded_batch.instances.len(), 1);
            }
            other => panic!("unexpected second activity: {other:?}"),
        }
    }
}

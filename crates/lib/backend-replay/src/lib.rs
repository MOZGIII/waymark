use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use uuid::Uuid;
use waymark_backend_activity_log::{
    CoreBackendActivity, RecordedInstanceLockStatus, RecordedResult,
};
use waymark_backends_core::{BackendError, BackendResult};
use waymark_core_backend::{
    ActionDone, CoreBackend, GraphUpdate, InstanceDone, InstanceLockStatus, LockClaim,
    QueuedInstance, QueuedInstanceBatch,
};

#[derive(Clone)]
pub struct ReplayBackend {
    activity: Arc<Mutex<VecDeque<CoreBackendActivity>>>,
}

impl ReplayBackend {
    pub fn new(activity: Vec<CoreBackendActivity>) -> Self {
        Self {
            activity: Arc::new(Mutex::new(VecDeque::from(activity))),
        }
    }

    pub fn remaining_activity_len(&self) -> usize {
        self.activity.lock().expect("activity queue poisoned").len()
    }

    fn next_activity(&self, method: &str) -> BackendResult<CoreBackendActivity> {
        let mut guard = self.activity.lock().expect("activity queue poisoned");
        let Some(entry) = guard.pop_front() else {
            return Err(BackendError::Message(format!(
                "replay exhausted while handling method: {method}",
            )));
        };
        Ok(entry)
    }

    fn unexpected(expected: &str, got: &CoreBackendActivity) -> BackendError {
        BackendError::Message(format!(
            "replay activity mismatch: expected {expected}, got {got:?}",
        ))
    }
}

fn replay_statuses(
    result: RecordedResult<Vec<RecordedInstanceLockStatus>>,
) -> BackendResult<Vec<InstanceLockStatus>> {
    result
        .into_backend_result()
        .map(|statuses| statuses.into_iter().map(InstanceLockStatus::from).collect())
}

#[async_trait::async_trait]
impl CoreBackend for ReplayBackend {
    fn clone_box(&self) -> Box<dyn CoreBackend> {
        Box::new(self.clone())
    }

    async fn save_graphs(
        &self,
        _claim: LockClaim,
        _graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        let entry = self.next_activity("save_graphs")?;
        match entry {
            CoreBackendActivity::SaveGraphs { result, .. } => replay_statuses(result),
            other => Err(Self::unexpected("SaveGraphs", &other)),
        }
    }

    async fn save_actions_done(&self, _actions: &[ActionDone]) -> BackendResult<()> {
        let entry = self.next_activity("save_actions_done")?;
        match entry {
            CoreBackendActivity::SaveActionsDone { result, .. } => result.into_backend_result(),
            other => Err(Self::unexpected("SaveActionsDone", &other)),
        }
    }

    async fn get_queued_instances(
        &self,
        _size: usize,
        _claim: LockClaim,
    ) -> BackendResult<QueuedInstanceBatch> {
        let entry = self.next_activity("get_queued_instances")?;
        match entry {
            CoreBackendActivity::GetQueuedInstances { result, .. } => {
                result.into_backend_result().map(QueuedInstanceBatch::from)
            }
            other => Err(Self::unexpected("GetQueuedInstances", &other)),
        }
    }

    async fn refresh_instance_locks(
        &self,
        _claim: LockClaim,
        _instance_ids: &[Uuid],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        let entry = self.next_activity("refresh_instance_locks")?;
        match entry {
            CoreBackendActivity::RefreshInstanceLocks { result, .. } => replay_statuses(result),
            other => Err(Self::unexpected("RefreshInstanceLocks", &other)),
        }
    }

    async fn release_instance_locks(
        &self,
        _lock_uuid: Uuid,
        _instance_ids: &[Uuid],
    ) -> BackendResult<()> {
        let entry = self.next_activity("release_instance_locks")?;
        match entry {
            CoreBackendActivity::ReleaseInstanceLocks { result, .. } => {
                result.into_backend_result()
            }
            other => Err(Self::unexpected("ReleaseInstanceLocks", &other)),
        }
    }

    async fn save_instances_done(&self, _instances: &[InstanceDone]) -> BackendResult<()> {
        let entry = self.next_activity("save_instances_done")?;
        match entry {
            CoreBackendActivity::SaveInstancesDone { result, .. } => result.into_backend_result(),
            other => Err(Self::unexpected("SaveInstancesDone", &other)),
        }
    }

    async fn queue_instances(&self, _instances: &[QueuedInstance]) -> BackendResult<()> {
        let entry = self.next_activity("queue_instances")?;
        match entry {
            CoreBackendActivity::QueueInstances { result, .. } => result.into_backend_result(),
            other => Err(Self::unexpected("QueueInstances", &other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{Duration, Utc};
    use uuid::Uuid;
    use waymark_backend_activity_log::{
        CoreBackendActivity, RecordedLockClaim, RecordedQueuedInstanceBatch, RecordedResult,
    };
    use waymark_core_backend::{CoreBackend, LockClaim, QueuedInstance};

    use crate::ReplayBackend;

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
    async fn test_replays_queue_and_poll() {
        let claim = sample_claim();
        let instance = sample_queued_instance();
        let activity = vec![
            CoreBackendActivity::QueueInstances {
                instances: vec![instance.clone()],
                result: RecordedResult::Ok(()),
            },
            CoreBackendActivity::GetQueuedInstances {
                size: 1,
                claim: RecordedLockClaim::from(claim.clone()),
                result: RecordedResult::Ok(RecordedQueuedInstanceBatch {
                    instances: vec![instance],
                }),
            },
        ];
        let backend = ReplayBackend::new(activity);

        backend
            .queue_instances(&[])
            .await
            .expect("queue instances replay");

        let batch = backend
            .get_queued_instances(1, claim)
            .await
            .expect("poll instances replay");

        assert_eq!(batch.instances.len(), 1);
        assert_eq!(backend.remaining_activity_len(), 0);
    }

    #[tokio::test]
    async fn test_returns_error_on_activity_mismatch() {
        let activity = vec![CoreBackendActivity::QueueInstances {
            instances: vec![],
            result: RecordedResult::Ok(()),
        }];
        let backend = ReplayBackend::new(activity);

        let result = backend.save_actions_done(&[]).await;
        assert!(result.is_err());

        let message = result.expect_err("expected replay mismatch").to_string();
        assert!(message.contains("SaveActionsDone"));
    }
}

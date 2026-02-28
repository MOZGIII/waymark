use chrono::{DateTime, Utc};
use uuid::Uuid;
use waymark_backends_core::{BackendError, BackendResult};
use waymark_core_backend::{
    ActionDone, GraphUpdate, InstanceDone, InstanceLockStatus, LockClaim, QueuedInstance,
    QueuedInstanceBatch,
};
use waymark_workflow_registry_backend::{WorkflowRegistration, WorkflowVersion};

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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RecordedWorkflowRegistration {
    pub workflow_name: String,
    pub workflow_version: String,
    pub ir_hash: String,
    pub program_proto: Vec<u8>,
    pub concurrent: bool,
}

impl From<WorkflowRegistration> for RecordedWorkflowRegistration {
    fn from(value: WorkflowRegistration) -> Self {
        Self {
            workflow_name: value.workflow_name,
            workflow_version: value.workflow_version,
            ir_hash: value.ir_hash,
            program_proto: value.program_proto,
            concurrent: value.concurrent,
        }
    }
}

impl From<RecordedWorkflowRegistration> for WorkflowRegistration {
    fn from(value: RecordedWorkflowRegistration) -> Self {
        Self {
            workflow_name: value.workflow_name,
            workflow_version: value.workflow_version,
            ir_hash: value.ir_hash,
            program_proto: value.program_proto,
            concurrent: value.concurrent,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RecordedWorkflowVersion {
    pub id: Uuid,
    pub workflow_name: String,
    pub workflow_version: String,
    pub ir_hash: String,
    pub program_proto: Vec<u8>,
    pub concurrent: bool,
}

impl From<WorkflowVersion> for RecordedWorkflowVersion {
    fn from(value: WorkflowVersion) -> Self {
        Self {
            id: value.id,
            workflow_name: value.workflow_name,
            workflow_version: value.workflow_version,
            ir_hash: value.ir_hash,
            program_proto: value.program_proto,
            concurrent: value.concurrent,
        }
    }
}

impl From<RecordedWorkflowVersion> for WorkflowVersion {
    fn from(value: RecordedWorkflowVersion) -> Self {
        Self {
            id: value.id,
            workflow_name: value.workflow_name,
            workflow_version: value.workflow_version,
            ir_hash: value.ir_hash,
            program_proto: value.program_proto,
            concurrent: value.concurrent,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum WorkflowRegistryActivity {
    UpsertWorkflowVersion {
        registration: RecordedWorkflowRegistration,
        result: RecordedResult<Uuid>,
    },
    GetWorkflowVersions {
        ids: Vec<Uuid>,
        result: RecordedResult<Vec<RecordedWorkflowVersion>>,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum BackendActivity {
    Core(CoreBackendActivity),
    WorkflowRegistry(WorkflowRegistryActivity),
}

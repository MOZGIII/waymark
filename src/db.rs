use anyhow::{Context, Result};
use sqlx::{FromRow, PgPool, postgres::PgPoolOptions};

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

#[derive(Debug, Clone, FromRow)]
pub struct LedgerAction {
    pub id: i64,
    pub instance_id: i64,
    pub partition_id: i32,
    pub action_seq: i32,
    pub payload: Vec<u8>,
}

impl Database {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .with_context(|| "failed to connect to postgres")?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .with_context(|| "failed to run migrations")?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn reset_partition(&self, partition_id: i32) -> Result<()> {
        let span = tracing::info_span!("db.reset_partition", partition_id);
        let _guard = span.enter();
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM daemon_action_ledger WHERE partition_id = $1")
            .bind(partition_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM workflow_instances WHERE partition_id = $1")
            .bind(partition_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn seed_actions(
        &self,
        partition_id: i32,
        action_count: usize,
        payload_size: usize,
    ) -> Result<()> {
        let span = tracing::info_span!("db.seed_actions", partition_id, action_count, payload_size);
        let _guard = span.enter();
        let mut tx = self.pool.begin().await?;
        let instance_id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO workflow_instances (partition_id, workflow_name, next_action_seq)
            VALUES ($1, $2, $3)
            RETURNING id
            "#,
        )
        .bind(partition_id)
        .bind("benchmark")
        .bind(action_count as i32)
        .fetch_one(&mut *tx)
        .await?;

        for seq in 0..action_count {
            let payload = vec![0u8; payload_size];
            sqlx::query(
                r#"
                INSERT INTO daemon_action_ledger (
                    instance_id,
                    partition_id,
                    action_seq,
                    status,
                    payload
                ) VALUES ($1, $2, $3, 'queued', $4)
                "#,
            )
            .bind(instance_id)
            .bind(partition_id)
            .bind(seq as i32)
            .bind(payload)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn dispatch_actions(
        &self,
        partition_id: i32,
        limit: i64,
    ) -> Result<Vec<LedgerAction>> {
        let span = tracing::info_span!("db.dispatch_actions", partition_id, limit);
        let _guard = span.enter();
        let records = sqlx::query_as::<_, LedgerAction>(
            r#"
            WITH next_actions AS (
                SELECT id
                FROM daemon_action_ledger
                WHERE partition_id = $1 AND status = 'queued'
                ORDER BY action_seq
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
            UPDATE daemon_action_ledger AS dal
            SET status = 'dispatched', dispatched_at = NOW()
            FROM next_actions
            WHERE dal.id = next_actions.id
            RETURNING dal.id, dal.instance_id, dal.partition_id, dal.action_seq, dal.payload
            "#,
        )
        .bind(partition_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(records)
    }

    pub async fn record_delivery(&self, action_id: i64, delivery_id: u64) -> Result<()> {
        tracing::debug!(action_id, delivery_id, "db.record_delivery");
        sqlx::query("UPDATE daemon_action_ledger SET delivery_id = $2 WHERE id = $1")
            .bind(action_id)
            .bind(delivery_id as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn mark_action_acked(&self, action_id: i64) -> Result<()> {
        tracing::debug!(action_id, "db.mark_action_acked");
        sqlx::query(
            "UPDATE daemon_action_ledger SET status = 'acked', acked_at = NOW() WHERE id = $1",
        )
        .bind(action_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_action_completed(
        &self,
        action_id: i64,
        success: bool,
        result_payload: &[u8],
    ) -> Result<()> {
        tracing::debug!(
            action_id,
            success,
            payload_len = result_payload.len(),
            "db.mark_action_completed"
        );
        sqlx::query(
            r#"
            UPDATE daemon_action_ledger
            SET status = 'completed', success = $2, completed_at = NOW(), result_payload = $3
            WHERE id = $1
            "#,
        )
        .bind(action_id)
        .bind(success)
        .bind(result_payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

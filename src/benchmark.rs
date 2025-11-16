use std::time::{Duration, Instant};

use anyhow::Result;
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use tracing::info;

use crate::{
    db::Database,
    messages::MessageError,
    python_worker::{
        ActionDispatchPayload, PythonWorkerConfig, PythonWorkerPool, RoundTripMetrics,
    },
};

#[derive(Debug, Clone)]
pub struct HarnessConfig {
    pub total_messages: usize,
    pub in_flight: usize,
    pub payload_size: usize,
    pub partition_id: i32,
    pub progress_interval: Option<Duration>,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            total_messages: 10_000,
            in_flight: 32,
            payload_size: 4096,
            partition_id: 0,
            progress_interval: None,
        }
    }
}

pub struct BenchmarkHarness {
    workers: PythonWorkerPool,
    database: Database,
}

impl BenchmarkHarness {
    pub async fn new(
        config: PythonWorkerConfig,
        worker_count: usize,
        database: Database,
    ) -> Result<Self> {
        let workers = PythonWorkerPool::new(config, worker_count).await?;
        Ok(Self { workers, database })
    }

    pub async fn run(&self, config: &HarnessConfig) -> Result<BenchmarkSummary> {
        self.database.reset_partition(config.partition_id).await?;
        self.database
            .seed_actions(
                config.partition_id,
                config.total_messages,
                config.payload_size,
            )
            .await?;

        let total = config.total_messages;
        let mut completed = Vec::with_capacity(total);
        let mut inflight: FuturesUnordered<BoxFuture<'_, Result<RoundTripMetrics, MessageError>>> =
            FuturesUnordered::new();
        let mut dispatched = 0usize;
        let start = Instant::now();
        let worker_count = self.workers.len().max(1);
        let max_inflight = config.in_flight.max(1) * worker_count;
        let mut last_report = start;

        while completed.len() < total {
            while inflight.len() < max_inflight && dispatched < total {
                let needed = (max_inflight - inflight.len()).min(total - dispatched);
                let actions = self
                    .database
                    .dispatch_actions(config.partition_id, needed as i64)
                    .await?;
                if actions.is_empty() {
                    break;
                }

                for action in actions {
                    let payload = ActionDispatchPayload {
                        action_id: action.id,
                        instance_id: action.instance_id,
                        sequence: action.action_seq,
                        payload: action.payload,
                    };
                    let worker = self.workers.next_worker();
                    let fut: BoxFuture<'_, Result<RoundTripMetrics, MessageError>> =
                        Box::pin(async move {
                            let span = tracing::info_span!(
                                "dispatch",
                                action_id = payload.action_id,
                                instance_id = payload.instance_id,
                                sequence = payload.sequence
                            );
                            let _guard = span.enter();
                            worker.send_action(payload).await
                        });
                    inflight.push(fut);
                    dispatched += 1;
                }
            }

            match inflight.next().await {
                Some(Ok(metrics)) => {
                    self.database
                        .record_delivery(metrics.action_id, metrics.delivery_id)
                        .await?;
                    self.database.mark_action_acked(metrics.action_id).await?;
                    self.database
                        .mark_action_completed(
                            metrics.action_id,
                            metrics.success,
                            &metrics.response_payload,
                        )
                        .await?;
                    tracing::debug!(
                        action_id = metrics.action_id,
                        round_trip_ms = %format!("{:.3}", metrics.round_trip.as_secs_f64() * 1000.0),
                        ack_ms = %format!("{:.3}", metrics.ack_latency.as_secs_f64() * 1000.0),
                        worker_ms = %format!("{:.3}", metrics.worker_duration.as_secs_f64() * 1000.0),
                        "action completed"
                    );
                    completed.push(BenchmarkResult::from(metrics));
                    if let Some(interval) = config.progress_interval {
                        let now = Instant::now();
                        if now.duration_since(last_report) >= interval {
                            let elapsed = now.duration_since(start);
                            let throughput =
                                completed.len() as f64 / elapsed.as_secs_f64().max(1e-9);
                            info!(
                                processed = completed.len(),
                                total,
                                elapsed = %format!("{:.1}s", elapsed.as_secs_f64()),
                                throughput = %format!("{:.0} msg/s", throughput),
                                in_flight = inflight.len(),
                                worker_count,
                                db_queue = dispatched - completed.len(),
                                dispatched,
                                "benchmark progress",
                            );
                            last_report = now;
                        }
                    }
                }
                Some(Err(err)) => return Err(err.into()),
                None => {
                    if dispatched >= total {
                        break;
                    }
                }
            }
        }

        let elapsed = start.elapsed();
        let summary = BenchmarkSummary::from_results(completed, elapsed);
        Ok(summary)
    }

    pub async fn shutdown(self) -> Result<()> {
        self.workers.shutdown().await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub sequence: u32,
    pub ack_latency: Duration,
    pub round_trip: Duration,
    pub worker_duration: Duration,
}

impl From<RoundTripMetrics> for BenchmarkResult {
    fn from(value: RoundTripMetrics) -> Self {
        Self {
            sequence: value.sequence,
            ack_latency: value.ack_latency,
            round_trip: value.round_trip,
            worker_duration: value.worker_duration,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkSummary {
    pub total_messages: usize,
    pub elapsed: Duration,
    pub throughput_per_sec: f64,
    pub avg_ack_ms: f64,
    pub avg_round_trip_ms: f64,
    pub worker_avg_ms: f64,
    pub p95_round_trip_ms: f64,
}

impl BenchmarkSummary {
    fn from_results(results: Vec<BenchmarkResult>, elapsed: Duration) -> Self {
        let total_messages = results.len();
        let throughput_per_sec = total_messages as f64 / elapsed.as_secs_f64().max(1e-9);

        let mut ack_sum = 0f64;
        let mut round_sum = 0f64;
        let mut worker_sum = 0f64;
        let mut round_sorted = Vec::with_capacity(total_messages);

        for result in &results {
            ack_sum += result.ack_latency.as_secs_f64();
            round_sum += result.round_trip.as_secs_f64();
            worker_sum += result.worker_duration.as_secs_f64();
            round_sorted.push(result.round_trip);
        }

        round_sorted.sort();
        let denom = total_messages.max(1) as f64;
        let p95_index = if round_sorted.is_empty() {
            0
        } else {
            let idx = ((round_sorted.len() - 1) as f64 * 0.95).round() as usize;
            idx.min(round_sorted.len() - 1)
        };
        let p95_round_trip_ms = round_sorted
            .get(p95_index)
            .copied()
            .unwrap_or_default()
            .as_secs_f64()
            * 1000.0;

        Self {
            total_messages,
            elapsed,
            throughput_per_sec,
            avg_ack_ms: (ack_sum / denom) * 1000.0,
            avg_round_trip_ms: (round_sum / denom) * 1000.0,
            worker_avg_ms: (worker_sum / denom) * 1000.0,
            p95_round_trip_ms,
        }
    }

    pub fn log(&self) {
        info!(
            total = self.total_messages,
            elapsed = %format!("{:.2}s", self.elapsed.as_secs_f64()),
            throughput = %format!("{:.0} msg/s", self.throughput_per_sec),
            avg_ack_ms = %format!("{:.3} ms", self.avg_ack_ms),
            avg_round_trip_ms = %format!("{:.3} ms", self.avg_round_trip_ms),
            worker_avg_ms = %format!("{:.3} ms", self.worker_avg_ms),
            p95_round_trip_ms = %format!("{:.3} ms", self.p95_round_trip_ms),
            "benchmark summary",
        );
    }
}

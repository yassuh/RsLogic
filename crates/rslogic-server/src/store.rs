use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{Duration, Utc};
use rslogic_protocol::{
    new_id, now, AgentStatus, Challenge, DesiredState, EnrollmentApproval, EnrollmentRequest,
    EnrollmentRequestRecord, EnrollmentStatus, JobEvent, JobState, MachineTelemetry, PipelineJob,
    ServerCommand, SessionToken, UploadedArtifact,
};
use serde::{Deserialize, Serialize};
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    PgPool, Row,
};
use thiserror::Error;
use tokio::sync::RwLock;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("{0}")]
    NotFound(&'static str),
    #[error("{0}")]
    InvalidState(&'static str),
    #[error("invalid enrollment status: {0}")]
    InvalidEnrollmentStatus(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug, Clone)]
pub struct ClientRecord {
    pub client_id: String,
    pub public_key: String,
    pub desired_state: DesiredState,
    pub approved_at: chrono::DateTime<Utc>,
    pub revoked_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminClientRecord {
    pub client_id: String,
    pub public_key: String,
    pub desired_state: DesiredState,
    pub approved_at: chrono::DateTime<Utc>,
    pub revoked_at: Option<chrono::DateTime<Utc>>,
    pub enrollment: Option<EnrollmentRequest>,
    pub latest_status: Option<AgentStatus>,
    pub latest_telemetry: Option<MachineTelemetry>,
    pub last_heartbeat_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedCommand {
    pub command_id: String,
    pub client_id: String,
    pub command: ServerCommand,
    pub created_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub job_id: String,
    pub client_id: String,
    pub job: PipelineJob,
    pub state: JobState,
    pub assigned_at: chrono::DateTime<Utc>,
    pub updated_at: chrono::DateTime<Utc>,
    pub completed_at: Option<chrono::DateTime<Utc>>,
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn create_enrollment_request(
        &self,
        request: EnrollmentRequest,
    ) -> Result<EnrollmentRequestRecord>;
    async fn get_enrollment_request(
        &self,
        request_id: &str,
    ) -> Result<Option<EnrollmentRequestRecord>>;
    async fn list_enrollment_requests(&self) -> Result<Vec<EnrollmentRequestRecord>>;
    async fn approve_enrollment_request(&self, request_id: &str) -> Result<EnrollmentApproval>;
    async fn reject_enrollment_request(
        &self,
        request_id: &str,
        reason: Option<String>,
    ) -> Result<EnrollmentRequestRecord>;
    async fn list_clients(&self) -> Result<Vec<AdminClientRecord>>;
    async fn get_client(&self, client_id: &str) -> Result<Option<ClientRecord>>;
    async fn create_challenge(&self, client_id: &str) -> Result<Option<Challenge>>;
    async fn consume_challenge(&self, challenge_id: &str) -> Result<Option<Challenge>>;
    async fn create_session(&self, token: SessionToken) -> Result<SessionToken>;
    async fn get_session(&self, token: &str) -> Result<Option<SessionToken>>;
    async fn revoke_client(&self, client_id: &str) -> Result<Option<ClientRecord>>;
    async fn enqueue_command(
        &self,
        client_id: &str,
        command: ServerCommand,
    ) -> Result<Option<QueuedCommand>>;
    async fn list_pending_commands(
        &self,
        client_id: &str,
        limit: u32,
    ) -> Result<Vec<QueuedCommand>>;
    async fn mark_command_delivered(&self, command_id: &str) -> Result<()>;
    async fn record_client_heartbeat(
        &self,
        client_id: &str,
        observed_at: chrono::DateTime<Utc>,
        telemetry: MachineTelemetry,
    ) -> Result<()>;
    async fn record_agent_status(&self, client_id: &str, status: AgentStatus) -> Result<()>;
    async fn record_job_assignment(&self, client_id: &str, job: PipelineJob) -> Result<JobRecord>;
    async fn record_job_event(&self, client_id: &str, event: JobEvent) -> Result<()>;
    async fn list_job_events(&self, job_id: Option<&str>, limit: u32) -> Result<Vec<JobEvent>>;
    async fn record_uploaded_artifact(
        &self,
        client_id: &str,
        artifact: UploadedArtifact,
    ) -> Result<()>;
    async fn list_uploaded_artifacts(&self) -> Result<Vec<UploadedArtifact>>;
    async fn list_jobs(&self) -> Result<Vec<JobRecord>>;
}

#[derive(Debug, Default)]
pub struct InMemoryStore {
    inner: RwLock<ServerState>,
}

#[derive(Debug, Default)]
struct ServerState {
    enrollments: HashMap<String, EnrollmentRequestRecord>,
    clients: HashMap<String, ClientRecord>,
    challenges: HashMap<String, Challenge>,
    sessions: HashMap<String, SessionToken>,
    commands: HashMap<String, Vec<QueuedCommand>>,
    latest_agent_status: HashMap<String, AgentStatus>,
    latest_telemetry: HashMap<String, (chrono::DateTime<Utc>, MachineTelemetry)>,
    jobs: HashMap<String, JobRecord>,
    artifacts: HashMap<String, UploadedArtifact>,
    job_events: Vec<JobEvent>,
}

#[async_trait]
impl Store for InMemoryStore {
    async fn create_enrollment_request(
        &self,
        request: EnrollmentRequest,
    ) -> Result<EnrollmentRequestRecord> {
        let request_id = new_id();
        let record = EnrollmentRequestRecord {
            request_id: request_id.clone(),
            status: EnrollmentStatus::Pending,
            client_id: None,
            request,
            created_at: now(),
            decided_at: None,
            rejection_reason: None,
        };
        let mut guard = self.inner.write().await;
        guard.enrollments.insert(request_id, record.clone());
        Ok(record)
    }

    async fn get_enrollment_request(
        &self,
        request_id: &str,
    ) -> Result<Option<EnrollmentRequestRecord>> {
        let guard = self.inner.read().await;
        Ok(guard.enrollments.get(request_id).cloned())
    }

    async fn list_enrollment_requests(&self) -> Result<Vec<EnrollmentRequestRecord>> {
        let guard = self.inner.read().await;
        let mut records: Vec<_> = guard.enrollments.values().cloned().collect();
        records.sort_by_key(|record| record.created_at);
        Ok(records)
    }

    async fn approve_enrollment_request(&self, request_id: &str) -> Result<EnrollmentApproval> {
        let mut guard = self.inner.write().await;
        let client_id = format!("client-{}", new_id());
        let approved_at = now();
        let public_key = {
            let record = guard
                .enrollments
                .get_mut(request_id)
                .ok_or(StoreError::NotFound("enrollment request not found"))?;
            if record.status != EnrollmentStatus::Pending {
                return Err(StoreError::InvalidState(
                    "enrollment request is not pending",
                ));
            }
            record.status = EnrollmentStatus::Approved;
            record.client_id = Some(client_id.clone());
            record.decided_at = Some(approved_at);
            record.request.public_key.clone()
        };

        guard.clients.insert(
            client_id.clone(),
            ClientRecord {
                client_id: client_id.clone(),
                public_key,
                desired_state: DesiredState::default(),
                approved_at,
                revoked_at: None,
            },
        );

        Ok(EnrollmentApproval {
            request_id: request_id.to_string(),
            client_id,
            approved_at,
        })
    }

    async fn reject_enrollment_request(
        &self,
        request_id: &str,
        reason: Option<String>,
    ) -> Result<EnrollmentRequestRecord> {
        let mut guard = self.inner.write().await;
        let record = guard
            .enrollments
            .get_mut(request_id)
            .ok_or(StoreError::NotFound("enrollment request not found"))?;
        if record.status != EnrollmentStatus::Pending {
            return Err(StoreError::InvalidState(
                "enrollment request is not pending",
            ));
        }
        record.status = EnrollmentStatus::Rejected;
        record.decided_at = Some(now());
        record.rejection_reason = reason;
        Ok(record.clone())
    }

    async fn list_clients(&self) -> Result<Vec<AdminClientRecord>> {
        let guard = self.inner.read().await;
        let mut clients: Vec<_> = guard
            .clients
            .values()
            .map(|client| {
                let enrollment = guard
                    .enrollments
                    .values()
                    .find(|record| record.client_id.as_deref() == Some(client.client_id.as_str()))
                    .map(|record| record.request.clone());
                AdminClientRecord {
                    client_id: client.client_id.clone(),
                    public_key: client.public_key.clone(),
                    desired_state: client.desired_state.clone(),
                    approved_at: client.approved_at,
                    revoked_at: client.revoked_at,
                    enrollment,
                    latest_status: guard.latest_agent_status.get(&client.client_id).cloned(),
                    latest_telemetry: guard
                        .latest_telemetry
                        .get(&client.client_id)
                        .map(|(_, telemetry)| telemetry.clone()),
                    last_heartbeat_at: guard
                        .latest_telemetry
                        .get(&client.client_id)
                        .map(|(observed_at, _)| *observed_at),
                }
            })
            .collect();
        clients.sort_by_key(|client| std::cmp::Reverse(client.approved_at));
        Ok(clients)
    }

    async fn get_client(&self, client_id: &str) -> Result<Option<ClientRecord>> {
        let guard = self.inner.read().await;
        Ok(guard.clients.get(client_id).cloned())
    }

    async fn create_challenge(&self, client_id: &str) -> Result<Option<Challenge>> {
        let mut guard = self.inner.write().await;
        let Some(client) = guard.clients.get(client_id) else {
            return Ok(None);
        };
        if client.revoked_at.is_some() {
            return Ok(None);
        }
        let challenge = Challenge {
            challenge_id: new_id(),
            client_id: client_id.to_string(),
            nonce: new_id(),
            expires_at: now() + Duration::minutes(5),
        };
        guard
            .challenges
            .insert(challenge.challenge_id.clone(), challenge.clone());
        Ok(Some(challenge))
    }

    async fn consume_challenge(&self, challenge_id: &str) -> Result<Option<Challenge>> {
        let mut guard = self.inner.write().await;
        Ok(guard.challenges.remove(challenge_id))
    }

    async fn create_session(&self, token: SessionToken) -> Result<SessionToken> {
        let mut guard = self.inner.write().await;
        guard.sessions.insert(token.token.clone(), token.clone());
        Ok(token)
    }

    async fn get_session(&self, token: &str) -> Result<Option<SessionToken>> {
        let guard = self.inner.read().await;
        Ok(guard.sessions.get(token).cloned())
    }

    async fn revoke_client(&self, client_id: &str) -> Result<Option<ClientRecord>> {
        let mut guard = self.inner.write().await;
        let revoked_at = now();
        let Some(client) = guard.clients.get_mut(client_id) else {
            return Ok(None);
        };
        client.revoked_at = Some(revoked_at);
        let client = client.clone();
        guard
            .sessions
            .retain(|_, session| session.client_id != client_id);
        Ok(Some(client))
    }

    async fn enqueue_command(
        &self,
        client_id: &str,
        command: ServerCommand,
    ) -> Result<Option<QueuedCommand>> {
        let mut guard = self.inner.write().await;
        let Some(client) = guard.clients.get(client_id) else {
            return Ok(None);
        };
        if client.revoked_at.is_some() {
            return Ok(None);
        }
        let queued = QueuedCommand {
            command_id: new_id(),
            client_id: client_id.to_string(),
            command,
            created_at: now(),
        };
        guard
            .commands
            .entry(client_id.to_string())
            .or_default()
            .push(queued.clone());
        Ok(Some(queued))
    }

    async fn list_pending_commands(
        &self,
        client_id: &str,
        limit: u32,
    ) -> Result<Vec<QueuedCommand>> {
        let guard = self.inner.read().await;
        Ok(guard
            .commands
            .get(client_id)
            .map(|commands| {
                commands
                    .iter()
                    .take(limit as usize)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default())
    }

    async fn mark_command_delivered(&self, command_id: &str) -> Result<()> {
        let mut guard = self.inner.write().await;
        for commands in guard.commands.values_mut() {
            if let Some(index) = commands
                .iter()
                .position(|command| command.command_id == command_id)
            {
                commands.remove(index);
                return Ok(());
            }
        }
        Ok(())
    }

    async fn record_client_heartbeat(
        &self,
        client_id: &str,
        observed_at: chrono::DateTime<Utc>,
        telemetry: MachineTelemetry,
    ) -> Result<()> {
        let mut guard = self.inner.write().await;
        guard
            .latest_telemetry
            .insert(client_id.to_string(), (observed_at, telemetry));
        Ok(())
    }

    async fn record_agent_status(&self, client_id: &str, status: AgentStatus) -> Result<()> {
        let mut guard = self.inner.write().await;
        guard.latest_telemetry.insert(
            client_id.to_string(),
            (status.telemetry.observed_at, status.telemetry.clone()),
        );
        guard
            .latest_agent_status
            .insert(client_id.to_string(), status);
        Ok(())
    }

    async fn record_job_assignment(&self, client_id: &str, job: PipelineJob) -> Result<JobRecord> {
        let now = now();
        let record = JobRecord {
            job_id: job.job_id.clone(),
            client_id: client_id.to_string(),
            job,
            state: JobState::Assigned,
            assigned_at: now,
            updated_at: now,
            completed_at: None,
        };
        let mut guard = self.inner.write().await;
        guard.jobs.insert(record.job_id.clone(), record.clone());
        Ok(record)
    }

    async fn record_job_event(&self, _client_id: &str, event: JobEvent) -> Result<()> {
        let mut guard = self.inner.write().await;
        if let Some(job) = guard.jobs.get_mut(&event.job_id) {
            job.state = event.state.clone();
            job.updated_at = now();
            if is_terminal_job_state(&event.state) {
                job.completed_at = Some(job.updated_at);
            }
        }
        guard.job_events.push(event);
        Ok(())
    }

    async fn list_job_events(&self, job_id: Option<&str>, limit: u32) -> Result<Vec<JobEvent>> {
        let guard = self.inner.read().await;
        let limit = limit.clamp(1, 500) as usize;
        let mut events = guard
            .job_events
            .iter()
            .filter(|event| job_id.map_or(true, |job_id| event.job_id == job_id))
            .cloned()
            .collect::<Vec<_>>();
        events.sort_by_key(|event| std::cmp::Reverse(event.observed_at));
        events.truncate(limit);
        Ok(events)
    }

    async fn record_uploaded_artifact(
        &self,
        _client_id: &str,
        artifact: UploadedArtifact,
    ) -> Result<()> {
        let mut guard = self.inner.write().await;
        guard
            .artifacts
            .insert(artifact.artifact_id.clone(), artifact);
        Ok(())
    }

    async fn list_uploaded_artifacts(&self) -> Result<Vec<UploadedArtifact>> {
        let guard = self.inner.read().await;
        let mut artifacts: Vec<_> = guard.artifacts.values().cloned().collect();
        artifacts.sort_by(|left, right| {
            left.job_id
                .cmp(&right.job_id)
                .then_with(|| left.filename.cmp(&right.filename))
        });
        Ok(artifacts)
    }

    async fn list_jobs(&self) -> Result<Vec<JobRecord>> {
        let guard = self.inner.read().await;
        let mut jobs: Vec<_> = guard.jobs.values().cloned().collect();
        jobs.sort_by_key(|job| std::cmp::Reverse(job.updated_at));
        Ok(jobs)
    }
}

#[derive(Debug, Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl Store for PostgresStore {
    async fn create_enrollment_request(
        &self,
        request: EnrollmentRequest,
    ) -> Result<EnrollmentRequestRecord> {
        let request_id = new_id();
        let record = EnrollmentRequestRecord {
            request_id: request_id.clone(),
            status: EnrollmentStatus::Pending,
            client_id: None,
            request,
            created_at: now(),
            decided_at: None,
            rejection_reason: None,
        };
        let request_payload = serde_json::to_value(&record.request)?;
        sqlx::query(
            r#"
            INSERT INTO client_enrollment_requests
              (request_id, status, client_id, request_payload, created_at, decided_at, rejection_reason)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(&record.request_id)
        .bind(enrollment_status_as_str(&record.status))
        .bind(&record.client_id)
        .bind(request_payload)
        .bind(record.created_at)
        .bind(record.decided_at)
        .bind(&record.rejection_reason)
        .execute(&self.pool)
        .await?;
        Ok(record)
    }

    async fn get_enrollment_request(
        &self,
        request_id: &str,
    ) -> Result<Option<EnrollmentRequestRecord>> {
        let row = sqlx::query(
            r#"
            SELECT request_id, status, client_id, request_payload, created_at, decided_at, rejection_reason
            FROM client_enrollment_requests
            WHERE request_id = $1
            "#,
        )
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(enrollment_from_row).transpose()
    }

    async fn list_enrollment_requests(&self) -> Result<Vec<EnrollmentRequestRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT request_id, status, client_id, request_payload, created_at, decided_at, rejection_reason
            FROM client_enrollment_requests
            ORDER BY created_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(enrollment_from_row).collect()
    }

    async fn approve_enrollment_request(&self, request_id: &str) -> Result<EnrollmentApproval> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            SELECT request_id, status, client_id, request_payload, created_at, decided_at, rejection_reason
            FROM client_enrollment_requests
            WHERE request_id = $1
            FOR UPDATE
            "#,
        )
        .bind(request_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StoreError::NotFound("enrollment request not found"))?;
        let record = enrollment_from_row(row)?;
        if record.status != EnrollmentStatus::Pending {
            return Err(StoreError::InvalidState(
                "enrollment request is not pending",
            ));
        }

        let client_id = format!("client-{}", new_id());
        let approved_at = now();
        sqlx::query(
            r#"
            UPDATE client_enrollment_requests
            SET status = 'approved', client_id = $2, decided_at = $3
            WHERE request_id = $1
            "#,
        )
        .bind(request_id)
        .bind(&client_id)
        .bind(approved_at)
        .execute(&mut *tx)
        .await?;

        let desired_state = DesiredState::default();
        sqlx::query(
            r#"
            INSERT INTO clients (client_id, public_key, desired_state, approved_at)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(&client_id)
        .bind(&record.request.public_key)
        .bind(serde_json::to_value(desired_state)?)
        .bind(approved_at)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(EnrollmentApproval {
            request_id: request_id.to_string(),
            client_id,
            approved_at,
        })
    }

    async fn reject_enrollment_request(
        &self,
        request_id: &str,
        reason: Option<String>,
    ) -> Result<EnrollmentRequestRecord> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            SELECT request_id, status, client_id, request_payload, created_at, decided_at, rejection_reason
            FROM client_enrollment_requests
            WHERE request_id = $1
            FOR UPDATE
            "#,
        )
        .bind(request_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StoreError::NotFound("enrollment request not found"))?;
        let record = enrollment_from_row(row)?;
        if record.status != EnrollmentStatus::Pending {
            return Err(StoreError::InvalidState(
                "enrollment request is not pending",
            ));
        }

        let decided_at = now();
        let row = sqlx::query(
            r#"
            UPDATE client_enrollment_requests
            SET status = 'rejected', decided_at = $2, rejection_reason = $3
            WHERE request_id = $1
            RETURNING request_id, status, client_id, request_payload, created_at, decided_at, rejection_reason
            "#,
        )
        .bind(request_id)
        .bind(decided_at)
        .bind(reason)
        .fetch_one(&mut *tx)
        .await?;
        let record = enrollment_from_row(row)?;
        tx.commit().await?;
        Ok(record)
    }

    async fn list_clients(&self) -> Result<Vec<AdminClientRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT c.client_id,
                   c.public_key,
                   c.desired_state,
                   c.approved_at,
                   c.revoked_at,
                   e.request_payload,
                   s.heartbeat_at,
                   s.telemetry_payload,
                   s.status_payload
            FROM clients c
            LEFT JOIN client_enrollment_requests e ON e.client_id = c.client_id
            LEFT JOIN client_runtime_status s ON s.client_id = c.client_id
            ORDER BY c.approved_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(admin_client_from_row).collect()
    }

    async fn get_client(&self, client_id: &str) -> Result<Option<ClientRecord>> {
        let row = sqlx::query(
            r#"
            SELECT client_id, public_key, desired_state, approved_at, revoked_at
            FROM clients
            WHERE client_id = $1
            "#,
        )
        .bind(client_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(client_from_row).transpose()
    }

    async fn create_challenge(&self, client_id: &str) -> Result<Option<Challenge>> {
        let Some(client) = self.get_client(client_id).await? else {
            return Ok(None);
        };
        if client.revoked_at.is_some() {
            return Ok(None);
        }
        let challenge = Challenge {
            challenge_id: new_id(),
            client_id: client_id.to_string(),
            nonce: new_id(),
            expires_at: now() + Duration::minutes(5),
        };
        sqlx::query(
            r#"
            INSERT INTO client_challenges (challenge_id, client_id, nonce, expires_at)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(&challenge.challenge_id)
        .bind(&challenge.client_id)
        .bind(&challenge.nonce)
        .bind(challenge.expires_at)
        .execute(&self.pool)
        .await?;
        Ok(Some(challenge))
    }

    async fn consume_challenge(&self, challenge_id: &str) -> Result<Option<Challenge>> {
        let row = sqlx::query(
            r#"
            DELETE FROM client_challenges
            WHERE challenge_id = $1
            RETURNING challenge_id, client_id, nonce, expires_at
            "#,
        )
        .bind(challenge_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(challenge_from_row).transpose()
    }

    async fn create_session(&self, token: SessionToken) -> Result<SessionToken> {
        sqlx::query(
            r#"
            INSERT INTO client_sessions (token, client_id, expires_at)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(&token.token)
        .bind(&token.client_id)
        .bind(token.expires_at)
        .execute(&self.pool)
        .await?;
        Ok(token)
    }

    async fn get_session(&self, token: &str) -> Result<Option<SessionToken>> {
        let row = sqlx::query(
            r#"
            SELECT token, client_id, expires_at
            FROM client_sessions
            WHERE token = $1
            "#,
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;
        row.map(session_from_row).transpose()
    }

    async fn revoke_client(&self, client_id: &str) -> Result<Option<ClientRecord>> {
        let mut tx = self.pool.begin().await?;
        let revoked_at = now();
        let row = sqlx::query(
            r#"
            UPDATE clients
            SET revoked_at = $2
            WHERE client_id = $1
            RETURNING client_id, public_key, desired_state, approved_at, revoked_at
            "#,
        )
        .bind(client_id)
        .bind(revoked_at)
        .fetch_optional(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM client_sessions WHERE client_id = $1")
            .bind(client_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        row.map(client_from_row).transpose()
    }

    async fn enqueue_command(
        &self,
        client_id: &str,
        command: ServerCommand,
    ) -> Result<Option<QueuedCommand>> {
        let Some(client) = self.get_client(client_id).await? else {
            return Ok(None);
        };
        if client.revoked_at.is_some() {
            return Ok(None);
        }
        let queued = QueuedCommand {
            command_id: new_id(),
            client_id: client_id.to_string(),
            command,
            created_at: now(),
        };
        sqlx::query(
            r#"
            INSERT INTO client_commands (command_id, client_id, command_payload, created_at)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(&queued.command_id)
        .bind(&queued.client_id)
        .bind(serde_json::to_value(&queued.command)?)
        .bind(queued.created_at)
        .execute(&self.pool)
        .await?;
        Ok(Some(queued))
    }

    async fn list_pending_commands(
        &self,
        client_id: &str,
        limit: u32,
    ) -> Result<Vec<QueuedCommand>> {
        let rows = sqlx::query(
            r#"
            SELECT command_id, client_id, command_payload, created_at
            FROM client_commands
            WHERE client_id = $1 AND delivered_at IS NULL
            ORDER BY created_at ASC
            LIMIT $2
            "#,
        )
        .bind(client_id)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(queued_command_from_row).collect()
    }

    async fn mark_command_delivered(&self, command_id: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE client_commands
            SET delivered_at = $2
            WHERE command_id = $1
            "#,
        )
        .bind(command_id)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_client_heartbeat(
        &self,
        client_id: &str,
        observed_at: chrono::DateTime<Utc>,
        telemetry: MachineTelemetry,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO client_runtime_status
              (client_id, heartbeat_at, telemetry_payload, updated_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (client_id) DO UPDATE
            SET heartbeat_at = EXCLUDED.heartbeat_at,
                telemetry_payload = EXCLUDED.telemetry_payload,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(client_id)
        .bind(observed_at)
        .bind(serde_json::to_value(telemetry)?)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_agent_status(&self, client_id: &str, status: AgentStatus) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO client_runtime_status
              (client_id, heartbeat_at, telemetry_payload, status_payload, updated_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (client_id) DO UPDATE
            SET heartbeat_at = EXCLUDED.heartbeat_at,
                telemetry_payload = EXCLUDED.telemetry_payload,
                status_payload = EXCLUDED.status_payload,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(client_id)
        .bind(status.telemetry.observed_at)
        .bind(serde_json::to_value(&status.telemetry)?)
        .bind(serde_json::to_value(status)?)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_job_assignment(&self, client_id: &str, job: PipelineJob) -> Result<JobRecord> {
        let assigned_at = now();
        let record = JobRecord {
            job_id: job.job_id.clone(),
            client_id: client_id.to_string(),
            job,
            state: JobState::Assigned,
            assigned_at,
            updated_at: assigned_at,
            completed_at: None,
        };
        sqlx::query(
            r#"
            INSERT INTO pipeline_jobs
              (job_id, client_id, job_payload, state, assigned_at, updated_at, completed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (job_id) DO UPDATE SET
              client_id = EXCLUDED.client_id,
              job_payload = EXCLUDED.job_payload,
              state = EXCLUDED.state,
              updated_at = EXCLUDED.updated_at,
              completed_at = EXCLUDED.completed_at
            "#,
        )
        .bind(&record.job_id)
        .bind(&record.client_id)
        .bind(serde_json::to_value(&record.job)?)
        .bind(job_state_to_string(&record.state)?)
        .bind(record.assigned_at)
        .bind(record.updated_at)
        .bind(record.completed_at)
        .execute(&self.pool)
        .await?;
        Ok(record)
    }

    async fn record_job_event(&self, client_id: &str, event: JobEvent) -> Result<()> {
        let received_at = now();
        let state = job_state_to_string(&event.state)?;
        let completed_at = is_terminal_job_state(&event.state).then_some(received_at);
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO job_events
              (event_id, job_id, client_id, event_payload, state, observed_at, received_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(new_id())
        .bind(&event.job_id)
        .bind(client_id)
        .bind(serde_json::to_value(&event)?)
        .bind(&state)
        .bind(event.observed_at)
        .bind(received_at)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            UPDATE pipeline_jobs
            SET state = $2, updated_at = $3, completed_at = COALESCE($4, completed_at)
            WHERE job_id = $1
            "#,
        )
        .bind(&event.job_id)
        .bind(state)
        .bind(received_at)
        .bind(completed_at)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn list_job_events(&self, job_id: Option<&str>, limit: u32) -> Result<Vec<JobEvent>> {
        let limit = i64::from(limit.clamp(1, 500));
        let rows = if let Some(job_id) = job_id {
            sqlx::query(
                r#"
                SELECT event_payload
                FROM job_events
                WHERE job_id = $1
                ORDER BY observed_at DESC
                LIMIT $2
                "#,
            )
            .bind(job_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"
                SELECT event_payload
                FROM job_events
                ORDER BY observed_at DESC
                LIMIT $1
                "#,
            )
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };
        rows.into_iter()
            .map(|row| {
                let event_payload: serde_json::Value = row.try_get("event_payload")?;
                Ok(serde_json::from_value(event_payload)?)
            })
            .collect()
    }

    async fn record_uploaded_artifact(
        &self,
        client_id: &str,
        artifact: UploadedArtifact,
    ) -> Result<()> {
        let size_bytes = artifact
            .size_bytes
            .and_then(|value| i64::try_from(value).ok());
        sqlx::query(
            r#"
            INSERT INTO uploaded_artifacts
              (artifact_id, job_id, client_id, filename, storage_uri, content_type, sha256, size_bytes, artifact_payload, uploaded_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (artifact_id) DO UPDATE SET
              job_id = EXCLUDED.job_id,
              client_id = EXCLUDED.client_id,
              filename = EXCLUDED.filename,
              storage_uri = EXCLUDED.storage_uri,
              content_type = EXCLUDED.content_type,
              sha256 = EXCLUDED.sha256,
              size_bytes = EXCLUDED.size_bytes,
              artifact_payload = EXCLUDED.artifact_payload,
              uploaded_at = EXCLUDED.uploaded_at
            "#,
        )
        .bind(&artifact.artifact_id)
        .bind(&artifact.job_id)
        .bind(client_id)
        .bind(&artifact.filename)
        .bind(&artifact.storage_uri)
        .bind(&artifact.content_type)
        .bind(&artifact.sha256)
        .bind(size_bytes)
        .bind(serde_json::to_value(&artifact)?)
        .bind(now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_uploaded_artifacts(&self) -> Result<Vec<UploadedArtifact>> {
        let rows = sqlx::query(
            r#"
            SELECT artifact_payload
            FROM uploaded_artifacts
            ORDER BY uploaded_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let artifact_payload: serde_json::Value = row.try_get("artifact_payload")?;
                Ok(serde_json::from_value(artifact_payload)?)
            })
            .collect()
    }

    async fn list_jobs(&self) -> Result<Vec<JobRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT job_id, client_id, job_payload, state, assigned_at, updated_at, completed_at
            FROM pipeline_jobs
            ORDER BY updated_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(job_from_row).collect()
    }
}

fn enrollment_from_row(row: PgRow) -> Result<EnrollmentRequestRecord> {
    let status: String = row.try_get("status")?;
    let request_payload: serde_json::Value = row.try_get("request_payload")?;
    Ok(EnrollmentRequestRecord {
        request_id: row.try_get("request_id")?,
        status: enrollment_status_from_str(&status)?,
        client_id: row.try_get("client_id")?,
        request: serde_json::from_value(request_payload)?,
        created_at: row.try_get("created_at")?,
        decided_at: row.try_get("decided_at")?,
        rejection_reason: row.try_get("rejection_reason")?,
    })
}

fn client_from_row(row: PgRow) -> Result<ClientRecord> {
    let desired_state: serde_json::Value = row.try_get("desired_state")?;
    Ok(ClientRecord {
        client_id: row.try_get("client_id")?,
        public_key: row.try_get("public_key")?,
        desired_state: serde_json::from_value(desired_state)?,
        approved_at: row.try_get("approved_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

fn admin_client_from_row(row: PgRow) -> Result<AdminClientRecord> {
    let desired_state: serde_json::Value = row.try_get("desired_state")?;
    let request_payload: Option<serde_json::Value> = row.try_get("request_payload")?;
    let telemetry_payload: Option<serde_json::Value> = row.try_get("telemetry_payload")?;
    let status_payload: Option<serde_json::Value> = row.try_get("status_payload")?;
    Ok(AdminClientRecord {
        client_id: row.try_get("client_id")?,
        public_key: row.try_get("public_key")?,
        desired_state: serde_json::from_value(desired_state)?,
        approved_at: row.try_get("approved_at")?,
        revoked_at: row.try_get("revoked_at")?,
        enrollment: request_payload.map(serde_json::from_value).transpose()?,
        latest_status: status_payload.map(serde_json::from_value).transpose()?,
        latest_telemetry: telemetry_payload.map(serde_json::from_value).transpose()?,
        last_heartbeat_at: row.try_get("heartbeat_at")?,
    })
}

fn challenge_from_row(row: PgRow) -> Result<Challenge> {
    Ok(Challenge {
        challenge_id: row.try_get("challenge_id")?,
        client_id: row.try_get("client_id")?,
        nonce: row.try_get("nonce")?,
        expires_at: row.try_get("expires_at")?,
    })
}

fn session_from_row(row: PgRow) -> Result<SessionToken> {
    Ok(SessionToken {
        token: row.try_get("token")?,
        client_id: row.try_get("client_id")?,
        expires_at: row.try_get("expires_at")?,
    })
}

fn queued_command_from_row(row: PgRow) -> Result<QueuedCommand> {
    let command_payload: serde_json::Value = row.try_get("command_payload")?;
    Ok(QueuedCommand {
        command_id: row.try_get("command_id")?,
        client_id: row.try_get("client_id")?,
        command: serde_json::from_value(command_payload)?,
        created_at: row.try_get("created_at")?,
    })
}

fn job_from_row(row: PgRow) -> Result<JobRecord> {
    let job_payload: serde_json::Value = row.try_get("job_payload")?;
    let state: String = row.try_get("state")?;
    Ok(JobRecord {
        job_id: row.try_get("job_id")?,
        client_id: row.try_get("client_id")?,
        job: serde_json::from_value(job_payload)?,
        state: job_state_from_string(&state)?,
        assigned_at: row.try_get("assigned_at")?,
        updated_at: row.try_get("updated_at")?,
        completed_at: row.try_get("completed_at")?,
    })
}

fn enrollment_status_as_str(status: &EnrollmentStatus) -> &'static str {
    match status {
        EnrollmentStatus::Pending => "pending",
        EnrollmentStatus::Approved => "approved",
        EnrollmentStatus::Rejected => "rejected",
    }
}

fn job_state_to_string(state: &JobState) -> Result<String> {
    let value = serde_json::to_value(state)?;
    value
        .as_str()
        .map(str::to_string)
        .ok_or(StoreError::InvalidState(
            "job state did not serialize to string",
        ))
}

fn job_state_from_string(value: &str) -> Result<JobState> {
    Ok(serde_json::from_value(serde_json::Value::String(
        value.to_string(),
    ))?)
}

fn is_terminal_job_state(state: &JobState) -> bool {
    matches!(
        state,
        JobState::Completed | JobState::Failed | JobState::Cancelled
    )
}

fn enrollment_status_from_str(value: &str) -> Result<EnrollmentStatus> {
    match value {
        "pending" => Ok(EnrollmentStatus::Pending),
        "approved" => Ok(EnrollmentStatus::Approved),
        "rejected" => Ok(EnrollmentStatus::Rejected),
        other => Err(StoreError::InvalidEnrollmentStatus(other.to_string())),
    }
}

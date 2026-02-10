use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, RwLock};

use crate::auth::AuthService;
use crate::error::{Error, Result};
use crate::proto::google::spanner::v1 as pb;

/// Type alias for the gRPC client used throughout the crate.
pub(crate) type GrpcClient =
    pb::spanner_client::SpannerClient<AuthService<tonic::transport::Channel>>;

const SESSION_ROTATION_INTERVAL: Duration = Duration::from_secs(6 * 24 * 60 * 60);
const SESSION_ROTATION_RETRY_INTERVAL: Duration = Duration::from_secs(5 * 60);

struct SessionSlot {
    name: String,
    in_flight: AtomicUsize,
    drained: Notify,
}

impl SessionSlot {
    fn new(name: String) -> Self {
        Self {
            name,
            in_flight: AtomicUsize::new(0),
            drained: Notify::new(),
        }
    }

    fn acquire(self: &Arc<Self>) -> SessionLease {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        SessionLease { slot: self.clone() }
    }
}

struct SessionPool {
    slots: Vec<Arc<SessionSlot>>,
}

impl SessionPool {
    fn from_names(names: Vec<String>) -> Self {
        let slots = names
            .into_iter()
            .map(|name| Arc::new(SessionSlot::new(name)))
            .collect();
        Self { slots }
    }

    fn lease_random(&self) -> (usize, SessionLease) {
        debug_assert!(!self.slots.is_empty(), "session pool must be non-empty");
        let idx = fastrand::usize(..self.slots.len());
        (idx, self.slots[idx].acquire())
    }
}

pub(crate) struct SessionLease {
    slot: Arc<SessionSlot>,
}

impl SessionLease {
    pub(crate) fn name(&self) -> &str {
        &self.slot.name
    }
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        let prev = self.slot.in_flight.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(
            prev > 0,
            "session lease counter underflow for {}",
            self.slot.name
        );
        if prev == 1 {
            self.slot.drained.notify_waiters();
        }
    }
}

async fn wait_for_slot_drain(slot: &Arc<SessionSlot>) {
    let notified = slot.drained.notified();
    tokio::pin!(notified);

    loop {
        notified.as_mut().enable();
        if slot.in_flight.load(Ordering::SeqCst) == 0 {
            return;
        }
        notified.as_mut().await;
        notified.set(slot.drained.notified());
    }
}

async fn wait_for_pool_drain(pool: &Arc<SessionPool>) {
    for slot in &pool.slots {
        wait_for_slot_drain(slot).await;
    }
}

/// Manages a pool of multiplexed Spanner sessions.
///
/// A background task rotates the whole pool every 6 days (before the 7-day
/// server expiry), then drains and deletes the previous pool.
pub(crate) struct SessionManager {
    current: Arc<RwLock<Arc<SessionPool>>>,
    clients: Arc<Vec<GrpcClient>>,
    rotation_handle: tokio::task::JoinHandle<()>,
}

impl SessionManager {
    /// Create pooled multiplexed sessions and start background rotation.
    pub(crate) async fn new(clients: Vec<GrpcClient>, database: &str) -> Result<Self> {
        if clients.is_empty() {
            return Err(Error::Auth("pool_size must be greater than 0".to_string()));
        }

        let clients = Arc::new(clients);
        let initial_pool = Arc::new(Self::create_pool(clients.as_ref(), database).await?);
        let current = Arc::new(RwLock::new(initial_pool));

        let rotation_current = current.clone();
        let rotation_clients = clients.clone();
        let rotation_db = database.to_string();

        let handle = tokio::spawn(async move {
            // Rotate every 6 days (before 7-day server expiry), but retry
            // failed rotations quickly to avoid waiting another full interval.
            let mut next_delay = SESSION_ROTATION_INTERVAL;
            loop {
                tokio::time::sleep(next_delay).await;
                match Self::create_pool(rotation_clients.as_ref(), &rotation_db).await {
                    Ok(new_pool) => {
                        let new_pool = Arc::new(new_pool);
                        let old_pool = {
                            let mut current_guard = rotation_current.write().await;
                            let old_pool = current_guard.clone();
                            *current_guard = new_pool.clone();
                            old_pool
                        };

                        tracing::info!(
                            pool_size = new_pool.slots.len(),
                            "rotated multiplexed session pool"
                        );

                        wait_for_pool_drain(&old_pool).await;
                        for (idx, slot) in old_pool.slots.iter().enumerate() {
                            let mut delete_client = rotation_clients[idx].clone();
                            match Self::delete_session(&mut delete_client, &slot.name).await {
                                Ok(()) => {
                                    tracing::info!(session = %slot.name, "deleted drained multiplexed session");
                                }
                                Err(e) => {
                                    tracing::error!(
                                        error = %e,
                                        session = %slot.name,
                                        "failed to delete drained multiplexed session"
                                    );
                                }
                            }
                        }
                        next_delay = SESSION_ROTATION_INTERVAL;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to rotate multiplexed session");
                        next_delay = SESSION_ROTATION_RETRY_INTERVAL;
                    }
                }
            }
        });

        Ok(Self {
            current,
            clients,
            rotation_handle: handle,
        })
    }

    /// Acquire a lease for the current session.
    ///
    /// The lease keeps a randomly selected session alive for the duration of an in-flight query.
    pub(crate) async fn lease(&self) -> (SessionLease, GrpcClient) {
        let pool = self.current.read().await.clone();
        debug_assert_eq!(
            pool.slots.len(),
            self.clients.len(),
            "session pool size must match transport pool size"
        );
        let (idx, lease) = pool.lease_random();
        (lease, self.clients[idx].clone())
    }

    async fn create_pool(clients: &[GrpcClient], database: &str) -> Result<SessionPool> {
        let mut names = Vec::with_capacity(clients.len());
        for client in clients {
            let mut client = client.clone();
            names.push(Self::create_session(&mut client, database).await?);
        }
        Ok(SessionPool::from_names(names))
    }

    async fn create_session(client: &mut GrpcClient, database: &str) -> Result<String> {
        let resp = client
            .create_session(pb::CreateSessionRequest {
                database: database.to_string(),
                session: Some(pb::Session {
                    multiplexed: true,
                    ..Default::default()
                }),
            })
            .await
            .map_err(Error::Status)?;

        Ok(resp.into_inner().name)
    }

    async fn delete_session(client: &mut GrpcClient, session_name: &str) -> Result<()> {
        match client
            .delete_session(pb::DeleteSessionRequest {
                name: session_name.to_string(),
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(()),
            Err(status) => Err(Error::Status(status)),
        }
    }
}

impl Drop for SessionManager {
    fn drop(&mut self) {
        self.rotation_handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[tokio::test]
    async fn wait_for_slot_drain_waits_until_lease_drop() {
        let slot = Arc::new(SessionSlot::new("sessions/test".to_string()));
        let lease = slot.acquire();

        let early =
            tokio::time::timeout(Duration::from_millis(20), wait_for_slot_drain(&slot)).await;
        assert!(early.is_err(), "drain should block while lease is held");

        drop(lease);

        tokio::time::timeout(Duration::from_millis(200), wait_for_slot_drain(&slot))
            .await
            .expect("drain should complete after lease drop");
    }

    #[tokio::test]
    async fn wait_for_pool_drain_waits_until_all_leases_drop() {
        let pool = Arc::new(SessionPool::from_names(vec![
            "sessions/one".to_string(),
            "sessions/two".to_string(),
        ]));
        let lease_one = pool.slots[0].acquire();
        let lease_two = pool.slots[1].acquire();

        let early =
            tokio::time::timeout(Duration::from_millis(20), wait_for_pool_drain(&pool)).await;
        assert!(
            early.is_err(),
            "pool drain should block while leases are held"
        );

        drop(lease_one);
        let mid = tokio::time::timeout(Duration::from_millis(20), wait_for_pool_drain(&pool)).await;
        assert!(
            mid.is_err(),
            "pool drain should still block while one lease is still held"
        );

        drop(lease_two);
        tokio::time::timeout(Duration::from_millis(200), wait_for_pool_drain(&pool))
            .await
            .expect("pool drain should complete after all leases drop");
    }

    #[tokio::test]
    async fn lease_random_returns_pool_session() {
        let pool = Arc::new(SessionPool::from_names(vec![
            "sessions/a".to_string(),
            "sessions/b".to_string(),
            "sessions/c".to_string(),
        ]));
        let allowed = HashSet::from(["sessions/a", "sessions/b", "sessions/c"]);

        for _ in 0..64 {
            let (_, lease) = pool.lease_random();
            assert!(
                allowed.contains(lease.name()),
                "unexpected lease {}",
                lease.name()
            );
            drop(lease);
        }
    }
}

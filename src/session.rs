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

/// Manages a single multiplexed Spanner session.
///
/// Multiplexed sessions support unlimited concurrent transactions on a single session.
/// A background task rotates the session every 6 days (before the 7-day server
/// expiry), then drains and deletes the previous session.
pub(crate) struct SessionManager {
    current: Arc<RwLock<Arc<SessionSlot>>>,
    rotation_handle: tokio::task::JoinHandle<()>,
}

impl SessionManager {
    /// Create a multiplexed session and start background rotation.
    pub(crate) async fn new(client: &mut GrpcClient, database: &str) -> Result<Self> {
        let name = Self::create_session(client, database).await?;
        let current = Arc::new(RwLock::new(Arc::new(SessionSlot::new(name))));

        let rotation_current = current.clone();
        let mut rotation_client = client.clone();
        let rotation_db = database.to_string();

        let handle = tokio::spawn(async move {
            // Rotate every 6 days (before 7-day server expiry), but retry
            // failed rotations quickly to avoid waiting another full interval.
            let mut next_delay = SESSION_ROTATION_INTERVAL;
            loop {
                tokio::time::sleep(next_delay).await;
                match Self::create_session(&mut rotation_client, &rotation_db).await {
                    Ok(new_name) => {
                        let new_slot = Arc::new(SessionSlot::new(new_name.clone()));
                        let old_slot = {
                            let mut current_guard = rotation_current.write().await;
                            let old_slot = current_guard.clone();
                            *current_guard = new_slot;
                            old_slot
                        };

                        tracing::info!(
                            new_session = %new_name,
                            old_session = %old_slot.name,
                            "rotated multiplexed session"
                        );

                        wait_for_slot_drain(&old_slot).await;
                        match Self::delete_session(&mut rotation_client, &old_slot.name).await {
                            Ok(()) => {
                                tracing::info!(session = %old_slot.name, "deleted drained multiplexed session");
                            }
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    session = %old_slot.name,
                                    "failed to delete drained multiplexed session"
                                );
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
            rotation_handle: handle,
        })
    }

    /// Acquire a lease for the current session.
    ///
    /// The lease keeps the session alive for the duration of an in-flight query.
    pub(crate) async fn lease(&self) -> SessionLease {
        let guard = self.current.read().await;
        guard.acquire()
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
}

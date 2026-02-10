use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_core::Stream;
use serde::de::DeserializeOwned;

use crate::auth::{AuthService, TokenSource};
use crate::error::{Error, Result};
use crate::params::{ToSpanner, params_to_proto};
use crate::proto::google::spanner::v1 as pb;
use crate::result::StreamingAssembler;
use crate::session::SessionManager;

/// Timestamp bound for read-only operations.
#[derive(Debug, Clone, Copy)]
pub enum TimestampBound {
    /// Read at the latest committed data.
    Strong,
    /// Read data at a timestamp that is at most `duration` old.
    ExactStaleness(std::time::Duration),
    /// Read data at the given timestamp.
    ReadTimestamp(prost_types::Timestamp),
}

impl TimestampBound {
    fn to_proto(self) -> pb::transaction_options::read_only::TimestampBound {
        match self {
            TimestampBound::Strong => {
                pb::transaction_options::read_only::TimestampBound::Strong(true)
            }
            TimestampBound::ExactStaleness(d) => {
                pb::transaction_options::read_only::TimestampBound::ExactStaleness(
                    prost_types::Duration {
                        seconds: d.as_secs() as i64,
                        nanos: d.subsec_nanos() as i32,
                    },
                )
            }
            TimestampBound::ReadTimestamp(ts) => {
                pb::transaction_options::read_only::TimestampBound::ReadTimestamp(ts)
            }
        }
    }
}

/// Request priority hint for Spanner operations.
#[derive(Debug, Clone, Copy)]
pub enum RequestPriority {
    Low,
    Medium,
    High,
}

impl RequestPriority {
    fn to_proto(self) -> i32 {
        match self {
            RequestPriority::Low => pb::request_options::Priority::Low as i32,
            RequestPriority::Medium => pb::request_options::Priority::Medium as i32,
            RequestPriority::High => pb::request_options::Priority::High as i32,
        }
    }
}

fn read_only_options(bound: TimestampBound) -> pb::TransactionOptions {
    pb::TransactionOptions {
        mode: Some(pb::transaction_options::Mode::ReadOnly(
            pb::transaction_options::ReadOnly {
                return_read_timestamp: true,
                timestamp_bound: Some(bound.to_proto()),
            },
        )),
        ..Default::default()
    }
}

fn read_write_options() -> pb::TransactionOptions {
    pb::TransactionOptions {
        mode: Some(pb::transaction_options::Mode::ReadWrite(
            pb::transaction_options::ReadWrite::default(),
        )),
        ..Default::default()
    }
}

fn make_request_options(
    request_tag: Option<&str>,
    priority: Option<RequestPriority>,
) -> Option<pb::RequestOptions> {
    if request_tag.is_none() && priority.is_none() {
        return None;
    }
    Some(pb::RequestOptions {
        request_tag: request_tag.unwrap_or_default().to_string(),
        priority: priority.map_or(0, |p| p.to_proto()),
        ..Default::default()
    })
}

const DEFAULT_ENDPOINT: &str = "https://spanner.googleapis.com";
const READ_ONLY_DEADLINE_EXCEEDED_MSG: &str = "read-only query deadline exceeded";
const READ_WRITE_DEADLINE_EXCEEDED_MSG: &str = "read-write query deadline exceeded";

fn endpoint_uses_tls(endpoint: &str) -> Result<bool> {
    let uri: http::Uri = endpoint
        .parse()
        .map_err(|e| Error::Auth(format!("invalid endpoint URI `{endpoint}`: {e}")))?;

    match uri.scheme_str() {
        Some("https") => Ok(true),
        Some("http") => Ok(false),
        Some(other) => Err(Error::Auth(format!(
            "unsupported endpoint scheme `{other}`; use `http://` or `https://`"
        ))),
        None => Err(Error::Auth(
            "endpoint must include scheme (`http://` or `https://`)".to_string(),
        )),
    }
}

fn remaining_timeout(
    deadline: Instant,
    exceeded_msg: &'static str,
) -> std::result::Result<Duration, tonic::Status> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(tonic::Status::deadline_exceeded(exceeded_msg))
    } else {
        Ok(remaining)
    }
}

fn choose_precommit_token(
    left: Option<pb::MultiplexedSessionPrecommitToken>,
    right: Option<pb::MultiplexedSessionPrecommitToken>,
) -> Option<pb::MultiplexedSessionPrecommitToken> {
    match (left, right) {
        (Some(a), Some(b)) => {
            if a.seq_num >= b.seq_num {
                Some(a)
            } else {
                Some(b)
            }
        }
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

/// Result for a one-shot read-write DML statement.
#[derive(Debug, Clone)]
pub struct ReadWriteResult {
    pub affected_rows: i64,
    pub commit_timestamp: prost_types::Timestamp,
}

/// Configuration for creating a [`Client`].
pub struct ClientConfig<T: TokenSource> {
    database: String,
    token_source: T,
    endpoint: String,
}

impl<T: TokenSource> ClientConfig<T> {
    /// Create a config with defaults.
    pub fn new(database: impl Into<String>, token_source: T) -> Self {
        Self {
            database: database.into(),
            token_source,
            endpoint: DEFAULT_ENDPOINT.to_string(),
        }
    }

    /// Override the gRPC endpoint (for example, emulator/local endpoint).
    ///
    /// Endpoints with `https://` use TLS. Endpoints with `http://` use plaintext.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }
}

/// A Cloud Spanner client for high-throughput one-shot reads.
///
/// Uses a single multiplexed session (auto-rotated before expiry).
/// All queries use single-use read-only transactions (one RPC per query).
#[derive(Clone)]
pub struct Client {
    client: pb::spanner_client::SpannerClient<AuthService<tonic::transport::Channel>>,
    session: Arc<SessionManager>,
}

impl Client {
    /// Connect to Spanner and create a multiplexed session.
    pub async fn new<T: TokenSource>(config: ClientConfig<T>) -> Result<Self> {
        let ClientConfig {
            database,
            token_source,
            endpoint,
        } = config;

        let use_tls = endpoint_uses_tls(&endpoint)?;
        let endpoint = tonic::transport::Endpoint::from_shared(endpoint)
            .map_err(|e| Error::Auth(e.to_string()))?;
        let endpoint = if use_tls {
            endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new().with_webpki_roots())
                .map_err(Error::Transport)?
        } else {
            endpoint
        };
        let channel = endpoint.connect().await.map_err(Error::Transport)?;

        let auth_channel = AuthService::new(channel, token_source);
        let mut grpc_client = pb::spanner_client::SpannerClient::new(auth_channel);
        let session = SessionManager::new(&mut grpc_client, &database).await?;

        Ok(Self {
            client: grpc_client,
            session: Arc::new(session),
        })
    }

    /// Start building a read-only query.
    pub fn read_only(&self) -> ReadOnlyBuilder<'_> {
        ReadOnlyBuilder {
            client: self,
            bound: TimestampBound::Strong,
            request_tag: None,
            priority: None,
            deadline: (),
        }
    }

    /// Start building a one-shot read-write DML statement.
    pub fn read_write(&self) -> ReadWriteBuilder<'_> {
        ReadWriteBuilder {
            client: self,
            priority: None,
            deadline: (),
        }
    }

    fn execute_query<T: DeserializeOwned + 'static>(
        &self,
        sql: String,
        param_struct: prost_types::Struct,
        param_types: std::collections::HashMap<String, pb::Type>,
        bound: TimestampBound,
        request_options: Option<pb::RequestOptions>,
        deadline: Instant,
    ) -> Result<impl Stream<Item = Result<T>>> {
        let session_manager = self.session.clone();
        let mut client = self.client.clone();
        let options = read_only_options(bound);

        Ok(async_stream::try_stream! {
            let session_lease = session_manager.lease().await;
            let session_name = session_lease.name().to_string();
            let _session_lease = session_lease;

            let req = pb::ExecuteSqlRequest {
                session: session_name,
                transaction: Some(pb::TransactionSelector {
                    selector: Some(pb::transaction_selector::Selector::SingleUse(options)),
                }),
                sql,
                params: Some(param_struct),
                param_types,
                request_options,
                ..Default::default()
            };

            let mut request = tonic::Request::new(req);
            request.set_timeout(
                remaining_timeout(deadline, READ_ONLY_DEADLINE_EXCEEDED_MSG).map_err(Error::Status)?,
            );
            let response = client.execute_streaming_sql(request).await.map_err(Error::Status)?;
            let mut stream = response.into_inner();
            let mut assembler = StreamingAssembler::new();

            while let Some(partial) = stream.message().await.map_err(Error::Status)? {
                for row in assembler.push(partial)? {
                    yield row.deserialize()?;
                }
            }
        })
    }

    async fn execute_dml(
        &self,
        sql: String,
        param_struct: prost_types::Struct,
        param_types: std::collections::HashMap<String, pb::Type>,
        priority: Option<RequestPriority>,
        deadline: Instant,
    ) -> Result<ReadWriteResult> {
        let mut client = self.client.clone();
        let session_lease = self.session.lease().await;
        let session_name = session_lease.name().to_string();
        let _session_lease = session_lease;
        let request_options = make_request_options(None, priority);

        let mut execute_request = tonic::Request::new(pb::ExecuteSqlRequest {
            session: session_name.clone(),
            transaction: Some(pb::TransactionSelector {
                selector: Some(pb::transaction_selector::Selector::Begin(
                    read_write_options(),
                )),
            }),
            sql,
            params: Some(param_struct),
            param_types,
            seqno: 1,
            request_options: request_options.clone(),
            ..Default::default()
        });
        execute_request.set_timeout(
            remaining_timeout(deadline, READ_WRITE_DEADLINE_EXCEEDED_MSG).map_err(Error::Status)?,
        );
        let execute_result = client
            .execute_sql(execute_request)
            .await
            .map_err(Error::Status)?
            .into_inner();

        let tx = execute_result
            .metadata
            .and_then(|meta| meta.transaction)
            .ok_or_else(|| {
                Error::Status(tonic::Status::internal(
                    "missing transaction metadata in DML ExecuteSql response",
                ))
            })?;
        if tx.id.is_empty() {
            return Err(Error::Status(tonic::Status::internal(
                "missing transaction id in DML ExecuteSql response",
            )));
        }

        let affected_rows = match execute_result.stats.and_then(|stats| stats.row_count) {
            Some(pb::result_set_stats::RowCount::RowCountExact(rows)) => rows,
            Some(pb::result_set_stats::RowCount::RowCountLowerBound(rows)) => rows,
            None => {
                return Err(Error::Status(tonic::Status::internal(
                    "missing row count in DML ExecuteSql response",
                )));
            }
        };

        let commit_precommit_token =
            choose_precommit_token(execute_result.precommit_token, tx.precommit_token);
        let mut commit_request = tonic::Request::new(pb::CommitRequest {
            session: session_name,
            request_options,
            precommit_token: commit_precommit_token,
            transaction: Some(pb::commit_request::Transaction::TransactionId(tx.id)),
            ..Default::default()
        });
        commit_request.set_timeout(
            remaining_timeout(deadline, READ_WRITE_DEADLINE_EXCEEDED_MSG).map_err(Error::Status)?,
        );
        let commit_response = client
            .commit(commit_request)
            .await
            .map_err(Error::Status)?
            .into_inner();
        let commit_timestamp = commit_response.commit_timestamp.ok_or_else(|| {
            Error::Status(tonic::Status::internal(
                "missing commit timestamp in Commit response",
            ))
        })?;

        Ok(ReadWriteResult {
            affected_rows,
            commit_timestamp,
        })
    }
}

/// Builder for read-only queries.
pub struct ReadOnlyQueryBuilder<'a, D> {
    client: &'a Client,
    bound: TimestampBound,
    request_tag: Option<String>,
    priority: Option<RequestPriority>,
    deadline: D,
}

pub type ReadOnlyBuilder<'a> = ReadOnlyQueryBuilder<'a, ()>;

impl<'a, D> ReadOnlyQueryBuilder<'a, D> {
    pub fn with_request_tag(mut self, tag: impl Into<String>) -> Self {
        self.request_tag = Some(tag.into());
        self
    }

    pub fn with_priority(mut self, p: RequestPriority) -> Self {
        self.priority = Some(p);
        self
    }

    pub fn with_timestamp_bound(mut self, b: TimestampBound) -> Self {
        self.bound = b;
        self
    }

    pub fn with_timeout(self, timeout: Duration) -> ReadOnlyQueryBuilder<'a, Instant> {
        let Self {
            client,
            bound,
            request_tag,
            priority,
            ..
        } = self;
        ReadOnlyQueryBuilder {
            client,
            bound,
            request_tag,
            priority,
            deadline: Instant::now() + timeout,
        }
    }
}

impl<'a> ReadOnlyQueryBuilder<'a, Instant> {
    /// Execute a one-shot read-only SQL query.
    ///
    /// Uses a single-use read-only transaction (one streaming RPC).
    pub fn run<T: DeserializeOwned + 'static>(
        self,
        sql: &str,
        params: &[(&str, &dyn ToSpanner)],
    ) -> Result<impl Stream<Item = Result<T>>> {
        let (param_struct, param_types) = params_to_proto(params);
        let request_options = make_request_options(self.request_tag.as_deref(), self.priority);
        self.client.execute_query(
            sql.to_string(),
            param_struct,
            param_types,
            self.bound,
            request_options,
            self.deadline,
        )
    }
}

/// Builder for one-shot read-write DML statements.
pub struct ReadWriteBuilder<'a, D = ()> {
    client: &'a Client,
    priority: Option<RequestPriority>,
    deadline: D,
}

impl<'a, D> ReadWriteBuilder<'a, D> {
    pub fn with_priority(mut self, p: RequestPriority) -> Self {
        self.priority = Some(p);
        self
    }

    pub fn with_timeout(self, timeout: Duration) -> ReadWriteBuilder<'a, Instant> {
        let Self {
            client, priority, ..
        } = self;
        ReadWriteBuilder {
            client,
            priority,
            deadline: Instant::now() + timeout,
        }
    }
}

impl<'a> ReadWriteBuilder<'a, Instant> {
    /// Execute a one-shot SQL DML statement inside a read-write transaction.
    ///
    /// This uses two RPCs:
    /// - `ExecuteSql` with transaction selector `Begin(read_write)`
    /// - `Commit` with the returned transaction id
    pub async fn run(
        self,
        sql: &str,
        params: &[(&str, &dyn ToSpanner)],
    ) -> Result<ReadWriteResult> {
        let (param_struct, param_types) = params_to_proto(params);
        self.client
            .execute_dml(
                sql.to_string(),
                param_struct,
                param_types,
                self.priority,
                self.deadline,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_options_is_strong() {
        let options = read_only_options(TimestampBound::Strong);
        let mode = options.mode.expect("read-only mode should exist");
        let pb::transaction_options::Mode::ReadOnly(read_only) = mode else {
            panic!("expected read-only mode");
        };
        let pb = read_only
            .timestamp_bound
            .expect("timestamp bound should be present");
        assert!(matches!(
            pb,
            pb::transaction_options::read_only::TimestampBound::Strong(true)
        ));
    }

    #[test]
    fn read_only_options_exact_staleness() {
        let options = read_only_options(TimestampBound::ExactStaleness(
            std::time::Duration::from_secs(15),
        ));
        let mode = options.mode.expect("read-only mode should exist");
        let pb::transaction_options::Mode::ReadOnly(read_only) = mode else {
            panic!("expected read-only mode");
        };
        let bound = read_only
            .timestamp_bound
            .expect("timestamp bound should be present");
        match bound {
            pb::transaction_options::read_only::TimestampBound::ExactStaleness(d) => {
                assert_eq!(d.seconds, 15);
                assert_eq!(d.nanos, 0);
            }
            other => panic!("expected ExactStaleness, got {other:?}"),
        }
    }

    #[test]
    fn read_only_options_read_timestamp() {
        let ts = prost_types::Timestamp {
            seconds: 1700000000,
            nanos: 42,
        };
        let options = read_only_options(TimestampBound::ReadTimestamp(ts));
        let mode = options.mode.expect("read-only mode should exist");
        let pb::transaction_options::Mode::ReadOnly(read_only) = mode else {
            panic!("expected read-only mode");
        };
        let bound = read_only
            .timestamp_bound
            .expect("timestamp bound should be present");
        match bound {
            pb::transaction_options::read_only::TimestampBound::ReadTimestamp(got) => {
                assert_eq!(got, ts);
            }
            other => panic!("expected ReadTimestamp, got {other:?}"),
        }
    }

    #[test]
    fn read_write_options_is_read_write() {
        let options = read_write_options();
        let mode = options.mode.expect("read-write mode should exist");
        let pb::transaction_options::Mode::ReadWrite(_) = mode else {
            panic!("expected read-write mode");
        };
    }

    #[test]
    fn endpoint_scheme_tls_enabled_for_https() {
        assert!(
            endpoint_uses_tls("https://spanner.googleapis.com").expect("endpoint should parse")
        );
    }

    #[test]
    fn endpoint_scheme_tls_disabled_for_http() {
        assert!(!endpoint_uses_tls("http://127.0.0.1:9010").expect("endpoint should parse"));
    }

    #[test]
    fn endpoint_scheme_rejects_missing_scheme() {
        let err = endpoint_uses_tls("spanner.googleapis.com:443")
            .expect_err("missing scheme should fail");
        assert!(
            matches!(err, Error::Auth(_)),
            "expected auth error for endpoint scheme validation"
        );
    }

    #[test]
    fn remaining_timeout_deadline_already_exceeded() {
        let err = remaining_timeout(Instant::now(), READ_ONLY_DEADLINE_EXCEEDED_MSG)
            .expect_err("expired deadline should return deadline-exceeded status");
        assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
    }
}

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{TryStreamExt, pin_mut};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;
use spanlite::{Client, ClientConfig, RequestPriority, TimestampBound, TokenSource};
use testcontainers::{
    GenericImage,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

const EMULATOR_IMAGE: &str = "gcr.io/cloud-spanner-emulator/emulator";
const EMULATOR_TAG: &str = "1.5.6";
const EMULATOR_TAG_ENV: &str = "SPANNER_EMULATOR_TAG";
const PROJECT_ID: &str = "spanlite-test-project";

type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone, Copy)]
struct StaticTokenSource;

impl TokenSource for StaticTokenSource {
    fn token(
        &self,
    ) -> impl std::future::Future<
        Output = std::result::Result<String, Box<dyn std::error::Error + Send + Sync>>,
    > + Send {
        std::future::ready(Ok("emulator-token".to_string()))
    }
}

#[derive(Debug, Deserialize)]
struct IntRow {
    n: i64,
}

#[derive(Debug, Deserialize)]
struct ItemRow {
    id: i64,
    value: String,
}

#[derive(Debug, Deserialize)]
struct Operation {
    name: String,
    done: Option<bool>,
    error: Option<RpcStatus>,
}

#[derive(Debug, Deserialize)]
struct RpcStatus {
    code: i32,
    message: String,
}

#[derive(Clone, Copy)]
struct DatabaseRef<'a> {
    project: &'a str,
    instance: &'a str,
    database: &'a str,
}

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .args(["info", "--format", "{{.ServerVersion}}"])
        .output()
        .is_ok_and(|output| output.status.success())
}

fn short_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut suffix = format!("{nanos:x}");
    if suffix.len() > 8 {
        suffix = suffix[suffix.len() - 8..].to_string();
    }
    suffix
}

fn emulator_tag() -> String {
    std::env::var(EMULATOR_TAG_ENV).unwrap_or_else(|_| EMULATOR_TAG.to_string())
}

async fn post_operation_with_retry(
    http: &reqwest::Client,
    url: &str,
    payload: serde_json::Value,
) -> TestResult<Operation> {
    let mut last_err = String::new();
    for _ in 0..80 {
        match http.post(url).json(&payload).send().await {
            Ok(resp) if resp.status().is_success() => {
                let op = resp.json::<Operation>().await?;
                return Ok(op);
            }
            Ok(resp) if resp.status() == StatusCode::CONFLICT => {
                return Ok(Operation {
                    name: String::new(),
                    done: Some(true),
                    error: None,
                });
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                if matches!(
                    status,
                    StatusCode::NOT_FOUND
                        | StatusCode::BAD_GATEWAY
                        | StatusCode::SERVICE_UNAVAILABLE
                        | StatusCode::INTERNAL_SERVER_ERROR
                ) {
                    last_err = format!("transient status {status}: {body}");
                } else {
                    return Err(format!("request failed with status {status}: {body}").into());
                }
            }
            Err(err) => {
                last_err = err.to_string();
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("request did not succeed after retries: {last_err}").into())
}

async fn wait_for_operation(
    http: &reqwest::Client,
    rest_base: &str,
    operation: Operation,
) -> TestResult<()> {
    if operation.done.unwrap_or(false) {
        if let Some(err) = operation.error {
            return Err(format!("operation failed ({}) {}", err.code, err.message).into());
        }
        return Ok(());
    }

    if operation.name.is_empty() {
        return Ok(());
    }

    let operation_url = format!("{rest_base}/v1/{}", operation.name);
    for _ in 0..120 {
        let resp = http.get(&operation_url).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("operation poll failed with status {status}: {body}").into());
        }
        let op = resp.json::<Operation>().await?;
        if op.done.unwrap_or(false) {
            if let Some(err) = op.error {
                return Err(format!("operation failed ({}) {}", err.code, err.message).into());
            }
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err(format!("timed out waiting for operation {}", operation.name).into())
}

async fn bootstrap_database(rest_base: &str, db: DatabaseRef<'_>) -> TestResult<()> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;

    let create_instance_url = format!("{rest_base}/v1/projects/{}/instances", db.project);
    let create_instance_payload = json!({
        "instanceId": db.instance,
        "instance": {
            "name": format!("projects/{}/instances/{}", db.project, db.instance),
            "config": format!("projects/{}/instanceConfigs/emulator-config", db.project),
            "displayName": db.instance,
            "nodeCount": 1
        }
    });
    let op =
        post_operation_with_retry(&http, &create_instance_url, create_instance_payload).await?;
    wait_for_operation(&http, rest_base, op).await?;

    let create_database_url = format!(
        "{rest_base}/v1/projects/{}/instances/{}/databases",
        db.project, db.instance
    );
    let create_database_payload = json!({
        "createStatement": format!("CREATE DATABASE `{}`", db.database),
        "extraStatements": [
            "CREATE TABLE items (id INT64 NOT NULL, value STRING(MAX)) PRIMARY KEY (id)"
        ]
    });
    let op =
        post_operation_with_retry(&http, &create_database_url, create_database_payload).await?;
    wait_for_operation(&http, rest_base, op).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn one_shot_query_against_emulator() -> TestResult<()> {
    if !docker_available() {
        eprintln!("skipping emulator integration test: Docker is unavailable");
        return Ok(());
    }

    let tag = emulator_tag();
    let container = GenericImage::new(EMULATOR_IMAGE, tag.as_str())
        .with_exposed_port(9010.tcp())
        .with_exposed_port(9020.tcp())
        .with_wait_for(WaitFor::millis(200))
        .start()
        .await?;

    let host = container.get_host().await?;
    let grpc_port = container.get_host_port_ipv4(9010.tcp()).await?;
    let rest_port = container.get_host_port_ipv4(9020.tcp()).await?;
    let grpc_endpoint = format!("http://{host}:{grpc_port}");
    let rest_endpoint = format!("http://{host}:{rest_port}");

    let suffix = short_suffix();
    let instance_id = format!("inst-{suffix}");
    let database_id = format!("db-{suffix}");
    let db = DatabaseRef {
        project: PROJECT_ID,
        instance: &instance_id,
        database: &database_id,
    };
    bootstrap_database(&rest_endpoint, db).await?;

    let db_path = format!(
        "projects/{}/instances/{}/databases/{}",
        db.project, db.instance, db.database
    );
    let config = ClientConfig::new(db_path, StaticTokenSource)
        .with_endpoint(grpc_endpoint)
        .with_session_pool_size(4);
    let client = Client::new(config).await?;

    // Strong read
    let stream = client
        .read_only()
        .with_timeout(Duration::from_secs(10))
        .run::<IntRow>("SELECT 1 AS n", &[])?;
    pin_mut!(stream);
    let row = stream.try_next().await?.expect("expected one row");
    assert_eq!(row.n, 1);
    assert!(stream.try_next().await?.is_none());

    // Stale read
    let stream = client
        .read_only()
        .with_timestamp_bound(TimestampBound::ExactStaleness(Duration::from_millis(1)))
        .with_timeout(Duration::from_secs(10))
        .run::<IntRow>("SELECT 2 AS n", &[])?;
    pin_mut!(stream);
    let row = stream.try_next().await?.expect("expected one row");
    assert_eq!(row.n, 2);

    // Read-write DML (two RPC flow under the hood)
    let id = 7i64;
    let value = "hello".to_string();
    let write_result = client
        .read_write()
        .with_priority(RequestPriority::Medium)
        .with_timeout(Duration::from_secs(10))
        .run(
            "INSERT INTO items (id, value) VALUES (@id, @value)",
            &[("id", &id), ("value", &value)],
        )
        .await?;
    assert_eq!(write_result.affected_rows, 1);
    assert!(
        write_result.commit_timestamp.seconds > 0 || write_result.commit_timestamp.nanos > 0,
        "commit timestamp should be set"
    );

    let lookup_params = [("id", &id as &dyn spanlite::ToSpanner)];
    let stream = client
        .read_only()
        .with_timeout(Duration::from_secs(10))
        .run::<ItemRow>("SELECT id, value FROM items WHERE id = @id", &lookup_params)?;
    pin_mut!(stream);
    let row = stream.try_next().await?.expect("expected inserted row");
    assert_eq!(row.id, id);
    assert_eq!(row.value, value);
    assert!(stream.try_next().await?.is_none());

    Ok(())
}

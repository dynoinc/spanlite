# spanlite

Minimal async Rust client for Google Cloud Spanner, optimized for high-throughput one-shot reads.

## What it provides

- One-shot read-only queries via `execute_streaming_sql` with single-use transactions
- One-shot read-write DML via a 2-RPC flow (`ExecuteSql` begin + `Commit`)
- Streaming row decoding into Rust structs via `serde`
- Multiplexed sessions (auto-rotated before expiry)
- Pluggable auth via a user-provided `TokenSource`

## Requirements

- Rust toolchain `1.88+`
- `protoc` for build-time protobuf generation
- Docker only for emulator integration tests

## Install

```toml
[dependencies]
spanlite = { git = "https://github.com/dynoinc/spanlite.git" }
```

## Quick example

```rust
use futures_util::{TryStreamExt, pin_mut};
use serde::Deserialize;
use spanlite::{Client, ClientConfig, TokenSource};

#[derive(Clone, Copy)]
struct StaticToken;

impl TokenSource for StaticToken {
    fn token(
        &self,
    ) -> impl std::future::Future<
        Output = std::result::Result<String, Box<dyn std::error::Error + Send + Sync>>,
    > + Send {
        std::future::ready(Ok("YOUR_BEARER_TOKEN".to_string()))
    }
}

#[derive(Debug, Deserialize)]
struct Row {
    n: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = "projects/p/instances/i/databases/d";
    let client = Client::new(ClientConfig::new(db, StaticToken)).await?;

    let rows = client
        .read_only()
        .with_timeout(std::time::Duration::from_secs(10))
        .run::<Row>("SELECT 1 AS n", &[])?;
    pin_mut!(rows);

    while let Some(row) = rows.try_next().await? {
        println!("{}", row.n);
    }
    Ok(())
}
```

## Parameterized queries

```rust
use spanlite::ToSpanner;

let name = "alice";
let min_age = 18i64;
let rows = client
    .read_only()
    .with_timeout(std::time::Duration::from_secs(10))
    .run::<User>(
        "SELECT * FROM Users WHERE name = @name AND age >= @min_age",
        &[("name", &name), ("min_age", &min_age)],
    )?;
```

## Stale reads

```rust
use std::time::Duration;
use spanlite::TimestampBound;

let rows = client
    .read_only()
    .with_timestamp_bound(TimestampBound::ExactStaleness(Duration::from_secs(15)))
    .with_timeout(Duration::from_secs(10))
    .run::<Row>("SELECT 1 AS n", &[])?;
```

## One-shot DML

```rust
use spanlite::RequestPriority;

let id = 7i64;
let value = "hello".to_string();
let write = client
    .read_write()
    .with_priority(RequestPriority::Medium)
    .with_timeout(std::time::Duration::from_secs(10))
    .run(
        "INSERT INTO items (id, value) VALUES (@id, @value)",
        &[("id", &id), ("value", &value)],
    )
    .await?;

assert_eq!(write.affected_rows, 1);
println!(
    "committed at {}.{}",
    write.commit_timestamp.seconds, write.commit_timestamp.nanos
);
```

## Development

Run the full local check pipeline:

```bash
just check
```

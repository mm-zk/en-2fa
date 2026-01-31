use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

#[derive(Debug, Clone)]
pub struct ReadyExecuteCall {
    pub l1_batch_number: i64,
    pub execute_calldata: Vec<u8>,
}

/// Minimal DB interface so the rest of the program stays clean.
#[async_trait::async_trait]
pub trait BatchDb: Send + Sync {
    async fn fetch_next_ready_execute_call(&self, min_batch_exclusive: i64) -> Result<Option<ReadyExecuteCall>>;
}

pub struct PostgresBatchDb {
    pool: PgPool,
}

impl PostgresBatchDb {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl BatchDb for PostgresBatchDb {
    async fn fetch_next_ready_execute_call(&self, min_batch_exclusive: i64) -> Result<Option<ReadyExecuteCall>> {
        // Runtime query (no compile-time DB connection)
        let row = sqlx::query(
            r#"
            SELECT
                b.number  AS l1_batch_number,
                t.calldata AS execute_calldata
            FROM l1_batches b
            JOIN eth_txs t
              ON t.id = b.eth_execute_tx_id
            WHERE
                b.number > $1
                AND b.eth_commit_tx_id IS NOT NULL
                AND b.eth_execute_tx_id IS NOT NULL
            ORDER BY b.number ASC
            LIMIT 1
            "#,
        )
        .bind(min_batch_exclusive)
        .fetch_optional(&self.pool)
        .await
        .context("DB query failed while fetching next ready execute call")?;

        let Some(row) = row else {
            return Ok(None);
        };

        let l1_batch_number: i64 = row
            .try_get("l1_batch_number")
            .context("Missing/invalid l1_batch_number column")?;

        let execute_calldata: Vec<u8> = row
            .try_get("execute_calldata")
            .context("Missing/invalid execute_calldata column (expected bytea)")?;

        Ok(Some(ReadyExecuteCall {
            l1_batch_number,
            execute_calldata,
        }))
    }
}

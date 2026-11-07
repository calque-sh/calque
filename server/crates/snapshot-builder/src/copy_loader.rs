use thiserror::Error;

const MAX_BATCH_ROWS: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyLoadLimits {
    pub max_rows: u64,
    /// Maximum canonical decoded size of one non-null scalar.
    pub max_value_bytes: u64,
    /// Maximum sum of canonical decoded scalar sizes in one row.
    pub max_row_bytes: u64,
    /// Maximum sum of canonical decoded scalar sizes across the whole table.
    pub max_decoded_value_bytes: u64,
    /// Maximum retained decoded payload plus value-vector storage in one
    /// pending batch.
    pub max_batch_bytes: u64,
    pub batch_rows: usize,
}

impl Default for CopyLoadLimits {
    fn default() -> Self {
        Self {
            max_rows: 100_000_000,
            max_value_bytes: 16 * 1024 * 1024,
            max_row_bytes: 64 * 1024 * 1024,
            max_decoded_value_bytes: 8 * 1024 * 1024 * 1024,
            max_batch_bytes: 64 * 1024 * 1024,
            batch_rows: 4_096,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyLoadStats {
    pub rows: u64,
    pub decoded_value_bytes: u64,
    pub committed_batches: u64,
    pub peak_batch_bytes: u64,
}

pub(crate) fn validate_limits(limits: CopyLoadLimits) -> Result<(), CopyLoadError> {
    if limits.max_rows == 0
        || limits.max_value_bytes == 0
        || limits.max_row_bytes == 0
        || limits.max_decoded_value_bytes == 0
        || limits.max_batch_bytes == 0
        || limits.batch_rows == 0
        || limits.batch_rows > MAX_BATCH_ROWS
    {
        Err(CopyLoadError::InvalidLimits)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CopyLoadError {
    #[error("COPY load was cancelled")]
    Cancelled,
    #[error("COPY materialization channel closed before the builder acknowledged the batch")]
    MaterializationChannelClosed,
    #[error("COPY load limits must be positive and batch_rows cannot exceed 65536")]
    InvalidLimits,
    #[error("PostgreSQL binary COPY decoding failed: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("COPY row count overflowed u64")]
    RowOverflow,
    #[error("COPY decoded byte count overflowed u64")]
    DecodedByteOverflow,
    #[error("COPY exceeded the {maximum}-row limit")]
    RowLimitExceeded { maximum: u64 },
    #[error("COPY exceeded the {maximum}-decoded-value-byte limit")]
    DecodedByteLimitExceeded { maximum: u64 },
    #[error("COPY exceeded the {maximum}-byte retained batch-memory limit")]
    BatchByteLimitExceeded { maximum: u64 },
    #[error(transparent)]
    Direct(#[from] crate::DirectColumnarCopyError),
}

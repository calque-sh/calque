//! One exact point-in-time direct-columnar artifact, encoded straight from
//! PostgreSQL binary COPY.
//!
//! The builder plans the authorized table set, rolls the temporary stream
//! forward to the cut marker into an overlay, streams each table's COPY
//! through that overlay into the encoder, and writes the artifact only once a
//! `CanonicalCutProof` exists. Catalog publication and object upload are
//! separate layers.

mod build_state;
mod copy_loader;
mod direct_build;
mod direct_copy_loader;
mod direct_overlay;
mod plan;
mod roll_forward;

pub use build_state::{
    BoundedCopyLimits, BoundedCopyProgress, SnapshotBuildStateError, SnapshotCopyWorkload,
    COLLATION_GENERATION_V1, NO_INDEX_PROFILE_HASH, PUBLICATION_GENERATION_V1,
    SOURCE_WIDE_SHARD_PROFILE_HASH, TYPE_CODEC_GENERATION_V1,
};
pub use copy_loader::{CopyLoadError, CopyLoadLimits, CopyLoadStats};
pub use direct_build::{
    direct_columnar_artifact_identity, direct_columnar_workspace_schema, ArtifactTee,
    DirectArtifactAtCut, DirectArtifactRequest, DirectBuildSession, DirectFinalizationError,
};
pub use direct_columnar::EncoderLimits;
pub use direct_copy_loader::{
    decode_direct_columnar_row, direct_columnar_table_schema, DirectColumnarCopyError,
    DirectColumnarTableBinding,
};
pub use direct_overlay::DirectOverlayError;
pub use plan::{
    CopyColumnPlan, SnapshotBuildPlan, SnapshotColumnPlan, SnapshotPlanError, SnapshotTablePlan,
};
pub use roll_forward::{SnapshotApplyError, SnapshotRollForwardError, SnapshotRollForwardStats};

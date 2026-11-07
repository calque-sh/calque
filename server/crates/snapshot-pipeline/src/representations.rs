use std::{
    fs::File,
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread::JoinHandle,
    time::Instant,
};

use artifact_contract::{
    brotli_covering_window_bits, ArtifactChunks, ArtifactRepresentation, BrotliParameters,
    PackedParameters, RepresentationEncoding, ZstdParameters, DIRECT_COLUMNAR_CHUNK_BYTES,
    PACKED_BODY_VERSION,
};
use brotli::enc::{BrotliEncoderParams, StandardAlloc};
use direct_columnar::pack::{pack_unless, unpack, PackError, UnpackError};
use sha2::{Digest, Sha256};
use snapshot_builder::ArtifactTee;
use sync_types::{IdentifierError, ObjectId};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use zstd::stream::raw::CParameter;

/// The level preparation encodes a published zstd object at. Higher levels
/// cut more bytes but multiply the encode time every preparation waits for.
pub(crate) const ZSTD_LEVEL: i32 = 12;
/// An 8 MiB window, the largest RFC 9659 lets a zstd content coding require
/// of a browser.
pub(crate) const ZSTD_WINDOW_LOG: u8 = 23;
/// The worker count sets how long the encode takes, not what it writes.
const ZSTD_WORKERS: u32 = 12;
/// Jobs smaller than libzstd's 32 MiB default at this window hold less memory
/// and split an artifact into enough jobs to keep every worker busy.
const ZSTD_JOB_BYTES: u32 = 2 * 1024 * 1024;
/// Each job after the first preloads a sixteenth of the window into its match
/// finder instead of the quarter level 12 would, which is as much as the job.
const ZSTD_OVERLAP_LOG: u32 = 5;
/// libzstd sizes the hash table for the whole input, where a job reaches at
/// most its 2 MiB and the overlap's 512 KiB: 2^21 entries is about one for
/// each.
const ZSTD_HASH_LOG: u32 = 21;

/// The level a published artifact's zstd object is re-encoded at once it is
/// published, on the calling thread alone: more workers at this level hold
/// several times the memory.
pub const REENCODED_ZSTD_LEVEL: i32 = 19;

/// The level a zstd representation records and the worker threads that write
/// it. Every other parameter is the same for both.
#[derive(Debug, Clone, Copy)]
struct ZstdEncoding {
    level: i32,
    workers: u32,
}

const PUBLISHED_ZSTD: ZstdEncoding = ZstdEncoding {
    level: ZSTD_LEVEL,
    workers: ZSTD_WORKERS,
};

const REENCODED_ZSTD: ZstdEncoding = ZstdEncoding {
    level: REENCODED_ZSTD_LEVEL,
    workers: 0,
};

/// What a lowered thread publishes at: the same level, on the thread alone.
/// libzstd starts its own workers, which macOS runs at normal priority
/// whatever the class of the thread that starts them.
const BACKGROUND_ZSTD: ZstdEncoding = ZstdEncoding {
    level: ZSTD_LEVEL,
    workers: 0,
};

fn published_zstd() -> ZstdEncoding {
    if background_thread::current_thread_is_lowered() {
        BACKGROUND_ZSTD
    } else {
        PUBLISHED_ZSTD
    }
}

/// The quality of the brotli-large-window representation: brotli's highest,
/// since no lower quality halved its CPU time for at most 0.5% more bytes.
pub const BROTLI_QUALITY: u8 = 11;
/// The first byte of a stream that declares a large window; the low six bits
/// of the second are the window.
const BROTLI_LARGE_WINDOW_MARKER: u8 = 0x11;
/// The encoder's size hint is bounded by the largest window.
const BROTLI_MAX_SIZE_HINT: u64 = 1 << 30;

const IO_BUFFER_BYTES: usize = 64 * 1024;
const ZSTD_MAX_FRAME_HEADER_BYTES: usize = 18;
// The zstd encoder hands its sink one output buffer (about 128 KiB) at a
// time; this many may wait for the verifier before the encoder blocks.
const VERIFIER_QUEUE_CHUNKS: usize = 16;
// Whole decoded chunks that may wait for their digest before the verifier
// blocks. With the one being digested and the one being filled, at most two
// more chunk buffers than this exist.
const CHUNK_DIGEST_QUEUE: usize = 2;

/// The two transport representations the manifest schema mandates (zstd and
/// identity), zstd verified to decode back to the decoded artifact exactly
/// before it may be published, and the digest of each chunk of the decoded
/// artifact that verification read. Compressed files live beside the decoded
/// artifact and are owned by this value until every upload finishes.
pub struct PreparedRepresentations {
    entries: Vec<PreparedRepresentation>,
    decoded_chunks: ArtifactChunks,
}

impl PreparedRepresentations {
    #[cfg(test)]
    pub(crate) fn prepare(
        decoded_path: &Path,
        decoded_bytes: u64,
        decoded_sha256: &str,
        max_encoded_bytes: u64,
    ) -> Result<Self, RepresentationError> {
        Self::prepare_controlled(
            decoded_path,
            decoded_bytes,
            decoded_sha256,
            max_encoded_bytes,
            &CancellationToken::new(),
        )
    }

    pub fn prepare_controlled(
        decoded_path: &Path,
        decoded_bytes: u64,
        decoded_sha256: &str,
        max_encoded_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<Self, RepresentationError> {
        Self::prepare_chunked(
            decoded_path,
            decoded_bytes,
            decoded_sha256,
            max_encoded_bytes,
            DIRECT_COLUMNAR_CHUNK_BYTES,
            cancellation,
        )
    }

    fn prepare_chunked(
        decoded_path: &Path,
        decoded_bytes: u64,
        decoded_sha256: &str,
        max_encoded_bytes: u64,
        chunk_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<Self, RepresentationError> {
        ensure_not_cancelled(cancellation)?;
        if max_encoded_bytes < decoded_bytes {
            return Err(RepresentationError::EncodedLimitTooSmall {
                required: decoded_bytes,
                maximum: max_encoded_bytes,
            });
        }
        let identity = identity_representation(decoded_path, decoded_bytes, decoded_sha256)?;

        // Identity needs no verification pass of its own: the zstd
        // representation is decoded back and compared with `decoded_sha256`
        // below, and the blob store hashes identity's bytes as it stores them.
        let (zstd, chunk_digests) = prepare_zstd(
            decoded_path,
            decoded_bytes,
            decoded_sha256,
            max_encoded_bytes,
            chunk_bytes,
            published_zstd(),
            cancellation,
        )?;

        Ok(Self {
            entries: vec![zstd, identity],
            decoded_chunks: ArtifactChunks {
                bytes: chunk_bytes,
                sha256: chunk_digests,
            },
        })
    }

    pub fn manifests(&self) -> Vec<ArtifactRepresentation> {
        self.entries
            .iter()
            .map(|entry| entry.manifest.clone())
            .collect()
    }

    /// The SHA-256 of each consecutive [`DIRECT_COLUMNAR_CHUNK_BYTES`] of the
    /// decoded artifact, the last covering what is left. They are read off the
    /// bytes the zstd representation decoded to, the same bytes whose length
    /// and digest were checked against `decoded_sha256`.
    pub fn decoded_chunks(&self) -> &ArtifactChunks {
        &self.decoded_chunks
    }

    pub fn entries(&self) -> impl Iterator<Item = &PreparedRepresentation> {
        self.entries.iter()
    }
}

/// Re-encodes a published artifact's decoded object at
/// [`REENCODED_ZSTD_LEVEL`] on the calling thread, through the verifier a
/// publication's encode goes through, into a file beside `decoded_path`.
/// Returns the representation and the digest of each decoded chunk its
/// verification read. An encode that would pass `max_encoded_bytes` stops
/// there and fails.
pub fn reencode_zstd_controlled(
    decoded_path: &Path,
    decoded_bytes: u64,
    decoded_sha256: &str,
    max_encoded_bytes: u64,
    cancellation: &CancellationToken,
) -> Result<(PreparedRepresentation, Vec<String>), RepresentationError> {
    prepare_zstd(
        decoded_path,
        decoded_bytes,
        decoded_sha256,
        max_encoded_bytes,
        DIRECT_COLUMNAR_CHUNK_BYTES,
        REENCODED_ZSTD,
        cancellation,
    )
}

/// Encodes a published artifact's decoded object as one brotli stream with
/// the large-window extension, at [`BROTLI_QUALITY`] and the smallest window
/// that covers it, on the calling thread, through a verifier that decodes it
/// again and checks every chunk, into a file beside `decoded_path`. Returns
/// the representation and the digest of each decoded chunk its verification
/// read. An encode that would pass `max_encoded_bytes` stops there and fails.
pub fn encode_brotli_large_window_controlled(
    decoded_path: &Path,
    decoded_bytes: u64,
    decoded_sha256: &str,
    max_encoded_bytes: u64,
    cancellation: &CancellationToken,
) -> Result<(PreparedRepresentation, Vec<String>), RepresentationError> {
    prepare_brotli(
        decoded_path,
        decoded_bytes,
        decoded_sha256,
        max_encoded_bytes,
        DIRECT_COLUMNAR_CHUNK_BYTES,
        BROTLI_QUALITY,
        cancellation,
    )
}

/// A packed body in a file beside the artifact it stands for.
pub struct PackedArtifact {
    body: NamedTempFile,
    body_bytes: u64,
}

impl PackedArtifact {
    pub fn body_bytes(&self) -> u64 {
        self.body_bytes
    }
}

/// Packs the decoded artifact at `decoded_path` on the calling thread and
/// writes the body to a file beside it. Returns `None` where the packer
/// transforms no column, and refuses an artifact no brotli window covers
/// before it reads it.
///
/// `cancellation` is asked before the file is read and wherever
/// [`pack_unless`] asks, and the pack ends at the first ask after a cancel.
/// It does not ask while it reads the file, indexes one table's key,
/// transforms one column, or checks at the end that the body restores the
/// artifact.
pub fn pack_artifact_controlled(
    decoded_path: &Path,
    decoded_bytes: u64,
    cancellation: &CancellationToken,
) -> Result<Option<PackedArtifact>, RepresentationError> {
    pack_artifact(decoded_path, decoded_bytes, &|| cancellation.is_cancelled())
}

/// [`pack_artifact_controlled`], asking `cancelled` and nothing else whether
/// to stop.
fn pack_artifact(
    decoded_path: &Path,
    decoded_bytes: u64,
    cancelled: &dyn Fn() -> bool,
) -> Result<Option<PackedArtifact>, RepresentationError> {
    let started = Instant::now();
    let parent = decoded_path
        .parent()
        .ok_or(RepresentationError::MissingParentDirectory)?;
    if cancelled() {
        return Err(RepresentationError::Cancelled);
    }
    // The bound is the artifact's, as the plain encode has it: a body under
    // it would publish a brotli object for an artifact that had none, and
    // one over it would be refused only after the whole pack.
    if brotli_covering_window_bits(decoded_bytes).is_none() {
        return Err(RepresentationError::BrotliWindowExceeded { decoded_bytes });
    }
    let artifact = read_decoded(decoded_path, decoded_bytes)?;
    let packed = pack_unless(&artifact, cancelled).map_err(|error| match error {
        PackError::Cancelled => RepresentationError::Cancelled,
        error => RepresentationError::Pack(error),
    })?;
    drop(artifact);
    crate::phase_finished("pack", started);
    // A body that only leaves the artifact's padding out is shorter and
    // transforms nothing.
    if packed.stored
        || packed
            .columns
            .iter()
            .all(|column| column.transform.is_none())
    {
        return Ok(None);
    }
    let mut body = NamedTempFile::new_in(parent)?;
    body.write_all(&packed.body)?;
    Ok(Some(PackedArtifact {
        body,
        body_bytes: packed.body.len() as u64,
    }))
}

/// Encodes `packed` as one brotli stream with the large-window extension, at
/// [`BROTLI_QUALITY`] and the smallest window that covers the body, on the
/// calling thread, into a file beside the body. Its verifier decodes the
/// stream again, restores the artifact from the body it decoded, and checks
/// that artifact's length, SHA-256 and every chunk against `decoded_bytes` and
/// `decoded_sha256`. Returns the representation and the digest of each chunk
/// of the restored artifact. An encode that would pass `max_encoded_bytes`
/// stops there and fails.
pub fn encode_packed_brotli_large_window_controlled(
    packed: &PackedArtifact,
    decoded_bytes: u64,
    decoded_sha256: &str,
    max_encoded_bytes: u64,
    cancellation: &CancellationToken,
) -> Result<(PreparedRepresentation, Vec<String>), RepresentationError> {
    let chunk_bytes = usize::try_from(DIRECT_COLUMNAR_CHUNK_BYTES)
        .map_err(|_| RepresentationError::SizeOverflow)?;
    let body_path = packed.body.path();
    let body_bytes = packed.body_bytes;
    let window_bits = brotli_covering_window_bits(body_bytes).ok_or(
        RepresentationError::BrotliWindowExceeded {
            decoded_bytes: body_bytes,
        },
    )?;
    let verified = encode_verified(
        body_path,
        max_encoded_bytes,
        ["packed_brotli_encode", "packed_brotli_verify"],
        cancellation,
        |output| {
            encode_brotli(
                body_path,
                body_bytes,
                output,
                BROTLI_QUALITY,
                window_bits,
                cancellation,
            )
        },
        |input| {
            verify_packed_brotli_stream(
                input,
                body_bytes,
                decoded_bytes,
                decoded_sha256,
                window_bits,
                chunk_bytes,
                cancellation,
            )
        },
    )?;
    let prepared = prepared_compressed(
        verified.temporary,
        RepresentationEncoding::PackedBrotliLargeWindow,
        verified.encoded_bytes,
        verified.encoded_sha256,
        None,
        Some(BrotliParameters {
            quality: BROTLI_QUALITY,
            window_bits,
        }),
        Some(PackedParameters {
            version: PACKED_BODY_VERSION,
            body_bytes,
        }),
    )?;
    Ok((prepared, verified.decoded_chunks))
}

/// The zstd representation of an artifact encoded from the artifact's bytes
/// as [`snapshot_builder::DirectBuildSession::finish_at_cut`] writes them,
/// rather than from its file once it is written, so that the encode runs
/// beside the write. Its verifier starts when the artifact's length is known
/// and decodes the representation back as it is written;
/// [`ZstdWhileWriting::finish`] holds what it decoded to the written
/// artifact's digest.
pub(crate) struct ZstdWhileWriting {
    directory: PathBuf,
    max_encoded_bytes: u64,
    cancellation: CancellationToken,
    encode: Option<ZstdEncode>,
    refused: Option<RepresentationError>,
}

struct ZstdEncode {
    started: Instant,
    encoding: ZstdEncoding,
    decoded_bytes: u64,
    temporary: NamedTempFile,
    encoder: ZstdEncoder,
    verifier: JoinHandle<Result<(StreamVerification, String), RepresentationError>>,
    verifier_stopped: Arc<AtomicBool>,
}

impl ZstdWhileWriting {
    /// An encode into a file in `directory`, which is the artifact's.
    pub(crate) fn new(
        directory: &Path,
        max_encoded_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Self {
        Self {
            directory: directory.to_path_buf(),
            max_encoded_bytes,
            cancellation: cancellation.clone(),
            encode: None,
            refused: None,
        }
    }

    fn start(&self, decoded_bytes: u64) -> Result<ZstdEncode, RepresentationError> {
        ensure_not_cancelled(&self.cancellation)?;
        if self.max_encoded_bytes < decoded_bytes {
            return Err(RepresentationError::EncodedLimitTooSmall {
                required: decoded_bytes,
                maximum: self.max_encoded_bytes,
            });
        }
        let chunk_bytes = usize::try_from(DIRECT_COLUMNAR_CHUNK_BYTES)
            .map_err(|_| RepresentationError::SizeOverflow)?;
        let started = Instant::now();
        let temporary = NamedTempFile::new_in(&self.directory)?;
        let output = BoundedWriter::new(
            temporary.reopen()?,
            self.max_encoded_bytes,
            self.cancellation.clone(),
        );
        let (chunks, queue) = mpsc::sync_channel(VERIFIER_QUEUE_CHUNKS);
        let cancellation = self.cancellation.clone();
        let verifier = background_thread::spawn(
            std::thread::Builder::new().name("calque-zstd-verify".to_owned()),
            move || {
                decode_zstd_stream(
                    QueueReader::new(queue),
                    decoded_bytes,
                    chunk_bytes,
                    &cancellation,
                )
            },
        )?;
        let verifier_stopped = Arc::new(AtomicBool::new(false));
        let output = TeeWriter {
            output,
            chunks,
            verifier_stopped: Arc::clone(&verifier_stopped),
        };
        let encoding = published_zstd();
        Ok(ZstdEncode {
            started,
            encoding,
            decoded_bytes,
            temporary,
            encoder: zstd_encoder(output, encoding, decoded_bytes)?,
            verifier,
            verifier_stopped,
        })
    }

    /// Ends the encode of the artifact written at `decoded_path`, waits for
    /// its verifier, and returns the zstd and identity representations once
    /// what the verifier decoded is `decoded_bytes` long with `decoded_sha256`.
    pub(crate) fn finish(
        self,
        decoded_path: &Path,
        decoded_bytes: u64,
        decoded_sha256: &str,
    ) -> Result<PreparedRepresentations, RepresentationError> {
        if let Some(error) = self.refused {
            return Err(error);
        }
        let encode = self.encode.ok_or_else(|| {
            RepresentationError::Io(io::Error::other("the artifact's write never began"))
        })?;
        let encoded = finish_zstd(encode.encoder);
        let encoder_finished = Instant::now();
        let verified = encode
            .verifier
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        crate::phase_finished("zstd_encode", encode.started);
        crate::phase_finished("zstd_verify", encoder_finished);
        let (encoded_bytes, (verified, decoded)) = match (encoded, verified) {
            (Ok(encoded_bytes), Ok(verified)) => (encoded_bytes, verified),
            (Ok(_), Err(error)) => return Err(error),
            (Err(_), Err(error)) if encode.verifier_stopped.load(Ordering::Relaxed) => {
                return Err(error)
            }
            (Err(error), _) => return Err(error),
        };
        if encode.decoded_bytes != decoded_bytes {
            return Err(RepresentationError::DecodedLengthMismatch {
                expected: decoded_bytes,
                actual: encode.decoded_bytes,
            });
        }
        hold_decoded_digest(&decoded, decoded_sha256)?;
        let encoded_sha256 = encoded_digest(
            verified.encoded_bytes,
            verified.encoded_sha256,
            encoded_bytes,
        )?;
        let zstd = prepared_compressed(
            encode.temporary,
            RepresentationEncoding::Zstd,
            encoded_bytes,
            encoded_sha256,
            Some(ZstdParameters {
                level: encode.encoding.level,
                window_log: ZSTD_WINDOW_LOG,
            }),
            None,
            None,
        )?;
        Ok(PreparedRepresentations {
            entries: vec![
                zstd,
                identity_representation(decoded_path, decoded_bytes, decoded_sha256)?,
            ],
            decoded_chunks: ArtifactChunks {
                bytes: DIRECT_COLUMNAR_CHUNK_BYTES,
                sha256: verified.decoded_chunks,
            },
        })
    }

    /// Why the encode failed, after a write that failed: where the encode
    /// refused to begin or its verifier stopped reading, the write could only
    /// report that its tee failed.
    pub(crate) fn abandon(self) -> Option<RepresentationError> {
        if self.refused.is_some() {
            return self.refused;
        }
        let encode = self.encode?;
        // Dropping the encoder closes the verifier's queue mid-frame.
        drop(encode.encoder);
        let verified = encode
            .verifier
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        match verified {
            Err(error) if encode.verifier_stopped.load(Ordering::Relaxed) => Some(error),
            _ => None,
        }
    }
}

impl ArtifactTee for ZstdWhileWriting {
    fn begin(&mut self, artifact_bytes: u64) -> io::Result<()> {
        match self.start(artifact_bytes) {
            Ok(encode) => {
                self.encode = Some(encode);
                Ok(())
            }
            Err(error) => {
                let refused = io::Error::other(error.to_string());
                self.refused = Some(error);
                Err(refused)
            }
        }
    }
}

impl Write for ZstdWhileWriting {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match &mut self.encode {
            Some(encode) => encode.encoder.write(bytes),
            None => Err(io::Error::other("the zstd encode has not begun")),
        }
    }

    // Never flushed (`ArtifactTee`): a flush would end the frame's block.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The identity representation: the decoded artifact's own file.
fn identity_representation(
    decoded_path: &Path,
    decoded_bytes: u64,
    decoded_sha256: &str,
) -> Result<PreparedRepresentation, RepresentationError> {
    let id = format!("sha256:{decoded_sha256}");
    Ok(PreparedRepresentation {
        manifest: ArtifactRepresentation {
            representation_id: id.clone(),
            object_id: ObjectId::new(id)?,
            encoding: RepresentationEncoding::Identity,
            encoded_sha256: decoded_sha256.to_owned(),
            encoded_bytes: decoded_bytes,
            zstd: None,
            brotli: None,
            packed: None,
        },
        path: decoded_path.to_path_buf(),
        temporary: None,
    })
}

pub struct PreparedRepresentation {
    manifest: ArtifactRepresentation,
    path: PathBuf,
    // Owns the path until every immutable upload finishes.
    temporary: Option<NamedTempFile>,
}

impl PreparedRepresentation {
    pub fn manifest(&self) -> &ArtifactRepresentation {
        &self.manifest
    }

    pub fn path(&self) -> &Path {
        debug_assert!(self
            .temporary
            .as_ref()
            .is_none_or(|temporary| temporary.path() == self.path));
        &self.path
    }
}

/// An encode's file and what its verifier read of it.
struct VerifiedEncode {
    temporary: NamedTempFile,
    encoded_bytes: u64,
    encoded_sha256: String,
    decoded_chunks: Vec<String>,
}

/// Runs `encode` into a file beside `decoded_path`, bounded by
/// `max_encoded_bytes`, while `verify` reads the same bytes from a queue on
/// its own thread. `phases` names the encode and the verify in the phase log.
fn encode_verified(
    decoded_path: &Path,
    max_encoded_bytes: u64,
    phases: [&'static str; 2],
    cancellation: &CancellationToken,
    encode: impl FnOnce(TeeWriter<BoundedWriter<File>>) -> Result<u64, RepresentationError>,
    verify: impl FnOnce(QueueReader) -> Result<StreamVerification, RepresentationError> + Send,
) -> Result<VerifiedEncode, RepresentationError> {
    ensure_not_cancelled(cancellation)?;
    let started = Instant::now();
    let parent = decoded_path
        .parent()
        .ok_or(RepresentationError::MissingParentDirectory)?;
    let temporary = NamedTempFile::new_in(parent)?;
    let output = BoundedWriter::new(temporary.reopen()?, max_encoded_bytes, cancellation.clone());
    // A second thread decodes the encoder's output back as it is written, so
    // verification runs beside the encode instead of after it. It reads the
    // bytes the encoder wrote to the file rather than the file: the digest it
    // returns names exactly the bytes it verified, and the blob store checks
    // the file against that digest when it stores it.
    let (chunks, queue) = mpsc::sync_channel(VERIFIER_QUEUE_CHUNKS);
    let verifier_stopped = Arc::new(AtomicBool::new(false));
    let (encoded, verified, encoder_finished) = std::thread::scope(|scope| {
        let verifier =
            background_thread::spawn_scoped(scope, move || verify(QueueReader::new(queue)));
        let output = TeeWriter {
            output,
            chunks,
            verifier_stopped: Arc::clone(&verifier_stopped),
        };
        let encoded = encode(output);
        let encoder_finished = Instant::now();
        let verified = verifier
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        (encoded, verified, encoder_finished)
    });
    crate::phase_finished(phases[0], started);
    crate::phase_finished(phases[1], encoder_finished);
    let (encoded_bytes, verified) = match (encoded, verified) {
        (Ok(encoded_bytes), Ok(verified)) => (encoded_bytes, verified),
        (Ok(_), Err(error)) => return Err(error),
        // Once the verifier stops, the encoder can only report that its
        // reader went away; the verifier's error says why.
        (Err(_), Err(error)) if verifier_stopped.load(Ordering::Relaxed) => return Err(error),
        (Err(error), _) => return Err(error),
    };
    let encoded_sha256 = encoded_digest(
        verified.encoded_bytes,
        verified.encoded_sha256,
        encoded_bytes,
    )?;
    Ok(VerifiedEncode {
        temporary,
        encoded_bytes,
        encoded_sha256,
        decoded_chunks: verified.decoded_chunks,
    })
}

/// Returns the zstd representation and the digest of each decoded chunk its
/// verification read.
fn prepare_zstd(
    decoded_path: &Path,
    decoded_bytes: u64,
    decoded_sha256: &str,
    max_encoded_bytes: u64,
    chunk_bytes: u64,
    encoding: ZstdEncoding,
    cancellation: &CancellationToken,
) -> Result<(PreparedRepresentation, Vec<String>), RepresentationError> {
    let chunk_bytes =
        usize::try_from(chunk_bytes).map_err(|_| RepresentationError::SizeOverflow)?;
    let verified = encode_verified(
        decoded_path,
        max_encoded_bytes,
        ["zstd_encode", "zstd_verify"],
        cancellation,
        |output| encode_zstd(decoded_path, decoded_bytes, output, encoding, cancellation),
        |input| {
            verify_zstd_stream(
                input,
                decoded_bytes,
                decoded_sha256,
                chunk_bytes,
                cancellation,
            )
        },
    )?;
    let prepared = prepared_compressed(
        verified.temporary,
        RepresentationEncoding::Zstd,
        verified.encoded_bytes,
        verified.encoded_sha256,
        Some(ZstdParameters {
            level: encoding.level,
            window_log: ZSTD_WINDOW_LOG,
        }),
        None,
        None,
    )?;
    Ok((prepared, verified.decoded_chunks))
}

/// Returns the brotli-large-window representation at `quality` and the
/// covering window, and the digest of each decoded chunk its verification
/// read.
fn prepare_brotli(
    decoded_path: &Path,
    decoded_bytes: u64,
    decoded_sha256: &str,
    max_encoded_bytes: u64,
    chunk_bytes: u64,
    quality: u8,
    cancellation: &CancellationToken,
) -> Result<(PreparedRepresentation, Vec<String>), RepresentationError> {
    let chunk_bytes =
        usize::try_from(chunk_bytes).map_err(|_| RepresentationError::SizeOverflow)?;
    let window_bits = brotli_covering_window_bits(decoded_bytes)
        .ok_or(RepresentationError::BrotliWindowExceeded { decoded_bytes })?;
    let verified = encode_verified(
        decoded_path,
        max_encoded_bytes,
        ["brotli_encode", "brotli_verify"],
        cancellation,
        |output| {
            encode_brotli(
                decoded_path,
                decoded_bytes,
                output,
                quality,
                window_bits,
                cancellation,
            )
        },
        |input| {
            verify_brotli_stream(
                input,
                decoded_bytes,
                decoded_sha256,
                window_bits,
                chunk_bytes,
                cancellation,
            )
        },
    )?;
    let prepared = prepared_compressed(
        verified.temporary,
        RepresentationEncoding::BrotliLargeWindow,
        verified.encoded_bytes,
        verified.encoded_sha256,
        None,
        Some(BrotliParameters {
            quality,
            window_bits,
        }),
        None,
    )?;
    Ok((prepared, verified.decoded_chunks))
}

/// Reads the `decoded_bytes` of the decoded artifact at `decoded_path`, and
/// refuses a file of another length.
fn read_decoded(decoded_path: &Path, decoded_bytes: u64) -> Result<Vec<u8>, RepresentationError> {
    let length = usize::try_from(decoded_bytes).map_err(|_| RepresentationError::SizeOverflow)?;
    let mut input = File::open(decoded_path)?.take(decoded_bytes);
    let mut artifact = Vec::new();
    artifact
        .try_reserve_exact(length)
        .map_err(|_| RepresentationError::SizeOverflow)?;
    input.read_to_end(&mut artifact)?;
    let mut trailing = [0_u8; 1];
    if artifact.len() != length || input.into_inner().read(&mut trailing)? != 0 {
        return Err(RepresentationError::DecodedFileChanged {
            expected: decoded_bytes,
            actual: File::open(decoded_path)?.metadata()?.len(),
        });
    }
    Ok(artifact)
}

/// Encodes the decoded artifact at `decoded_path` through `output` and returns
/// the encoded length. `output`, and with it the verifier's queue, is closed by
/// the time this returns, whether or not it succeeded.
fn encode_zstd(
    decoded_path: &Path,
    decoded_bytes: u64,
    output: TeeWriter<BoundedWriter<File>>,
    encoding: ZstdEncoding,
    cancellation: &CancellationToken,
) -> Result<u64, RepresentationError> {
    let mut encoder = zstd_encoder(output, encoding, decoded_bytes)?;
    let mut input =
        CancellableReader::new(File::open(decoded_path)?, cancellation.clone()).take(decoded_bytes);
    let copied = io::copy(&mut input, &mut encoder)?;
    let mut trailing = [0_u8; 1];
    if copied != decoded_bytes || input.into_inner().read(&mut trailing)? != 0 {
        return Err(RepresentationError::DecodedFileChanged {
            expected: decoded_bytes,
            actual: File::open(decoded_path)?.metadata()?.len(),
        });
    }
    finish_zstd(encoder)
}

type ZstdEncoder = zstd::stream::write::Encoder<'static, TeeWriter<BoundedWriter<File>>>;

/// An encoder of one zstd frame at `encoding` whose content is exactly
/// `decoded_bytes` long.
fn zstd_encoder(
    output: TeeWriter<BoundedWriter<File>>,
    encoding: ZstdEncoding,
    decoded_bytes: u64,
) -> io::Result<ZstdEncoder> {
    let mut encoder = zstd::stream::write::Encoder::new(output, encoding.level)?;
    if encoding.workers > 0 {
        encoder.multithread(encoding.workers)?;
        encoder.set_parameter(CParameter::JobSize(ZSTD_JOB_BYTES))?;
        encoder.set_parameter(CParameter::OverlapSizeLog(ZSTD_OVERLAP_LOG))?;
        encoder.set_parameter(CParameter::HashLog(ZSTD_HASH_LOG))?;
    }
    encoder.window_log(u32::from(ZSTD_WINDOW_LOG))?;
    // No frame checksum: the plan's chunk digests already bind every decoded
    // byte, and a checksum makes the browser's decoder hash them a second time.
    encoder.include_checksum(false)?;
    encoder.include_contentsize(true)?;
    encoder.include_dictid(false)?;
    encoder.set_pledged_src_size(Some(decoded_bytes))?;
    Ok(encoder)
}

/// Ends the frame, closes the verifier's queue and flushes the file. Returns
/// the encoded length.
fn finish_zstd(encoder: ZstdEncoder) -> Result<u64, RepresentationError> {
    let TeeWriter { output, chunks, .. } = encoder.finish()?;
    // The verifier reads to the end of the queue, so close it before the
    // file's fsync rather than after.
    drop(chunks);
    let (output, encoded_bytes) = output.finish()?;
    output.sync_all()?;
    Ok(encoded_bytes)
}

/// Writes `decoded_path` as one large-window brotli stream to `output` and
/// returns the encoded length. `output`, and with it the verifier's queue, is
/// closed by the time this returns, whether or not it succeeded.
fn encode_brotli(
    decoded_path: &Path,
    decoded_bytes: u64,
    mut output: TeeWriter<BoundedWriter<File>>,
    quality: u8,
    window_bits: u8,
    cancellation: &CancellationToken,
) -> Result<u64, RepresentationError> {
    let params = BrotliEncoderParams {
        quality: i32::from(quality),
        lgwin: i32::from(window_bits),
        large_window: true,
        size_hint: usize::try_from(decoded_bytes.min(BROTLI_MAX_SIZE_HINT))
            .map_err(|_| RepresentationError::SizeOverflow)?,
        ..BrotliEncoderParams::default()
    };
    let mut input =
        CancellableReader::new(File::open(decoded_path)?, cancellation.clone()).take(decoded_bytes);
    let mut input_buffer = vec![0_u8; IO_BUFFER_BYTES];
    let mut output_buffer = vec![0_u8; IO_BUFFER_BYTES];
    brotli::BrotliCompressCustomAlloc(
        &mut input,
        &mut output,
        &mut input_buffer,
        &mut output_buffer,
        &params,
        StandardAlloc::default(),
    )?;
    let mut trailing = [0_u8; 1];
    if input.limit() != 0 || input.into_inner().read(&mut trailing)? != 0 {
        return Err(RepresentationError::DecodedFileChanged {
            expected: decoded_bytes,
            actual: File::open(decoded_path)?.metadata()?.len(),
        });
    }
    let TeeWriter { output, chunks, .. } = output;
    // The verifier reads to the end of the queue, so close it before the
    // file's fsync rather than after.
    drop(chunks);
    let (output, encoded_bytes) = output.finish()?;
    output.sync_all()?;
    Ok(encoded_bytes)
}

fn prepared_compressed(
    temporary: NamedTempFile,
    encoding: RepresentationEncoding,
    encoded_bytes: u64,
    encoded_sha256: String,
    zstd: Option<ZstdParameters>,
    brotli: Option<BrotliParameters>,
    packed: Option<PackedParameters>,
) -> Result<PreparedRepresentation, RepresentationError> {
    let id = format!("sha256:{encoded_sha256}");
    let path = temporary.path().to_path_buf();
    Ok(PreparedRepresentation {
        manifest: ArtifactRepresentation {
            representation_id: id.clone(),
            object_id: ObjectId::new(id)?,
            encoding,
            encoded_sha256,
            encoded_bytes,
            zstd,
            brotli,
            packed,
        },
        path,
        temporary: Some(temporary),
    })
}

/// Decodes the zstd file at `path` back to the decoded artifact and returns the
/// SHA-256 of the encoded bytes it read, which must be `encoded_bytes` long.
#[cfg(test)]
fn verify_zstd(
    path: &Path,
    encoded_bytes: u64,
    decoded_bytes: u64,
    decoded_sha256: &str,
    cancellation: &CancellationToken,
) -> Result<String, RepresentationError> {
    let verified = verify_zstd_stream(
        File::open(path)?,
        decoded_bytes,
        decoded_sha256,
        DIRECT_COLUMNAR_CHUNK_BYTES as usize,
        cancellation,
    )?;
    encoded_digest(
        verified.encoded_bytes,
        verified.encoded_sha256,
        encoded_bytes,
    )
}

/// What one verification of an encoded representation read.
struct StreamVerification {
    /// The length and SHA-256 of the encoded bytes read.
    encoded_bytes: u64,
    encoded_sha256: String,
    /// The digest of each consecutive chunk of the bytes they decoded to.
    decoded_chunks: Vec<String>,
}

/// Decodes the zstd representation read from `input` back to the decoded
/// artifact, and returns the length and SHA-256 of the encoded bytes it read,
/// so the digest names exactly the bytes that were verified, and the digest
/// of each `chunk_bytes` they decoded to.
fn verify_zstd_stream(
    input: impl Read,
    decoded_bytes: u64,
    decoded_sha256: &str,
    chunk_bytes: usize,
    cancellation: &CancellationToken,
) -> Result<StreamVerification, RepresentationError> {
    let (verified, decoded) = decode_zstd_stream(input, decoded_bytes, chunk_bytes, cancellation)?;
    hold_decoded_digest(&decoded, decoded_sha256)?;
    Ok(verified)
}

/// [`verify_zstd_stream`], except that it returns the SHA-256 of what it
/// decoded rather than holding it to an expected one, for an encode whose
/// artifact is still being written when its verifier starts.
fn decode_zstd_stream(
    input: impl Read,
    decoded_bytes: u64,
    chunk_bytes: usize,
    cancellation: &CancellationToken,
) -> Result<(StreamVerification, String), RepresentationError> {
    ensure_not_cancelled(cancellation)?;
    let mut input = HashingReader::new(input);
    // Zstandard's maximum standard frame header is 18 bytes. Keep this local
    // instead of enabling zstd-safe's experimental API surface.
    let mut prefix = [0_u8; ZSTD_MAX_FRAME_HEADER_BYTES];
    let mut prefix_bytes = 0;
    while prefix_bytes < prefix.len() {
        let read = input.read(&mut prefix[prefix_bytes..])?;
        if read == 0 {
            break;
        }
        prefix_bytes += read;
    }
    let prefix = &prefix[..prefix_bytes];
    let content_size = zstd::zstd_safe::get_frame_content_size(prefix)
        .map_err(|_| RepresentationError::InvalidZstdFrame)?
        .ok_or(RepresentationError::MissingZstdContentSize)?;
    if content_size != decoded_bytes {
        return Err(RepresentationError::ZstdContentSizeMismatch {
            expected: decoded_bytes,
            actual: content_size,
        });
    }
    if zstd::zstd_safe::get_dict_id_from_frame(prefix).is_some() {
        return Err(RepresentationError::ZstdDictionaryForbidden);
    }

    // The prefix was consumed to inspect it; the decoder reads it again from
    // memory, then the rest of the input.
    let reader = BufReader::with_capacity(IO_BUFFER_BYTES, prefix.chain(&mut input));
    let mut decoder = zstd::stream::read::Decoder::with_buffer(reader)?;
    decoder.window_log_max(u32::from(ZSTD_WINDOW_LOG))?;
    let mut decoder = decoder.single_frame();
    let decoded = decode_chunks(&mut decoder, decoded_bytes, chunk_bytes, cancellation)?;
    let mut reader = decoder.finish();
    if !reader.fill_buf()?.is_empty() {
        return Err(RepresentationError::TrailingEncodedBytes);
    }
    drop(reader);
    let (_, encoded_bytes, encoded_sha256) = input.finish();
    Ok((
        StreamVerification {
            encoded_bytes,
            encoded_sha256,
            decoded_chunks: decoded.chunks,
        },
        decoded.sha256,
    ))
}

/// Decodes one large-window brotli stream that declares exactly `window_bits`
/// and ends where its input does, as the client's decoder requires, and
/// returns what [`verify_zstd_stream`] does.
fn verify_brotli_stream(
    input: impl Read,
    decoded_bytes: u64,
    decoded_sha256: &str,
    window_bits: u8,
    chunk_bytes: usize,
    cancellation: &CancellationToken,
) -> Result<StreamVerification, RepresentationError> {
    verify_brotli_framing(input, window_bits, cancellation, |mut decoder| {
        verify_decoded(
            &mut decoder,
            decoded_bytes,
            decoded_sha256,
            chunk_bytes,
            cancellation,
        )
    })
}

/// Decodes a stream as [`verify_brotli_stream`] does into a packed body of
/// `body_bytes`, restores the artifact from that body, and checks the
/// artifact's length, SHA-256 and chunks. The restore is one call that does
/// not ask `cancellation`.
fn verify_packed_brotli_stream(
    input: impl Read,
    body_bytes: u64,
    decoded_bytes: u64,
    decoded_sha256: &str,
    window_bits: u8,
    chunk_bytes: usize,
    cancellation: &CancellationToken,
) -> Result<StreamVerification, RepresentationError> {
    verify_brotli_framing(input, window_bits, cancellation, |decoder| {
        let body = read_packed_body(decoder, body_bytes, cancellation)?;
        let artifact = unpack(&body, decoded_bytes)?;
        drop(body);
        verify_decoded(
            &mut artifact.as_slice(),
            decoded_bytes,
            decoded_sha256,
            chunk_bytes,
            cancellation,
        )
    })
}

/// Reads a packed body of exactly `body_bytes` from `decoder`, to its end.
fn read_packed_body(
    decoder: &mut dyn Read,
    body_bytes: u64,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, RepresentationError> {
    let expected = usize::try_from(body_bytes).map_err(|_| RepresentationError::SizeOverflow)?;
    let mut body = Vec::new();
    body.try_reserve_exact(expected.saturating_add(1))
        .map_err(|_| RepresentationError::SizeOverflow)?;
    // One byte past the body, so a longer one is seen and no more of it held.
    let mut decoder = decoder.take(body_bytes.saturating_add(1));
    let mut buffer = vec![0_u8; IO_BUFFER_BYTES];
    loop {
        ensure_not_cancelled(cancellation)?;
        let read = decoder.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&buffer[..read]);
    }
    if body.len() != expected {
        return Err(RepresentationError::PackedBodyLength {
            expected: body_bytes,
        });
    }
    Ok(body)
}

/// Checks that `input` is one large-window brotli stream that declares
/// exactly `window_bits` and ends where `input` does, as the client's decoder
/// requires. `decoded` reads what the stream decodes to, to its end, and
/// returns the chunk digests of the artifact it verified.
fn verify_brotli_framing(
    input: impl Read,
    window_bits: u8,
    cancellation: &CancellationToken,
    decoded: impl FnOnce(&mut dyn Read) -> Result<Vec<String>, RepresentationError>,
) -> Result<StreamVerification, RepresentationError> {
    ensure_not_cancelled(cancellation)?;
    let mut input = HashingReader::new(input);
    let mut header = [0_u8; 2];
    let declared = input.read_exact(&mut header).is_ok()
        && header[0] == BROTLI_LARGE_WINDOW_MARKER
        && (header[1] & 0x3f) == window_bits;
    if !declared {
        return Err(RepresentationError::BrotliWindowMismatch {
            expected: window_bits,
        });
    }
    // The header was consumed to inspect it; the decoder reads it again from
    // memory, then the rest of the input.
    let mut decoder =
        brotli::Decompressor::new(header.as_slice().chain(&mut input), IO_BUFFER_BYTES);
    let decoded_chunks = decoded(&mut decoder)?;
    // The decoder reports bytes it buffered past the stream's end only on a
    // read after the one that ended the stream, so it is read twice more.
    let mut trailing = [0_u8; 1];
    for _ in 0..2 {
        if !matches!(decoder.read(&mut trailing), Ok(0)) {
            return Err(RepresentationError::TrailingEncodedBytes);
        }
    }
    // And it never pulls bytes it did not need, so the input is read too.
    if decoder.into_inner().read(&mut trailing)? != 0 {
        return Err(RepresentationError::TrailingEncodedBytes);
    }
    let (_, encoded_bytes, encoded_sha256) = input.finish();
    Ok(StreamVerification {
        encoded_bytes,
        encoded_sha256,
        decoded_chunks,
    })
}

/// The digest of the encoded bytes the verifier read, checked against the
/// length the encoder wrote.
fn encoded_digest(
    actual: u64,
    sha256: String,
    encoded_bytes: u64,
) -> Result<String, RepresentationError> {
    if actual != encoded_bytes {
        return Err(RepresentationError::EncodedFileChanged {
            expected: encoded_bytes,
            actual,
        });
    }
    Ok(sha256)
}

/// Reads the decoded artifact to its end, checks its length and SHA-256, and
/// returns the SHA-256 of each consecutive `chunk_bytes` of it, the last
/// covering what is left, as `sha256:<hex>`.
///
/// Each chunk is read into one buffer, hashed into the whole digest here, and
/// handed to a second thread that digests it on its own while this one reads
/// the next. The chunk digests therefore name exactly the bytes whose length
/// and digest are checked, and none is returned unless that check passes.
fn verify_decoded(
    reader: &mut impl Read,
    expected_bytes: u64,
    expected_sha256: &str,
    chunk_bytes: usize,
    cancellation: &CancellationToken,
) -> Result<Vec<String>, RepresentationError> {
    let decoded = decode_chunks(reader, expected_bytes, chunk_bytes, cancellation)?;
    hold_decoded_digest(&decoded.sha256, expected_sha256)?;
    Ok(decoded.chunks)
}

/// What [`decode_chunks`] read: each chunk's digest and the whole's SHA-256.
struct DecodedChunks {
    chunks: Vec<String>,
    sha256: String,
}

/// [`verify_decoded`], except that it returns the SHA-256 of what it read
/// rather than holding it to an expected one.
fn decode_chunks(
    reader: &mut impl Read,
    expected_bytes: u64,
    chunk_bytes: usize,
    cancellation: &CancellationToken,
) -> Result<DecodedChunks, RepresentationError> {
    let mut verifier = DecodedVerifier::new(expected_bytes);
    let (whole, to_digest) = mpsc::sync_channel::<Vec<u8>>(CHUNK_DIGEST_QUEUE);
    let (spent, reusable) = mpsc::channel::<Vec<u8>>();
    let (read, digests) = std::thread::scope(|scope| {
        let digester = background_thread::spawn_scoped(scope, move || {
            to_digest
                .into_iter()
                .map(|chunk| {
                    let digest = format!("sha256:{}", hex::encode(Sha256::digest(&chunk)));
                    // Once the reader has stopped, the buffer is dropped.
                    let _ = spent.send(chunk);
                    digest
                })
                .collect::<Vec<_>>()
        });
        let read = read_chunks(
            reader,
            &mut verifier,
            chunk_bytes,
            &whole,
            &reusable,
            cancellation,
        );
        drop(whole);
        let digests = digester
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        (read, digests)
    });
    read?;
    Ok(DecodedChunks {
        chunks: digests,
        sha256: verifier.finish()?,
    })
}

fn hold_decoded_digest(decoded_sha256: &str, expected: &str) -> Result<(), RepresentationError> {
    if decoded_sha256 != expected {
        return Err(RepresentationError::DecodedDigestMismatch);
    }
    Ok(())
}

/// Reads `reader` to its end in chunks of `chunk_bytes`, the last possibly
/// shorter and none empty, writes each to `verifier`, then queues it on
/// `whole`. A buffer comes back on `reusable` once its digest is taken, and a
/// new one is allocated only when none has.
fn read_chunks(
    reader: &mut impl Read,
    verifier: &mut DecodedVerifier,
    chunk_bytes: usize,
    whole: &mpsc::SyncSender<Vec<u8>>,
    reusable: &mpsc::Receiver<Vec<u8>>,
    cancellation: &CancellationToken,
) -> Result<(), RepresentationError> {
    loop {
        let mut chunk = reusable.try_recv().unwrap_or_default();
        chunk.resize(chunk_bytes, 0);
        let mut filled = 0;
        while filled < chunk_bytes {
            ensure_not_cancelled(cancellation)?;
            let read = reader.read(&mut chunk[filled..])?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        if filled == 0 {
            return Ok(());
        }
        chunk.truncate(filled);
        verifier.write_all(&chunk)?;
        whole
            .send(chunk)
            .map_err(|_| io::Error::other("chunk digester stopped"))?;
        if filled < chunk_bytes {
            return Ok(());
        }
    }
}

#[cfg(test)]
fn hash_file(
    path: &Path,
    maximum_bytes: u64,
    cancellation: &CancellationToken,
) -> Result<(u64, String), RepresentationError> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = [0_u8; IO_BUFFER_BYTES];
    loop {
        ensure_not_cancelled(cancellation)?;
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(read as u64)
            .ok_or(RepresentationError::SizeOverflow)?;
        if bytes > maximum_bytes {
            return Err(RepresentationError::EncodedLimitExceeded {
                maximum: maximum_bytes,
            });
        }
        digest.update(&buffer[..read]);
    }
    Ok((bytes, hex::encode(digest.finalize())))
}

struct BoundedWriter<W> {
    inner: W,
    bytes: u64,
    maximum: u64,
    cancellation: CancellationToken,
}

impl<W> BoundedWriter<W> {
    fn new(inner: W, maximum: u64, cancellation: CancellationToken) -> Self {
        Self {
            inner,
            bytes: 0,
            maximum,
            cancellation,
        }
    }
}

impl<W: Write> BoundedWriter<W> {
    /// Returns the writer and the number of bytes written through it.
    fn finish(mut self) -> io::Result<(W, u64)> {
        self.inner.flush()?;
        Ok((self.inner, self.bytes))
    }
}

impl<W: Write> Write for BoundedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(io::Error::other(
                "snapshot representation preparation cancelled",
            ));
        }
        let remaining = self.maximum.saturating_sub(self.bytes);
        if remaining == 0 && !bytes.is_empty() {
            return Err(io::Error::other(
                "encoded artifact exceeds configured bound",
            ));
        }
        let limit = usize::try_from(remaining.min(bytes.len() as u64)).unwrap_or(bytes.len());
        let written = self.inner.write(&bytes[..limit])?;
        self.bytes = self
            .bytes
            .checked_add(written as u64)
            .ok_or_else(|| io::Error::other("encoded artifact size overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Writes to the representation file and queues every byte written for the
/// verifier. Once the verifier stops reading, writes fail and say so.
struct TeeWriter<W> {
    output: W,
    chunks: mpsc::SyncSender<Vec<u8>>,
    verifier_stopped: Arc<AtomicBool>,
}

impl<W: Write> Write for TeeWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.output.write(bytes)?;
        if written != 0 && self.chunks.send(bytes[..written].to_vec()).is_err() {
            self.verifier_stopped.store(true, Ordering::Relaxed);
            return Err(io::Error::other("the verifier stopped reading"));
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

/// Reads the chunks a [`TeeWriter`] queued, in order, and ends when the writer
/// closes the queue.
struct QueueReader {
    queue: mpsc::Receiver<Vec<u8>>,
    chunk: Vec<u8>,
    position: usize,
}

impl QueueReader {
    fn new(queue: mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            queue,
            chunk: Vec::new(),
            position: 0,
        }
    }
}

impl Read for QueueReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        while self.position == self.chunk.len() {
            match self.queue.recv() {
                Ok(chunk) => {
                    self.chunk = chunk;
                    self.position = 0;
                }
                Err(mpsc::RecvError) => return Ok(0),
            }
        }
        let read = bytes.len().min(self.chunk.len() - self.position);
        bytes[..read].copy_from_slice(&self.chunk[self.position..self.position + read]);
        self.position += read;
        Ok(read)
    }
}

struct CancellableReader<R> {
    inner: R,
    cancellation: CancellationToken,
}

impl<R> CancellableReader<R> {
    fn new(inner: R, cancellation: CancellationToken) -> Self {
        Self {
            inner,
            cancellation,
        }
    }
}

impl<R: Read> Read for CancellableReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(io::Error::other(
                "snapshot representation preparation cancelled",
            ));
        }
        self.inner.read(bytes)
    }
}

/// Hashes every byte read through it, so a pass that reads a file for another
/// reason also yields the file's length and digest.
struct HashingReader<R> {
    inner: R,
    digest: Sha256,
    bytes: u64,
}

impl<R> HashingReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            bytes: 0,
        }
    }

    fn finish(self) -> (R, u64, String) {
        (self.inner, self.bytes, hex::encode(self.digest.finalize()))
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(bytes)?;
        self.digest.update(&bytes[..read]);
        self.bytes = self
            .bytes
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("artifact size overflow"))?;
        Ok(read)
    }
}

fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<(), RepresentationError> {
    if cancellation.is_cancelled() {
        Err(RepresentationError::Cancelled)
    } else {
        Ok(())
    }
}

struct DecodedVerifier {
    digest: Sha256,
    bytes: u64,
    expected_bytes: u64,
}

impl DecodedVerifier {
    fn new(expected_bytes: u64) -> Self {
        Self {
            digest: Sha256::new(),
            bytes: 0,
            expected_bytes,
        }
    }

    /// The SHA-256 of what was written, once it is the expected length.
    fn finish(self) -> Result<String, RepresentationError> {
        if self.bytes != self.expected_bytes {
            return Err(RepresentationError::DecodedLengthMismatch {
                expected: self.expected_bytes,
                actual: self.bytes,
            });
        }
        Ok(hex::encode(self.digest.finalize()))
    }
}

impl Write for DecodedVerifier {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("decoded artifact size overflow"))?;
        if next > self.expected_bytes {
            return Err(io::Error::other("decoded artifact exceeds manifest length"));
        }
        self.digest.update(bytes);
        self.bytes = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum RepresentationError {
    #[error("snapshot representation preparation was cancelled")]
    Cancelled,
    #[error("artifact path has no parent directory")]
    MissingParentDirectory,
    #[error("encoded representation bound {maximum} is below mandatory identity bytes {required}")]
    EncodedLimitTooSmall { required: u64, maximum: u64 },
    #[error("encoded representation exceeds configured bound {maximum}")]
    EncodedLimitExceeded { maximum: u64 },
    #[error("artifact size overflow")]
    SizeOverflow,
    #[error("decoded artifact length changed: expected {expected}, got {actual}")]
    DecodedFileChanged { expected: u64, actual: u64 },
    #[error("encoded artifact length changed: expected {expected}, got {actual}")]
    EncodedFileChanged { expected: u64, actual: u64 },
    #[error("decoded representation length mismatch: expected {expected}, got {actual}")]
    DecodedLengthMismatch { expected: u64, actual: u64 },
    #[error("decoded representation SHA-256 mismatch")]
    DecodedDigestMismatch,
    #[error("zstd representation has an invalid frame header")]
    InvalidZstdFrame,
    #[error("zstd representation omits its decoded content size")]
    MissingZstdContentSize,
    #[error("zstd content size mismatch: expected {expected}, got {actual}")]
    ZstdContentSizeMismatch { expected: u64, actual: u64 },
    #[error("zstd dictionaries are forbidden")]
    ZstdDictionaryForbidden,
    #[error(
        "the decoded artifact is {decoded_bytes} bytes, more than the largest brotli window covers"
    )]
    BrotliWindowExceeded { decoded_bytes: u64 },
    #[error("the brotli stream does not declare a large window of {expected} bits")]
    BrotliWindowMismatch { expected: u8 },
    #[error("the artifact could not be packed: {0}")]
    Pack(#[source] PackError),
    #[error("the packed stream does not decode to a body of {expected} bytes")]
    PackedBodyLength { expected: u64 },
    #[error("the packed stream's body does not restore the artifact: {0}")]
    Unpack(#[from] UnpackError),
    #[error("encoded representation contains trailing bytes or multiple frames")]
    TrailingEncodedBytes,
    #[error(transparent)]
    Identifier(#[from] IdentifierError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    fn fixture() -> (tempfile::TempDir, PathBuf, Vec<u8>, String) {
        fixture_of(512 * 1024)
    }

    fn fixture_of(length: usize) -> (tempfile::TempDir, PathBuf, Vec<u8>, String) {
        written(
            (0..length)
                .map(|index| ((index * 31 + index / 4096) % 251) as u8)
                .collect(),
        )
    }

    /// A job and a half of pseudo-random letters from sixteen, in which the
    /// half MiB from 1 MiB on is pseudo-random over every byte value and
    /// repeats from the second job's first byte. The second job can match it
    /// only within its overlap: libzstd's default at this length, a quarter
    /// of the 4 MiB window, reaches it, and a sixteenth does not. Any five
    /// letters recur about once a MiB, so a job's positions overflow the rows
    /// of a hash table of `ZSTD_HASH_LOG`, which libzstd's default for the
    /// level does not.
    fn fixture_repeating_across_jobs() -> (tempfile::TempDir, PathBuf, Vec<u8>, String) {
        let job = ZSTD_JOB_BYTES as usize;
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 32) as u8
        };
        let mut bytes = (0..job + job / 2)
            .map(|_| b'a' + next() % 16)
            .collect::<Vec<_>>();
        for byte in &mut bytes[job / 2..job * 3 / 4] {
            *byte = next();
        }
        bytes.copy_within(job / 2..job * 3 / 4, job);
        written(bytes)
    }

    /// `bytes` in a file of their own, with their SHA-256.
    fn written(bytes: Vec<u8>) -> (tempfile::TempDir, PathBuf, Vec<u8>, String) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("artifact.calque");
        std::fs::write(&path, &bytes).unwrap();
        let digest = hex::encode(Sha256::digest(&bytes));
        (directory, path, bytes, digest)
    }

    #[test]
    fn produces_two_bounded_round_tripped_representations() {
        let (_directory, path, bytes, digest) = fixture();
        let prepared = PreparedRepresentations::prepare(
            &path,
            bytes.len() as u64,
            &digest,
            bytes.len() as u64 + 1024 * 1024,
        )
        .unwrap();
        let manifests = prepared.manifests();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].encoding, RepresentationEncoding::Zstd);
        assert_eq!(manifests[1].encoding, RepresentationEncoding::Identity);
        assert_eq!(manifests[0].zstd.as_ref().unwrap().level, ZSTD_LEVEL);
        assert_eq!(
            manifests[0].zstd.as_ref().unwrap().window_log,
            ZSTD_WINDOW_LOG
        );
        for entry in prepared.entries() {
            let (length, encoded_digest) = hash_file(
                entry.path(),
                bytes.len() as u64 + 1024 * 1024,
                &CancellationToken::new(),
            )
            .unwrap();
            assert_eq!(length, entry.manifest().encoded_bytes);
            assert_eq!(encoded_digest, entry.manifest().encoded_sha256);
        }
    }

    /// The published zstd object is the bytes libzstd writes from the decoded
    /// artifact at the level and window its manifest records, with no frame
    /// checksum, in jobs of `ZSTD_JOB_BYTES` with `ZSTD_OVERLAP_LOG` and
    /// `ZSTD_HASH_LOG`, and the re-encoded one is the bytes it writes at its
    /// own recorded level on one thread. The fixture spans two jobs and
    /// repeats across them from further back than the overlap reaches, so an
    /// encode in libzstd's default job size, overlap or hash table, or one on
    /// a different number of threads than the re-encode's, writes other
    /// bytes. The published object's reference runs on one worker where
    /// preparation runs `ZSTD_WORKERS`; the two agree only while libzstd's
    /// output does not depend on the worker count. A lowered thread publishes
    /// at the same level on itself alone.
    #[test]
    fn the_zstd_object_is_encoded_at_its_recorded_parameters() {
        let (_directory, path, bytes, digest) = fixture_repeating_across_jobs();
        let limit = bytes.len() as u64 + 1024 * 1024;
        let prepare =
            || PreparedRepresentations::prepare(&path, bytes.len() as u64, &digest, limit).unwrap();
        fn zstd_of(prepared: &PreparedRepresentations) -> &PreparedRepresentation {
            prepared
                .entries()
                .find(|entry| entry.manifest().encoding == RepresentationEncoding::Zstd)
                .unwrap()
        }
        let prepared = prepare();
        let published = zstd_of(&prepared);
        let lowered = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    background_thread::lower_current_thread().unwrap();
                    prepare()
                })
                .join()
                .unwrap()
        });
        let published_lowered = zstd_of(&lowered);
        assert_eq!(
            published_lowered.manifest().zstd.as_ref().unwrap().level,
            ZSTD_LEVEL
        );
        let (reencoded, _) = reencode_zstd_controlled(
            &path,
            bytes.len() as u64,
            &digest,
            limit,
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(
            reencoded.manifest().zstd.as_ref().unwrap().level,
            REENCODED_ZSTD_LEVEL
        );
        for (object, workers) in [(published, 1), (&reencoded, 0), (published_lowered, 0)] {
            let parameters = object.manifest().zstd.clone().unwrap();
            let mut encoder =
                zstd::stream::write::Encoder::new(Vec::new(), parameters.level).unwrap();
            if workers > 0 {
                encoder.multithread(workers).unwrap();
                encoder
                    .set_parameter(CParameter::JobSize(ZSTD_JOB_BYTES))
                    .unwrap();
                encoder
                    .set_parameter(CParameter::OverlapSizeLog(ZSTD_OVERLAP_LOG))
                    .unwrap();
                encoder
                    .set_parameter(CParameter::HashLog(ZSTD_HASH_LOG))
                    .unwrap();
            }
            encoder
                .window_log(u32::from(parameters.window_log))
                .unwrap();
            encoder.include_checksum(false).unwrap();
            encoder.include_contentsize(true).unwrap();
            encoder.include_dictid(false).unwrap();
            encoder
                .set_pledged_src_size(Some(bytes.len() as u64))
                .unwrap();
            encoder.write_all(&bytes).unwrap();
            assert!(
                encoder.finish().unwrap() == std::fs::read(object.path()).unwrap(),
                "level {}",
                parameters.level
            );
        }
    }

    /// The digest of each `chunk_bytes` of `bytes`, computed here on its own.
    fn chunk_digests(bytes: &[u8], chunk_bytes: usize) -> Vec<String> {
        bytes
            .chunks(chunk_bytes)
            .map(|chunk| format!("sha256:{}", hex::encode(Sha256::digest(chunk))))
            .collect()
    }

    #[test]
    fn the_decoded_chunks_are_digested_in_order_the_last_partial() {
        let (_directory, path, bytes, digest) = fixture();
        // 512 KiB in chunks of 100,000 bytes: five whole and 24,288 left.
        for (chunk_bytes, count) in [(100_000_u64, 6_usize), (128 * 1024, 4), (1 << 20, 1)] {
            let prepared = PreparedRepresentations::prepare_chunked(
                &path,
                bytes.len() as u64,
                &digest,
                bytes.len() as u64 + 1024 * 1024,
                chunk_bytes,
                &CancellationToken::new(),
            )
            .unwrap();
            let chunks = prepared.decoded_chunks();
            assert_eq!(chunks.bytes, chunk_bytes);
            assert_eq!(chunks.sha256.len(), count, "{chunk_bytes}");
            assert_eq!(
                chunks.sha256,
                chunk_digests(&bytes, chunk_bytes as usize),
                "{chunk_bytes}"
            );
        }
    }

    #[test]
    fn the_decoded_chunks_are_4_mib_by_default() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("artifact.calque");
        // Two whole chunks and a partial one.
        let bytes = (0..2 * (4 << 20) + 12_345)
            .map(|index: usize| (index.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect::<Vec<_>>();
        std::fs::write(&path, &bytes).unwrap();
        let prepared = PreparedRepresentations::prepare(
            &path,
            bytes.len() as u64,
            &hex::encode(Sha256::digest(&bytes)),
            bytes.len() as u64 + 1024 * 1024,
        )
        .unwrap();
        let chunks = prepared.decoded_chunks();
        assert_eq!(chunks.bytes, DIRECT_COLUMNAR_CHUNK_BYTES);
        assert_eq!(chunks.sha256, chunk_digests(&bytes, 4 << 20));
        assert_eq!(chunks.sha256.len(), 3);
    }

    #[test]
    fn no_chunk_digest_is_returned_for_bytes_that_fail_verification() {
        let (_directory, _path, bytes, digest) = fixture();
        let decode = |bytes: &[u8], expected_bytes: u64, expected_sha256: &str| {
            verify_decoded(
                &mut &bytes[..],
                expected_bytes,
                expected_sha256,
                100_000,
                &CancellationToken::new(),
            )
        };
        assert_eq!(
            decode(&bytes, bytes.len() as u64, &digest).unwrap(),
            chunk_digests(&bytes, 100_000)
        );
        let mut changed = bytes.clone();
        changed[bytes.len() - 1] ^= 0x01;
        assert!(matches!(
            decode(&changed, bytes.len() as u64, &digest),
            Err(RepresentationError::DecodedDigestMismatch)
        ));
        assert!(matches!(
            decode(&bytes[..bytes.len() - 1], bytes.len() as u64, &digest),
            Err(RepresentationError::DecodedLengthMismatch { .. })
        ));
        assert!(matches!(
            decode(&bytes, bytes.len() as u64 - 1, &digest),
            Err(RepresentationError::Io(_))
        ));
    }

    #[test]
    fn preparation_refuses_a_decoded_file_its_manifest_does_not_name() {
        let (directory, path, bytes, digest) = fixture();
        let prepare = || {
            PreparedRepresentations::prepare(
                &path,
                bytes.len() as u64,
                &digest,
                bytes.len() as u64 + 1024 * 1024,
            )
        };

        let mut changed = bytes.clone();
        changed[bytes.len() / 2] ^= 0x01;
        std::fs::write(&path, &changed).unwrap();
        assert!(matches!(
            prepare(),
            Err(RepresentationError::DecodedDigestMismatch)
        ));

        std::fs::write(&path, [&bytes[..], b"x"].concat()).unwrap();
        assert!(matches!(
            prepare(),
            Err(RepresentationError::DecodedFileChanged { .. })
        ));

        std::fs::write(&path, &bytes[..bytes.len() - 1]).unwrap();
        assert!(matches!(
            prepare(),
            Err(RepresentationError::DecodedFileChanged { .. })
        ));

        let left = std::fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(left, ["artifact.calque"]);
    }

    #[test]
    fn a_verified_length_other_than_the_written_one_is_refused() {
        let (_directory, _path, bytes, digest) = fixture();
        assert!(matches!(
            encoded_digest(bytes.len() as u64, digest, bytes.len() as u64 + 1),
            Err(RepresentationError::EncodedFileChanged { .. })
        ));
    }

    #[test]
    fn the_encoder_stops_when_its_verifier_does() {
        let (chunks, queue) = mpsc::sync_channel(1);
        let verifier_stopped = Arc::new(AtomicBool::new(false));
        let mut output = TeeWriter {
            output: Vec::new(),
            chunks,
            verifier_stopped: Arc::clone(&verifier_stopped),
        };
        output.write_all(b"first").unwrap();
        let mut reader = QueueReader::new(queue);
        let mut first = [0_u8; 3];
        reader.read_exact(&mut first).unwrap();
        assert_eq!(&first, b"fir");
        drop(reader);
        assert!(output.write_all(b"second").is_err());
        assert!(verifier_stopped.load(Ordering::Relaxed));
    }

    #[test]
    fn the_verifier_reads_the_queue_in_order_to_its_close() {
        let (chunks, queue) = mpsc::sync_channel(4);
        let verifier_stopped = Arc::new(AtomicBool::new(false));
        let mut output = TeeWriter {
            output: Vec::new(),
            chunks,
            verifier_stopped: Arc::clone(&verifier_stopped),
        };
        output.write_all(b"one").unwrap();
        output.write_all(b"").unwrap();
        output.write_all(b"two").unwrap();
        let TeeWriter { output, chunks, .. } = output;
        drop(chunks);
        let mut read = Vec::new();
        QueueReader::new(queue).read_to_end(&mut read).unwrap();
        assert_eq!(read, output);
        assert_eq!(read, b"onetwo");
        assert!(!verifier_stopped.load(Ordering::Relaxed));
    }

    fn write_beside(
        directory: &Path,
        bytes: &[u8],
        maximum: u64,
        cancellation: &CancellationToken,
    ) -> ZstdWhileWriting {
        let mut zstd = ZstdWhileWriting::new(directory, maximum, cancellation);
        zstd.begin(bytes.len() as u64).unwrap();
        // Uneven writes, as the artifact's buffered writer may issue them.
        let mut rest = bytes;
        for length in [7, 65_536, 1_000_003].into_iter().cycle() {
            if rest.is_empty() {
                break;
            }
            let (written, after) = rest.split_at(length.min(rest.len()));
            zstd.write_all(written).unwrap();
            rest = after;
        }
        zstd
    }

    #[test]
    fn zstd_written_beside_the_artifact_matches_zstd_encoded_from_its_file() {
        // More than two of the encoder's jobs, so that its workers share it.
        let (directory, path, bytes, digest) = fixture_of(5 * 1024 * 1024 + 123);
        let maximum = bytes.len() as u64 + 1024 * 1024;
        let from_file =
            PreparedRepresentations::prepare(&path, bytes.len() as u64, &digest, maximum).unwrap();
        let beside = write_beside(directory.path(), &bytes, maximum, &CancellationToken::new())
            .finish(&path, bytes.len() as u64, &digest)
            .unwrap();

        assert_eq!(beside.decoded_chunks(), from_file.decoded_chunks());
        for (beside, from_file) in beside.entries().zip(from_file.entries()) {
            assert_eq!(beside.manifest(), from_file.manifest());
            assert_eq!(
                std::fs::read(beside.path()).unwrap(),
                std::fs::read(from_file.path()).unwrap()
            );
        }
        assert_eq!(beside.entries().count(), 2);
    }

    #[test]
    fn zstd_written_beside_another_artifact_is_refused() {
        let (directory, path, bytes, _) = fixture();
        let other = hex::encode(Sha256::digest(b"another artifact"));
        assert!(matches!(
            write_beside(
                directory.path(),
                &bytes,
                bytes.len() as u64 + 1024 * 1024,
                &CancellationToken::new(),
            )
            .finish(&path, bytes.len() as u64, &other),
            Err(RepresentationError::DecodedDigestMismatch)
        ));
        assert!(matches!(
            write_beside(
                directory.path(),
                &bytes,
                bytes.len() as u64 + 1024 * 1024,
                &CancellationToken::new(),
            )
            .finish(&path, bytes.len() as u64 + 1, &other),
            Err(RepresentationError::DecodedLengthMismatch { .. })
        ));
    }

    #[test]
    fn a_zstd_encode_that_cannot_hold_the_artifact_refuses_to_begin_and_says_why() {
        let (directory, path, bytes, digest) = fixture();
        let maximum = bytes.len() as u64 - 1;
        let cancellation = CancellationToken::new();

        let mut zstd = ZstdWhileWriting::new(directory.path(), maximum, &cancellation);
        assert!(zstd.begin(bytes.len() as u64).is_err());
        assert!(zstd.write_all(&bytes).is_err());
        assert!(matches!(
            zstd.abandon(),
            Some(RepresentationError::EncodedLimitTooSmall { .. })
        ));

        let mut zstd = ZstdWhileWriting::new(directory.path(), maximum, &cancellation);
        assert!(zstd.begin(bytes.len() as u64).is_err());
        assert!(matches!(
            zstd.finish(&path, bytes.len() as u64, &digest),
            Err(RepresentationError::EncodedLimitTooSmall { .. })
        ));
    }

    #[test]
    fn a_cancelled_zstd_encode_neither_begins_nor_blames_its_verifier() {
        let (directory, _path, bytes, _) = fixture();
        let maximum = bytes.len() as u64 + 1024 * 1024;
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut zstd = ZstdWhileWriting::new(directory.path(), maximum, &cancellation);
        assert!(zstd.begin(bytes.len() as u64).is_err());
        assert!(matches!(
            zstd.abandon(),
            Some(RepresentationError::Cancelled)
        ));

        // A write that fails on its own leaves the write's error to stand.
        let zstd = write_beside(directory.path(), &bytes, maximum, &CancellationToken::new());
        assert!(zstd.abandon().is_none());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    /// An encode whose verifier has already stopped, with `failure`.
    fn zstd_beside_a_stopped_verifier(
        directory: &Path,
        decoded_bytes: u64,
        failure: RepresentationError,
    ) -> ZstdWhileWriting {
        let cancellation = CancellationToken::new();
        let temporary = NamedTempFile::new_in(directory).unwrap();
        let (chunks, queue) = mpsc::sync_channel(VERIFIER_QUEUE_CHUNKS);
        drop(queue);
        let verifier_stopped = Arc::new(AtomicBool::new(false));
        let output = TeeWriter {
            output: BoundedWriter::new(temporary.reopen().unwrap(), u64::MAX, cancellation.clone()),
            chunks,
            verifier_stopped: Arc::clone(&verifier_stopped),
        };
        ZstdWhileWriting {
            directory: directory.to_path_buf(),
            max_encoded_bytes: u64::MAX,
            cancellation,
            encode: Some(ZstdEncode {
                started: Instant::now(),
                encoding: PUBLISHED_ZSTD,
                decoded_bytes,
                temporary,
                encoder: zstd_encoder(output, PUBLISHED_ZSTD, decoded_bytes).unwrap(),
                verifier: std::thread::spawn(move || Err(failure)),
                verifier_stopped,
            }),
            refused: None,
        }
    }

    #[test]
    fn a_zstd_encode_whose_verifier_stopped_reports_the_verifiers_error() {
        let (directory, path, bytes, digest) = fixture();
        let mut zstd = zstd_beside_a_stopped_verifier(
            directory.path(),
            bytes.len() as u64,
            RepresentationError::TrailingEncodedBytes,
        );
        // Whether this write fails depends on when the encoder first emits.
        let _ = zstd.write_all(&bytes);
        assert!(matches!(
            zstd.finish(&path, bytes.len() as u64, &digest),
            Err(RepresentationError::TrailingEncodedBytes)
        ));

        let mut zstd = zstd_beside_a_stopped_verifier(
            directory.path(),
            bytes.len() as u64,
            RepresentationError::TrailingEncodedBytes,
        );
        zstd.write_all(&bytes[..1024]).unwrap();
        let encode = zstd.encode.as_mut().unwrap();
        // The artifact's write fails once the encoder emits its first block.
        assert!(encode.encoder.flush().is_err());
        assert!(matches!(
            zstd.abandon(),
            Some(RepresentationError::TrailingEncodedBytes)
        ));
    }

    #[test]
    fn mandatory_identity_makes_the_bound_explicit() {
        let (_directory, path, bytes, digest) = fixture();
        assert!(matches!(
            PreparedRepresentations::prepare(
                &path,
                bytes.len() as u64,
                &digest,
                bytes.len() as u64 - 1,
            ),
            Err(RepresentationError::EncodedLimitTooSmall { .. })
        ));
    }

    #[test]
    fn pre_cancelled_preparation_creates_no_representation() {
        let (_directory, path, bytes, digest) = fixture();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert!(matches!(
            PreparedRepresentations::prepare_controlled(
                &path,
                bytes.len() as u64,
                &digest,
                bytes.len() as u64 + 1024 * 1024,
                &cancellation,
            ),
            Err(RepresentationError::Cancelled)
        ));
    }

    #[test]
    fn zstd_verifier_rejects_trailing_frames_and_window_bombs() {
        let (_directory, path, bytes, digest) = fixture();
        let prepared = PreparedRepresentations::prepare(
            &path,
            bytes.len() as u64,
            &digest,
            bytes.len() as u64 + 1024 * 1024,
        )
        .unwrap();
        let zstd = prepared.entries().next().unwrap();
        let copied = tempfile::NamedTempFile::new().unwrap();
        std::fs::copy(zstd.path(), copied.path()).unwrap();
        let mut output = std::fs::OpenOptions::new()
            .append(true)
            .open(copied.path())
            .unwrap();
        output.write_all(b"trailing").unwrap();
        let length = output.metadata().unwrap().len();
        assert!(matches!(
            verify_zstd(
                copied.path(),
                length,
                bytes.len() as u64,
                &digest,
                &CancellationToken::new(),
            ),
            Err(RepresentationError::TrailingEncodedBytes)
        ));

        let bomb_bytes = (0..9 * 1024 * 1024)
            .map(|index| ((index * 31 + index / 4096) % 251) as u8)
            .collect::<Vec<_>>();
        let bomb_digest = hex::encode(Sha256::digest(&bomb_bytes));
        let bomb = tempfile::NamedTempFile::new().unwrap();
        let mut encoder = zstd::stream::write::Encoder::new(bomb.reopen().unwrap(), 1).unwrap();
        encoder.window_log(24).unwrap();
        encoder
            .set_pledged_src_size(Some(bomb_bytes.len() as u64))
            .unwrap();
        encoder.write_all(&bomb_bytes).unwrap();
        encoder.finish().unwrap().sync_all().unwrap();
        assert!(verify_zstd(
            bomb.path(),
            bomb.as_file().metadata().unwrap().len(),
            bomb_bytes.len() as u64,
            &bomb_digest,
            &CancellationToken::new(),
        )
        .is_err());
    }

    /// Preparation writes no frame checksum, and the verifier still accepts a
    /// frame that carries one: the decoded SHA-256 and chunk digests bind the
    /// bytes either way.
    #[test]
    fn zstd_verifier_accepts_a_frame_with_a_checksum() {
        let (_directory, _path, bytes, digest) = fixture();
        let checksummed = tempfile::NamedTempFile::new().unwrap();
        let mut encoder =
            zstd::stream::write::Encoder::new(checksummed.reopen().unwrap(), ZSTD_LEVEL).unwrap();
        encoder.window_log(u32::from(ZSTD_WINDOW_LOG)).unwrap();
        encoder.include_checksum(true).unwrap();
        encoder.include_contentsize(true).unwrap();
        encoder.include_dictid(false).unwrap();
        encoder
            .set_pledged_src_size(Some(bytes.len() as u64))
            .unwrap();
        encoder.write_all(&bytes).unwrap();
        encoder.finish().unwrap().sync_all().unwrap();
        let encoded = std::fs::read(checksummed.path()).unwrap();
        assert_eq!(encoded[4] & 0b0000_0100, 0b0000_0100);

        assert_eq!(
            verify_zstd(
                checksummed.path(),
                encoded.len() as u64,
                bytes.len() as u64,
                &digest,
                &CancellationToken::new(),
            )
            .unwrap(),
            hex::encode(Sha256::digest(&encoded))
        );
    }

    #[test]
    fn zstd_verifier_rejects_truncation_corruption_and_wrong_decoded_identity() {
        let (_directory, path, bytes, digest) = fixture();

        let prepared = PreparedRepresentations::prepare(
            &path,
            bytes.len() as u64,
            &digest,
            bytes.len() as u64 + 1024 * 1024,
        )
        .unwrap();
        let zstd = prepared.entries().next().unwrap();
        let truncated = tempfile::NamedTempFile::new().unwrap();
        let mut encoded = std::fs::read(zstd.path()).unwrap();
        encoded.truncate(encoded.len() - 1);
        std::fs::write(truncated.path(), &encoded).unwrap();
        assert!(verify_zstd(
            truncated.path(),
            encoded.len() as u64,
            bytes.len() as u64,
            &digest,
            &CancellationToken::new(),
        )
        .is_err());

        assert!(matches!(
            verify_zstd(
                zstd.path(),
                zstd.manifest().encoded_bytes,
                bytes.len() as u64 - 1,
                &digest,
                &CancellationToken::new(),
            ),
            Err(RepresentationError::ZstdContentSizeMismatch { .. })
        ));
        assert!(matches!(
            verify_zstd(
                zstd.path(),
                zstd.manifest().encoded_bytes,
                bytes.len() as u64,
                &"0".repeat(64),
                &CancellationToken::new(),
            ),
            Err(RepresentationError::DecodedDigestMismatch)
        ));

        let corrupted = tempfile::NamedTempFile::new().unwrap();
        let mut corrupted_bytes = std::fs::read(zstd.path()).unwrap();
        let corrupt_at = corrupted_bytes.len() / 2;
        corrupted_bytes[corrupt_at] ^= 0x40;
        std::fs::write(corrupted.path(), &corrupted_bytes).unwrap();
        assert!(verify_zstd(
            corrupted.path(),
            corrupted_bytes.len() as u64,
            bytes.len() as u64,
            &digest,
            &CancellationToken::new(),
        )
        .is_err());
    }

    #[test]
    fn zstd_verifier_rejects_dictionary_frames() {
        let (_directory, _path, bytes, digest) = fixture();
        let samples = bytes
            .chunks(4096)
            .take(64)
            .map(<[u8]>::to_vec)
            .collect::<Vec<_>>();
        let dictionary = zstd::dict::from_samples(&samples, 4096).unwrap();
        let encoded = tempfile::NamedTempFile::new().unwrap();
        let mut encoder = zstd::stream::write::Encoder::with_dictionary(
            encoded.reopen().unwrap(),
            ZSTD_LEVEL,
            &dictionary,
        )
        .unwrap();
        encoder.include_checksum(false).unwrap();
        encoder.include_contentsize(true).unwrap();
        encoder.include_dictid(true).unwrap();
        encoder
            .set_pledged_src_size(Some(bytes.len() as u64))
            .unwrap();
        encoder.write_all(&bytes).unwrap();
        encoder.finish().unwrap().sync_all().unwrap();

        assert!(matches!(
            verify_zstd(
                encoded.path(),
                encoded.as_file().metadata().unwrap().len(),
                bytes.len() as u64,
                &digest,
                &CancellationToken::new(),
            ),
            Err(RepresentationError::ZstdDictionaryForbidden)
        ));
    }

    /// Word-like bytes, which brotli's qualities write at different lengths.
    fn words_fixture(length: usize) -> (tempfile::TempDir, PathBuf, Vec<u8>, String) {
        const WORDS: [&str; 12] = [
            "incident", "latency", "deploy", "rollback", "checkout", "timeout", "p95", "region",
            "eu-west", "retry", "upstream", "degraded",
        ];
        let mut state = 0x2545_f491_u32;
        let mut bytes = Vec::with_capacity(length + 16);
        while bytes.len() < length {
            state = state.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            bytes.extend_from_slice(WORDS[(state >> 16) as usize % WORDS.len()].as_bytes());
            bytes.push(if state.is_multiple_of(7) { b'\n' } else { b' ' });
        }
        bytes.truncate(length);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("artifact.calque");
        std::fs::write(&path, &bytes).unwrap();
        let digest = hex::encode(Sha256::digest(&bytes));
        (directory, path, bytes, digest)
    }

    /// The stream the encoder library writes from `bytes` at these parameters.
    fn brotli_of(bytes: &[u8], quality: u8, window_bits: u8, large_window: bool) -> Vec<u8> {
        let params = BrotliEncoderParams {
            quality: i32::from(quality),
            lgwin: i32::from(window_bits),
            large_window,
            size_hint: bytes.len(),
            ..BrotliEncoderParams::default()
        };
        let mut input = bytes;
        let mut encoded = Vec::new();
        brotli::BrotliCompress(&mut input, &mut encoded, &params).unwrap();
        encoded
    }

    fn brotli_decoded(encoded: &[u8]) -> Vec<u8> {
        let mut decoded = Vec::new();
        brotli::Decompressor::new(encoded, IO_BUFFER_BYTES)
            .read_to_end(&mut decoded)
            .unwrap();
        decoded
    }

    fn files_in(directory: &tempfile::TempDir) -> Vec<std::ffi::OsString> {
        std::fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect()
    }

    /// The object is one stream at the quality and window its manifest
    /// records: the bytes the library writes at them, and other bytes at
    /// another quality. 64 KiB is covered by a 2^16 window.
    #[test]
    fn the_brotli_object_is_one_large_window_stream_at_its_recorded_parameters() {
        let (directory, path, bytes, digest) = words_fixture(64 * 1024);
        let length = bytes.len() as u64;
        let (object, chunks) = encode_brotli_large_window_controlled(
            &path,
            length,
            &digest,
            length,
            &CancellationToken::new(),
        )
        .unwrap();
        let manifest = object.manifest().clone();
        assert_eq!(manifest.encoding, RepresentationEncoding::BrotliLargeWindow);
        assert_eq!(manifest.zstd, None);
        let recorded = manifest.brotli.unwrap();
        assert_eq!(
            recorded,
            BrotliParameters {
                quality: BROTLI_QUALITY,
                window_bits: 16
            }
        );
        let encoded = std::fs::read(object.path()).unwrap();
        assert_eq!(encoded.len() as u64, manifest.encoded_bytes);
        assert_eq!(
            hex::encode(Sha256::digest(&encoded)),
            manifest.encoded_sha256
        );
        assert_eq!(
            manifest.representation_id,
            format!("sha256:{}", manifest.encoded_sha256)
        );
        assert_eq!(
            [encoded[0], encoded[1] & 0x3f],
            [BROTLI_LARGE_WINDOW_MARKER, 16]
        );
        assert_eq!(
            encoded,
            brotli_of(&bytes, recorded.quality, recorded.window_bits, true)
        );
        let other = if recorded.quality == 9 { 10 } else { 9 };
        assert_ne!(
            encoded,
            brotli_of(&bytes, other, recorded.window_bits, true)
        );
        assert_eq!(brotli_decoded(&encoded), bytes);
        assert_eq!(
            chunks,
            chunk_digests(&bytes, DIRECT_COLUMNAR_CHUNK_BYTES as usize)
        );
        drop(object);
        assert_eq!(files_in(&directory), ["artifact.calque"]);
    }

    fn verify_brotli(
        stream: impl Read,
        bytes: &[u8],
        window_bits: u8,
    ) -> Result<StreamVerification, RepresentationError> {
        verify_brotli_stream(
            stream,
            bytes.len() as u64,
            &hex::encode(Sha256::digest(bytes)),
            window_bits,
            16 * 1024,
            &CancellationToken::new(),
        )
    }

    /// The client's decoder refuses a wider window than the artifact needs and
    /// a body that continues after its stream, so the verifier does too.
    #[test]
    fn the_brotli_verifier_refuses_a_stream_the_client_would_not_decode() {
        let (_directory, _path, bytes, _digest) = words_fixture(64 * 1024);
        let stream = brotli_of(&bytes, 5, 16, true);
        let verified = verify_brotli(stream.as_slice(), &bytes, 16).unwrap();
        assert_eq!(verified.encoded_bytes, stream.len() as u64);
        assert_eq!(
            verified.encoded_sha256,
            hex::encode(Sha256::digest(&stream))
        );
        assert_eq!(verified.decoded_chunks, chunk_digests(&bytes, 16 * 1024));

        for refused in [
            brotli_of(&bytes, 5, 17, true),
            brotli_of(&bytes, 5, 16, false),
        ] {
            assert!(matches!(
                verify_brotli(refused.as_slice(), &bytes, 16),
                Err(RepresentationError::BrotliWindowMismatch { expected: 16 })
            ));
        }
        // A trailing byte the decoder has already buffered, and one it never
        // reads because the stream ended first.
        let buffered = [stream.as_slice(), b"x"].concat();
        assert!(matches!(
            verify_brotli(buffered.as_slice(), &bytes, 16),
            Err(RepresentationError::TrailingEncodedBytes)
        ));
        assert!(matches!(
            verify_brotli(stream.as_slice().chain(&b"x"[..]), &bytes, 16),
            Err(RepresentationError::TrailingEncodedBytes)
        ));
        assert!(matches!(
            verify_brotli(&stream[..stream.len() - 1], &bytes, 16),
            Err(RepresentationError::Io(_))
        ));
        let mut other = bytes.clone();
        other[100] ^= 1;
        assert!(matches!(
            verify_brotli(stream.as_slice(), &other, 16),
            Err(RepresentationError::DecodedDigestMismatch)
        ));
        assert!(verify_brotli(stream.as_slice(), &bytes[..bytes.len() - 1], 16).is_err());
        let mut corrupt = stream.clone();
        let middle = corrupt.len() / 2;
        corrupt[middle] ^= 0x55;
        assert!(verify_brotli(corrupt.as_slice(), &bytes, 16).is_err());
    }

    #[test]
    fn a_brotli_encode_that_is_not_under_its_bound_fails_and_leaves_no_file() {
        let (directory, path, bytes, digest) = words_fixture(64 * 1024);
        let size = brotli_of(&bytes, BROTLI_QUALITY, 16, true).len() as u64;
        let encode = |limit: u64| {
            encode_brotli_large_window_controlled(
                &path,
                bytes.len() as u64,
                &digest,
                limit,
                &CancellationToken::new(),
            )
        };
        assert!(encode(size - 1).is_err());
        assert_eq!(files_in(&directory), ["artifact.calque"]);
        let (object, _) = encode(size).unwrap();
        assert_eq!(object.manifest().encoded_bytes, size);
    }

    #[test]
    fn a_brotli_encode_refuses_a_cancelled_token_an_uncoverable_length_and_a_changed_file() {
        let (directory, path, bytes, digest) = words_fixture(64 * 1024);
        let length = bytes.len() as u64;
        let encode = |decoded_bytes: u64, cancellation: &CancellationToken| {
            encode_brotli_large_window_controlled(
                &path,
                decoded_bytes,
                &digest,
                length,
                cancellation,
            )
        };
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            encode(length, &cancelled),
            Err(RepresentationError::Cancelled)
        ));
        assert!(matches!(
            encode((1 << 30) + 1, &CancellationToken::new()),
            Err(RepresentationError::BrotliWindowExceeded { decoded_bytes }) if decoded_bytes == (1 << 30) + 1
        ));
        for changed in [
            [bytes.as_slice(), b"x"].concat(),
            bytes[..bytes.len() - 1].to_vec(),
        ] {
            std::fs::write(&path, &changed).unwrap();
            assert!(matches!(
                encode(length, &CancellationToken::new()),
                Err(RepresentationError::DecodedFileChanged { expected, .. }) if expected == length
            ));
        }
        assert_eq!(files_in(&directory), ["artifact.calque"]);
    }

    /// A checked-in artifact of `spec/fixtures/packed-artifact` in a directory
    /// of its own, and the body the packer makes of it.
    fn packed_fixture(case: &str) -> (tempfile::TempDir, PathBuf, Vec<u8>, String, Vec<u8>) {
        let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../spec/fixtures/packed-artifact")
            .join(case);
        let artifact = std::fs::read(fixtures.join("artifact.bin")).unwrap();
        let body = std::fs::read(fixtures.join("packed.bin")).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("artifact.calque");
        std::fs::write(&path, &artifact).unwrap();
        let digest = hex::encode(Sha256::digest(&artifact));
        (directory, path, artifact, digest, body)
    }

    /// The object is the packer's body as one stream at the quality and the
    /// window its manifest records, the window covering the body and not the
    /// artifact, and the chunks returned are the restored artifact's.
    #[test]
    fn the_packed_object_is_the_packer_s_body_as_one_stream_and_restores_the_artifact() {
        let (directory, path, artifact, digest, body) = packed_fixture("every-kind");
        let length = artifact.len() as u64;
        let packed = pack_artifact_controlled(&path, length, &CancellationToken::new())
            .unwrap()
            .unwrap();
        assert_eq!(packed.body_bytes(), body.len() as u64);
        assert_eq!(std::fs::read(packed.body.path()).unwrap(), body);
        let (object, chunks) = encode_packed_brotli_large_window_controlled(
            &packed,
            length,
            &digest,
            length,
            &CancellationToken::new(),
        )
        .unwrap();
        let manifest = object.manifest().clone();
        assert_eq!(
            manifest.encoding,
            RepresentationEncoding::PackedBrotliLargeWindow
        );
        assert_eq!(manifest.zstd, None);
        // 66,865 bytes of body need a 2^17 window, as the artifact's 72,714
        // do. The next test has a body that needs a smaller one.
        assert_eq!(
            manifest.brotli,
            Some(BrotliParameters {
                quality: BROTLI_QUALITY,
                window_bits: 17
            })
        );
        assert_eq!(
            manifest.packed,
            Some(PackedParameters {
                version: PACKED_BODY_VERSION,
                body_bytes: body.len() as u64
            })
        );
        let encoded = std::fs::read(object.path()).unwrap();
        assert_eq!(encoded.len() as u64, manifest.encoded_bytes);
        assert_eq!(
            hex::encode(Sha256::digest(&encoded)),
            manifest.encoded_sha256
        );
        assert_eq!(
            manifest.representation_id,
            format!("sha256:{}", manifest.encoded_sha256)
        );
        assert_eq!(encoded, brotli_of(&body, BROTLI_QUALITY, 17, true));
        assert_eq!(unpack(&brotli_decoded(&encoded), length).unwrap(), artifact);
        assert_eq!(
            chunks,
            chunk_digests(&artifact, DIRECT_COLUMNAR_CHUNK_BYTES as usize)
        );
        drop(object);
        drop(packed);
        assert_eq!(files_in(&directory), ["artifact.calque"]);
    }

    /// An artifact of two tables, 100 owners and `items` rows that each name
    /// one of them, which the packer writes as a byte a row instead of 16.
    fn references_artifact(items: usize) -> Vec<u8> {
        use direct_columnar::{
            schema_hash, ArtifactIdentity, ColumnKind, ColumnSchema, Encoder, EncoderLimits,
            TableSchema, Value, WorkspaceSchema,
        };
        const OWNERS: usize = 100;
        let table = |name: &str, columns: &[&str]| TableSchema {
            name: name.to_owned(),
            primary_key: vec!["id".to_owned()],
            columns: columns
                .iter()
                .map(|&name| ColumnSchema {
                    name: name.to_owned(),
                    kind: ColumnKind::Uuid,
                    nullable: false,
                })
                .collect(),
        };
        let schema = WorkspaceSchema {
            tables: vec![table("items", &["id", "owner"]), table("owners", &["id"])],
        };
        let mut encoder = Encoder::new(
            ArtifactIdentity {
                source_epoch: "source-epoch-1".to_owned(),
                manifest_version: "manifest-1".to_owned(),
                collection_set_hash: format!("sha256:{}", "1".repeat(64)),
                schema_hash: schema_hash(&schema).unwrap(),
                authorization_scope_version: "scope-1".to_owned(),
                authorization_scope_hash: format!("sha256:{}", "2".repeat(64)),
                cursor: "cursor-1".to_owned(),
            },
            schema,
            EncoderLimits::default(),
        )
        .unwrap();
        let mut state = 0x2545_f491_u64;
        let mut uuid = || {
            let mut id = [0_u8; 16];
            for half in id.chunks_exact_mut(8) {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                half.copy_from_slice(&(state | 1).to_le_bytes());
            }
            id
        };
        let owners: Vec<[u8; 16]> = (0..OWNERS).map(|_| uuid()).collect();
        for item in 0..items {
            let owner = owners[(item * 7) % OWNERS];
            encoder
                .push_row("items", &[Value::Uuid(uuid()), Value::Uuid(owner)])
                .unwrap();
        }
        for owner in owners {
            encoder.push_row("owners", &[Value::Uuid(owner)]).unwrap();
        }
        let mut artifact = Vec::new();
        encoder.finish(&mut artifact).unwrap();
        artifact
    }

    /// The window is the smallest that covers the body, where the artifact's
    /// own is one bit wider.
    #[test]
    fn the_packed_object_s_window_covers_its_body_and_not_its_artifact() {
        let artifact = references_artifact(3_000);
        let length = artifact.len() as u64;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("artifact.calque");
        std::fs::write(&path, &artifact).unwrap();
        let packed = pack_artifact_controlled(&path, length, &CancellationToken::new())
            .unwrap()
            .unwrap();
        assert_eq!(brotli_covering_window_bits(length), Some(17));
        assert_eq!(brotli_covering_window_bits(packed.body_bytes()), Some(16));
        let (object, chunks) = encode_packed_brotli_large_window_controlled(
            &packed,
            length,
            &hex::encode(Sha256::digest(&artifact)),
            length,
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(
            object.manifest().brotli,
            Some(BrotliParameters {
                quality: BROTLI_QUALITY,
                window_bits: 16
            })
        );
        let encoded = std::fs::read(object.path()).unwrap();
        assert_eq!(
            [encoded[0], encoded[1] & 0x3f],
            [BROTLI_LARGE_WINDOW_MARKER, 16]
        );
        assert_eq!(unpack(&brotli_decoded(&encoded), length).unwrap(), artifact);
        assert_eq!(
            chunks,
            chunk_digests(&artifact, DIRECT_COLUMNAR_CHUNK_BYTES as usize)
        );
    }

    /// One table of a key and 24 flags over five rows, which the packer
    /// transforms no column of. Each flag column ends in three bytes of
    /// padding, so a body that leaves the padding out is shorter than the
    /// artifact by more than the body's own framing.
    fn flags_artifact() -> Vec<u8> {
        use direct_columnar::{
            schema_hash, ArtifactIdentity, ColumnKind, ColumnSchema, Encoder, EncoderLimits,
            TableSchema, Value, WorkspaceSchema,
        };
        const FLAGS: usize = 24;
        let column = |name: String, kind| ColumnSchema {
            name,
            kind,
            nullable: false,
        };
        let schema = WorkspaceSchema {
            tables: vec![TableSchema {
                name: "flags".to_owned(),
                primary_key: vec!["id".to_owned()],
                columns: std::iter::once(column("id".to_owned(), ColumnKind::Uuid))
                    .chain((0..FLAGS).map(|flag| column(format!("flag_{flag}"), ColumnKind::Bool)))
                    .collect(),
            }],
        };
        let mut encoder = Encoder::new(
            ArtifactIdentity {
                source_epoch: "source-epoch-1".to_owned(),
                manifest_version: "manifest-1".to_owned(),
                collection_set_hash: format!("sha256:{}", "1".repeat(64)),
                schema_hash: schema_hash(&schema).unwrap(),
                authorization_scope_version: "scope-1".to_owned(),
                authorization_scope_hash: format!("sha256:{}", "2".repeat(64)),
                cursor: "cursor-1".to_owned(),
            },
            schema,
            EncoderLimits::default(),
        )
        .unwrap();
        for row in 0..5_u8 {
            let mut values = vec![Value::Uuid([row + 1; 16])];
            values.extend(
                (0..FLAGS).map(|flag| Value::Bool((flag + row as usize).is_multiple_of(3))),
            );
            encoder.push_row("flags", &values).unwrap();
        }
        let mut artifact = Vec::new();
        encoder.finish(&mut artifact).unwrap();
        artifact
    }

    /// An artifact is packed only where the packer transforms a column.
    /// A body can be shorter than its artifact by the padding alone, and
    /// that one is not written either.
    #[test]
    fn a_body_shorter_by_its_padding_alone_is_not_packed() {
        let artifact = flags_artifact();
        let packed = pack_unless(&artifact, &|| false).unwrap();
        assert!(!packed.stored && packed.body.len() < artifact.len());
        assert!(packed
            .columns
            .iter()
            .all(|column| column.transform.is_none()));
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("artifact.calque");
        std::fs::write(&path, &artifact).unwrap();
        let packed =
            pack_artifact_controlled(&path, artifact.len() as u64, &CancellationToken::new())
                .unwrap();
        assert!(packed.is_none());
        assert_eq!(files_in(&directory), ["artifact.calque"]);
    }

    /// An artifact no brotli window covers is refused before it is read, as
    /// the plain encode refuses it, whatever its body's length would be.
    #[test]
    fn a_pack_refuses_an_artifact_no_window_covers_before_it_reads_it() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("artifact.calque");
        std::fs::write(&path, b"not read").unwrap();
        assert!(matches!(
            pack_artifact_controlled(&path, (1 << 30) + 1, &CancellationToken::new()),
            Err(RepresentationError::BrotliWindowExceeded { decoded_bytes })
                if decoded_bytes == (1 << 30) + 1
        ));
        assert_eq!(files_in(&directory), ["artifact.calque"]);
    }

    /// The two checked-in artifacts the packer would not shorten: nothing is
    /// written, and the caller publishes the plain object.
    #[test]
    fn an_artifact_the_packer_transforms_no_column_of_is_not_packed() {
        for case in ["stored", "emit-fixture"] {
            let (directory, path, artifact, _digest, body) = packed_fixture(case);
            assert_eq!(body, artifact);
            let packed =
                pack_artifact_controlled(&path, artifact.len() as u64, &CancellationToken::new())
                    .unwrap();
            assert!(packed.is_none(), "{case}");
            assert_eq!(files_in(&directory), ["artifact.calque"]);
        }
    }

    #[test]
    fn a_pack_refuses_a_file_of_another_length_and_bytes_that_are_no_artifact() {
        let (directory, path, artifact, _digest, _body) = packed_fixture("every-kind");
        let length = artifact.len() as u64;
        for changed in [
            [artifact.as_slice(), b"x"].concat(),
            artifact[..artifact.len() - 1].to_vec(),
        ] {
            std::fs::write(&path, &changed).unwrap();
            assert!(matches!(
                pack_artifact_controlled(&path, length, &CancellationToken::new()),
                Err(RepresentationError::DecodedFileChanged { expected, actual })
                    if expected == length && actual == changed.len() as u64
            ));
        }
        let mut other = artifact.clone();
        other[..8].copy_from_slice(b"CALQART0");
        std::fs::write(&path, &other).unwrap();
        assert!(matches!(
            pack_artifact_controlled(&path, length, &CancellationToken::new()),
            Err(RepresentationError::Pack(_))
        ));
        assert_eq!(files_in(&directory), ["artifact.calque"]);
    }

    /// The pack asks its token and nothing else: a cancelled token ends it,
    /// and so does a cancel that arrives at any later ask, with no body left
    /// behind. An ask that is never answered yes changes nothing.
    #[test]
    fn a_cancel_at_any_ask_ends_the_pack_and_leaves_no_body() {
        let (directory, path, artifact, _digest, body) = packed_fixture("every-kind");
        let length = artifact.len() as u64;
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            pack_artifact_controlled(&path, length, &cancelled),
            Err(RepresentationError::Cancelled)
        ));
        // Before the file is read: one that is not there is not missed.
        assert!(matches!(
            pack_artifact_controlled(&directory.path().join("absent"), length, &cancelled),
            Err(RepresentationError::Cancelled)
        ));
        assert!(matches!(
            pack_artifact_controlled(
                &directory.path().join("absent"),
                length,
                &CancellationToken::new()
            ),
            Err(RepresentationError::Io(_))
        ));

        let asks = Cell::new(0_usize);
        let packed = pack_artifact(&path, length, &|| {
            asks.set(asks.get() + 1);
            false
        })
        .unwrap()
        .unwrap();
        assert_eq!(std::fs::read(packed.body.path()).unwrap(), body);
        drop(packed);
        let total = asks.get();
        assert!(total > 30, "{total} asks");
        let at = (0..30).chain((30..total).step_by(17)).chain([total - 1]);
        for cancel_at in at {
            let asked = Cell::new(0_usize);
            let result = pack_artifact(&path, length, &|| {
                let ask = asked.get();
                asked.set(ask + 1);
                ask == cancel_at
            });
            assert!(
                matches!(result, Err(RepresentationError::Cancelled)),
                "ask {cancel_at}"
            );
            assert_eq!(asked.get(), cancel_at + 1, "ask {cancel_at}");
            assert_eq!(files_in(&directory), ["artifact.calque"]);
        }
    }

    fn verify_packed(
        stream: impl Read,
        body_bytes: u64,
        artifact: &[u8],
    ) -> Result<StreamVerification, RepresentationError> {
        verify_packed_brotli_stream(
            stream,
            body_bytes,
            artifact.len() as u64,
            &hex::encode(Sha256::digest(artifact)),
            brotli_covering_window_bits(body_bytes).unwrap(),
            16 * 1024,
            &CancellationToken::new(),
        )
    }

    /// The verifier restores the artifact from the body the stream decodes to
    /// and checks that artifact, so it refuses a body of another length, one
    /// the unpacker refuses, and one that restores another artifact.
    #[test]
    fn the_packed_verifier_refuses_a_stream_whose_body_does_not_restore_the_artifact() {
        let (_directory, _path, artifact, _digest, body) = packed_fixture("every-kind");
        let (_other_directory, _other_path, other, _other_digest, other_body) =
            packed_fixture("tables-smallest-first");
        let body_bytes = body.len() as u64;
        let stream = brotli_of(&body, 5, 17, true);
        let verified = verify_packed(stream.as_slice(), body_bytes, &artifact).unwrap();
        assert_eq!(verified.encoded_bytes, stream.len() as u64);
        assert_eq!(
            verified.encoded_sha256,
            hex::encode(Sha256::digest(&stream))
        );
        assert_eq!(verified.decoded_chunks, chunk_digests(&artifact, 16 * 1024));

        // The artifact's own bytes, which an unpacker reads as a stored body.
        let plain = brotli_of(&artifact, 5, 17, true);
        assert!(matches!(
            verify_packed(plain.as_slice(), body_bytes, &artifact),
            Err(RepresentationError::PackedBodyLength { expected }) if expected == body_bytes
        ));
        let short = brotli_of(&body[..body.len() - 1], 5, 17, true);
        assert!(matches!(
            verify_packed(short.as_slice(), body_bytes, &artifact),
            Err(RepresentationError::PackedBodyLength { expected }) if expected == body_bytes
        ));
        assert!(matches!(
            verify_packed(short.as_slice(), body_bytes - 1, &artifact),
            Err(RepresentationError::Unpack(UnpackError::Truncated))
        ));
        // A body the unpacker restores, to other bytes than the artifact's:
        // the last byte of it whose flipped bit the unpacker does not refuse.
        let flipped = (0..body.len())
            .rev()
            .find_map(|index| {
                let mut flipped = body.clone();
                flipped[index] ^= 1;
                unpack(&flipped, artifact.len() as u64)
                    .is_ok()
                    .then_some(flipped)
            })
            .unwrap();
        assert!(matches!(
            verify_packed(
                brotli_of(&flipped, 5, 17, true).as_slice(),
                body_bytes,
                &artifact
            ),
            Err(RepresentationError::DecodedDigestMismatch)
        ));
        // Another artifact's body, against this artifact's length and digest
        // and then against its own length with this artifact's digest.
        let other_stream = brotli_of(&other_body, 5, 17, true);
        assert!(matches!(
            verify_packed(other_stream.as_slice(), other_body.len() as u64, &artifact),
            Err(RepresentationError::Unpack(
                UnpackError::DecodedLength { .. }
            ))
        ));
        assert!(matches!(
            verify_packed_brotli_stream(
                other_stream.as_slice(),
                other_body.len() as u64,
                other.len() as u64,
                &hex::encode(Sha256::digest(&artifact)),
                17,
                16 * 1024,
                &CancellationToken::new(),
            ),
            Err(RepresentationError::DecodedDigestMismatch)
        ));
        // What the plain verifier refuses of a stream's framing.
        assert!(matches!(
            verify_packed(
                brotli_of(&body, 5, 18, true).as_slice(),
                body_bytes,
                &artifact
            ),
            Err(RepresentationError::BrotliWindowMismatch { expected: 17 })
        ));
        assert!(matches!(
            verify_packed(stream.as_slice().chain(&b"x"[..]), body_bytes, &artifact),
            Err(RepresentationError::TrailingEncodedBytes)
        ));
        assert!(matches!(
            verify_packed(&stream[..stream.len() - 1], body_bytes, &artifact),
            Err(RepresentationError::Io(_))
        ));
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            verify_packed_brotli_stream(
                stream.as_slice(),
                body_bytes,
                artifact.len() as u64,
                &hex::encode(Sha256::digest(&artifact)),
                17,
                16 * 1024,
                &cancelled,
            ),
            Err(RepresentationError::Cancelled)
        ));
    }

    /// Cancels its token at the first read and counts what is read through it.
    struct CancellingReader<'a> {
        inner: &'a [u8],
        cancellation: &'a CancellationToken,
        read: &'a Cell<usize>,
    }

    impl Read for CancellingReader<'_> {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            self.cancellation.cancel();
            let read = self.inner.read(bytes)?;
            self.read.set(self.read.get() + read);
            Ok(read)
        }
    }

    /// A cancel that arrives while the verifier reads the stream ends it
    /// there: it reads no more than the two bytes it had asked for.
    #[test]
    fn the_packed_verifier_stops_reading_at_a_cancel() {
        let (_directory, _path, artifact, digest, body) = packed_fixture("every-kind");
        let stream = brotli_of(&body, 5, 17, true);
        let cancellation = CancellationToken::new();
        let read = Cell::new(0);
        let verified = verify_packed_brotli_stream(
            CancellingReader {
                inner: &stream,
                cancellation: &cancellation,
                read: &read,
            },
            body.len() as u64,
            artifact.len() as u64,
            &digest,
            17,
            16 * 1024,
            &cancellation,
        );
        assert!(matches!(verified, Err(RepresentationError::Cancelled)));
        assert_eq!(read.get(), 2);
    }

    #[test]
    fn a_packed_encode_that_is_not_under_its_bound_fails_and_leaves_only_the_body() {
        let (directory, path, artifact, digest, body) = packed_fixture("every-kind");
        let length = artifact.len() as u64;
        let packed = pack_artifact_controlled(&path, length, &CancellationToken::new())
            .unwrap()
            .unwrap();
        let size = brotli_of(&body, BROTLI_QUALITY, 17, true).len() as u64;
        let encode = |limit: u64, cancellation: &CancellationToken| {
            encode_packed_brotli_large_window_controlled(
                &packed,
                length,
                &digest,
                limit,
                cancellation,
            )
        };
        assert!(encode(size - 1, &CancellationToken::new()).is_err());
        assert_eq!(files_in(&directory).len(), 2);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(matches!(
            encode(size, &cancelled),
            Err(RepresentationError::Cancelled)
        ));
        assert_eq!(files_in(&directory).len(), 2);
        let (object, _) = encode(size, &CancellationToken::new()).unwrap();
        assert_eq!(object.manifest().encoded_bytes, size);
        assert_eq!(files_in(&directory).len(), 3);
    }
}

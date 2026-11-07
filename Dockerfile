# Calque server image.
#
# Ships the `calque` binary: `inspect`, `init`, `serve`, `dev`, `status`,
# `destroy` and the three recovery subcommands. `serve` is what
# `docker-compose.selfhost.yml` runs.
#
# The workspace also builds a second binary that must NEVER be in an image:
# `calque-dev-grant` (server/crates/dataset-grant-issuer), which mints signed
# dataset grants for development only.
# `cargo build --workspace --release` produces it. What keeps it out of here is
# the explicit `--package` list in the builder stage below, and nothing else:
# the grant-issuer linkage gate (scripts/verify-grant-issuer-linkage.ts) walks
# `cargo metadata` dependency edges and structurally cannot see a binary
# target. Keep the builder stage naming its packages; never build the whole
# workspace here, and never copy from `target/release` by glob.
#
# Native build inputs, all load-bearing:
#   libpq   the vendored pg_walstream fork links pq-sys, so libpq is required
#           to build and at runtime. It is not optional.
#   protoc  NOT needed; sync-protocol's build.rs uses protoc-bin-vendored.
#   TLS     rustls, not OpenSSL, so no libssl-dev is required.

# The toolchain is pinned by rust-toolchain.toml; keep this tag in step with it.
FROM rust:1.97-bookworm AS builder

RUN apt-get update \
 && apt-get install --no-install-recommends -y libpq-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Manifests and the vendored fork first, so a source-only edit does not
# invalidate the dependency layer.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY vendor ./vendor
COPY spec ./spec
COPY server ./server

# The target cache below is shared by every image build on this host, and COPY
# keeps each file's modification time from the build context. Cargo treats a
# path crate as fresh when its sources are older than its cached build, so a
# source tree older than the cache's last build would link that build's crates.
# Touching the copied sources first makes every path crate rebuild whenever
# this layer runs; registry dependencies stay cached.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    find server vendor spec -type f -exec touch {} + \
 && cargo build --release --locked \
      --package calque \
 && mkdir -p /build/bin \
 && cp target/release/calque /build/bin/

FROM debian:bookworm-slim AS runtime

# libpq5 is the runtime half of the pq-sys link. ca-certificates holds the
# roots TLS to an object store uses, and the bundle a remote PostgreSQL's
# `sslrootcert` can name: /etc/ssl/certs/ca-certificates.crt.
RUN apt-get update \
 && apt-get install --no-install-recommends -y libpq5 ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --create-home --uid 10001 calque

COPY --from=builder /build/bin/calque /usr/local/bin/calque

# The binary's licence and notice, and the licences of the pg_walstream and
# tokio-postgres forks it links, which a binary redistribution must carry.
COPY LICENSE NOTICE /usr/share/doc/calque/
COPY vendor/pg-walstream/LICENSE /usr/share/doc/calque/pg-walstream/
COPY vendor/tokio-postgres/LICENSE-MIT vendor/tokio-postgres/LICENSE-APACHE /usr/share/doc/calque/tokio-postgres/

# `serve`'s private durable state directory, and `/recovery`, where `backup`
# writes a unit and `restore` reads one. Both exist in the image, owned by the
# unprivileged runtime user, because Docker gives a fresh named volume the
# ownership and mode of the image path it is mounted over; without this the
# composition's volumes would arrive root-owned, and `--state-dir` or
# `backup --output` under them would fail on its first write. A host bind
# mount does not get this: it keeps the host directory's owner, which on Linux
# is not this user, so the composition keeps units on a volume too.
RUN mkdir -p /var/lib/calque /recovery \
 && chown calque:calque /var/lib/calque /recovery

USER calque
WORKDIR /home/calque

# Connection strings are read from the environment, never from argv, so they do
# not appear in this host's process listing or in a shell history. An
# environment value is still readable through `docker inspect` and
# `/proc/<pid>/environ`; argv is the exposure this closes.
ENTRYPOINT ["calque"]
CMD ["--help"]

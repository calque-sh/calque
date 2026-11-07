# Manifest and bootstrap schema notes

## Immutable artifact manifest

`artifact-manifest.schema.json` describes facts that never change for published artifact bytes:

- decoded-object identity, size, and hash, with `decoded.format` naming the
  format (`directColumnar` is the only one); the client refuses any other
  `artifactRuntime.format` while parsing the plan, before it asks for a byte;
- exact PIT/canonical cursor;
- source/schema/authorization/index/build compatibility generations;
- complete table membership and row-count diagnostics;
- immutable encoded representations;
- memory estimates;
- optional signature.

It must not contain expiring signed URLs, a current server head, or a client-specific activation fence.

## Dynamic bootstrap plan

`bootstrap-plan.schema.json` is authenticated and short lived. It selects:

- which immutable artifacts/representations to install, and in what decoded
  format (`artifactRuntime.format`, mirroring the manifest's `decoded.format`);
- delivery URLs;
- a fixed activation cursor;
- the exact canonical catch-up range;
- memory admission estimates;
- optional replanning policy.

A plan does not override an artifact manifest. The client validates both and requires compatible duplicate fields to match.

## Publication and parsing rules

- Canonicalize manifest bytes before signing; specify the canonical JSON scheme before production signing.
- Validate size limits before parsing large arrays/strings.
- Reject duplicate logical table IDs across selected active artifacts.
- Reject a selected representation absent from the immutable manifest.
- Open the decoded image only after hash verification; validate its embedded identity and table membership immediately afterward.
- Treat row counts as diagnostics, not the sole proof of completeness.
- Never accept unknown compatibility generations through a permissive fallback.

The `*.example.json` documents in this directory validate against these draft schemas.

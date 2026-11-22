---
kind: fixed
---
A page that loads while `calque serve` re-establishes its replication session
now waits for it, for up to about 10 s, instead of reporting `SERVER_ERROR`.
The bootstrap answers `503 INTERNAL_RETRYABLE` during that window, which the
client asks again for, and artifact downloads are served through it.

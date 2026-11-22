---
kind: fixed
---
`calque serve` no longer restarts its replication session, answering every
request `503` for a few seconds, when recording a change waits over a second
on a row another transaction holds, as behind a commit PostgreSQL takes over
a second to flush. It tries again for about 4 s first.

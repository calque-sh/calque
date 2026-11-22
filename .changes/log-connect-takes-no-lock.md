---
kind: fixed
---
When `calque serve` reconnects to its control database, it no longer waits
on, and holds up, the bootstraps and recorded changes that read the
canonical log's retention table. A reconnect behind one of them used to fail
after a second.

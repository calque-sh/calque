---
kind: fixed
---
`calque serve` and `calque dev` handle SIGINT and SIGTERM from the moment
they print that they are ready, so a stop sent right then exits cleanly.

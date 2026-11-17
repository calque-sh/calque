---
kind: added
---
After a re-cut, a page that stored an older cut catches up from it instead
of downloading the newest, while the server keeps that cut.
`--recut-keep-ratio` (default 0.25) sets how many bytes of changes since an
older cut the server keeps, per byte of the newest artifact; 0 keeps only
the newest cut.

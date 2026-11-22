---
kind: added
---

A release upgrades in place from the one before it: start the new image or
binary on the same state directory and database, without `calque destroy`.
Open pages keep their cursor and follow the upgraded server. `calque restore`
and `calque verify-restore` read a recovery unit the release before took as
well as this release's own.

From this release on, a release started on state a later one wrote refuses
it, says so, and names the remedy. Release 0.4.0 and earlier cannot tell, so
roll back to one of them only after `calque destroy`.

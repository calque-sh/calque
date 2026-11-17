# Change notes

Each file here is one entry of the next release's notes. A change that a user
of the `@calque` packages, the `calque` binary or its image would notice adds
one, in the same commit:

```md
---
kind: fixed
---
`calque serve` exits cleanly on a SIGTERM sent right after it prints
`ready=true`.
```

`kind` is one of:

- `breaking`: a configuration, stored state, wire format or API that worked
  before no longer does. Say what to change.
- `security`: closes a vulnerability.
- `added`: something new to use.
- `changed`: a behaviour that differs, with nothing to change on the user's
  side.
- `fixed`: a defect that is gone.

Write for someone upgrading: what they will see, and what they have to do,
in a sentence or two. Name the option, command, error code or API, and leave
out how the code does it. Name the file after the change, such as
`stored-rows.md`.

`pnpm release:prepare` turns the notes into the release's section of
`CHANGELOG.md`, grouped by kind, moves every package and the binary to the
next version and deletes the notes. Before 1.0 a breaking change or an
addition raises the minor version, anything else the patch version.
`pnpm release:notes` prints a release's section, which becomes its tag
message and GitHub release.

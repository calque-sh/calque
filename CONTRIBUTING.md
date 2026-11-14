# Contributing

Calque takes issues, not pull requests, while its internals still change
daily. A pull request is closed with a pointer here.

Open an issue for a bug, a question, a proposed change, or a place where the
documentation and the code disagree. A bug report is most useful when it
names:

- the Calque commit, and the PostgreSQL and Chromium versions;
- the command you ran or the page you opened;
- what you expected and what happened, with the full error text;
- for a refused table or column, the output of `calque inspect`.

To report a security issue, follow [`SECURITY.md`](SECURITY.md) instead of
opening a public issue.

## Running the checks

To check a fix before you describe it in an issue, install what
[Try it locally](README.md#try-it-locally) lists, and Playwright's Chromium
and Firefox, which the browser suites run in:
`pnpm exec playwright install chromium firefox`. Then build once with
`pnpm install && pnpm -r build` and run `pnpm verify`. It builds every crate
and package, runs the Rust and TypeScript test suites and lints under
`clippy -D warnings`, and refuses `unwrap`, `panic!`, `todo!`,
`unimplemented!` and `dbg!` outside Rust tests. The Rust tests that need a live PostgreSQL or an S3
bucket are `#[ignore]`d, so it skips them;
[`docs/development.md`](docs/development.md) shows how to run them.

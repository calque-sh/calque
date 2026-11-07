# @calque/protocol

The wire protocol between `@calque/client` and the `calque` server:
change-stream framing and state, integrity checks, revocations, and the
protobuf messages for changes and mutations. It is a dependency of
`@calque/client` rather than an API for applications: install
`@calque/client` instead.

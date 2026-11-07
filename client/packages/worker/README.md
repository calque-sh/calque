# @calque/worker

The workers `@calque/client` starts: the engine that holds the working set,
the hashers that check artifact bytes as they arrive, and the decoders,
including a Brotli decoder compiled to WebAssembly. It is a dependency of
`@calque/client` rather than an API for applications: install
`@calque/client` instead.

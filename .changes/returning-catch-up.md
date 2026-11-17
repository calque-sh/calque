---
kind: changed
---
A returning page catches up sooner and with less memory: it asks for the
changes since its stored copy while it reads that copy, and it decodes
change frames with `@bufbuild/protobuf` 2.17.0.

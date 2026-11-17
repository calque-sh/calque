---
kind: added
---
`renderBootstrap` takes the page request's `Cookie` header as `cookie`, and
leaves out the artifact preload when the page already holds that artifact.

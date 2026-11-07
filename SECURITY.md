# Security

Report a vulnerability through GitHub's private vulnerability reporting: open
this repository's **Security** tab and choose **Report a vulnerability**.
Please do not open a public issue for a security problem.

Calque is pre-alpha. There is no release and no supported version yet, and a
fix lands on `main` only.

## In scope

The code on `main`, and in particular:

- `calque serve`: its verification of signed dataset grants, authorization and
  revocation, and its bootstrap, artifact, change and mutation routes;
- the browser packages under `client/packages/`;
- the self-hosted composition, `docker-compose.selfhost.yml` and the
  `Dockerfile`;
- the protocol contracts under `spec/`.

## Known limits

These are documented, and are not vulnerabilities by themselves. A report that
one of them is worse than documented is in scope.

- `calque dev` is for a disposable local database. It listens only on
  `127.0.0.1` and accepts one shared development bearer instead of signed
  grants.
- `calque` reaches PostgreSQL in plaintext over a Unix socket or a numeric
  loopback address, and over TLS that verifies the server's certificate and
  name anywhere else. In the self-hosted composition, the hops behind Caddy,
  to the server and from it to the object store, are plaintext on the compose
  network.
- Revoking access cannot take back bytes a client has already received.

The threat model Calque is built against is in
[`docs/security/threat-model.md`](docs/security/threat-model.md).

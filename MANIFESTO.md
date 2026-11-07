# Calque manifesto

Calque is a sync engine for PostgreSQL. It copies the complete contents of an
application's collections, for the scope a user's grant allows, into the
browser, keeps that copy at one PostgreSQL transaction as the database
changes, and answers the page's queries locally over every row.

## The browser can hold the working set

A browser can sort, filter, group, join and aggregate a large dataset without
a round trip. Yet applications whose users work in one dataset all day send
each of those operations to a server: an endpoint per view, an analytics
database to make the endpoints fast, cache invalidation to keep them fresh,
and optimistic UI to hide the wait. They do it because moving enough
trustworthy data to the browser has been too slow or too hard.

If a working set fits in the page, the page should hold all of it, and making
it interactive should not take a second query system on the backend. Calque
exists to remove that work.

## Who it is for

Calque is for data-rich applications whose users work in a large dataset all
day: incident and operations consoles, internal tools, support, planning and
reporting. The design target is a working set of 500,000 to 1,000,000 mixed
rows behind large grids, charts, relationship views and collaborative
workflows.

## Pagination is the wrong default for a working set

A page of rows answers the query that fetched it and no other. Each new sort,
filter or grouping is another request, a count and the rows beside it can
come from different moments, and a live change has to guess which pages it
touches. Syncing only the rows a query selected has the same limit: the
client knows what it asked for, not what exists.

Calque synchronizes whole collections. A query predicate never changes what
is synchronized, so every question the page asks is answered over every row.

## A collection is complete or absent

A count over a half-loaded collection is wrong, and nothing on the screen
says so. In Calque a query never observes a collection that is partly
downloaded, decoded or installed. A client without the memory for its working
set receives a structured capacity error, never a subset.

## Consistency is one transaction

Everything a page shows comes from one PostgreSQL transaction boundary. A
transaction that changes several collections appears in all of them at once,
and two views on one page never disagree about which commits have happened.

A write shows at once and stays optimistic until its change comes back from
PostgreSQL through logical replication. The server accepting a write does not
confirm it; only that echo does, so the page never marks a write saved before
the database has it.

## PostgreSQL stays the source of truth

Calque follows the PostgreSQL an application already runs. A change committed
by any backend, job or migration reaches every open page, and nothing has to
write through Calque to be seen.

Because the server stays authoritative, Calque is a sync engine, not a
local-first or offline-first system. A page needs the server to load, a short
disconnection may queue writes in memory, and durable offline work and
long-lived divergent replicas are out of scope.

## The application owns the tables the browser sees

The application's backend decides what a client may read and write, through
short-lived grants it signs. Calque verifies every grant, no request can widen
one, and a read grant never implies write permission. Revoking access stops
further access through Calque, and a page that can verify the revocation
discards what it holds and rebuilds. Bytes a hostile browser has already
copied stay copied: shipping a working set to a browser trusts that browser's
user with it.

A collection maps to one PostgreSQL table, and Calque narrows it natively to
one tenant. A finer rule, per user, team or record, lives in a projection
table the application maintains, holding the rows and columns that scope may
see. Calque runs no application code and no generic live joins inside change
capture. The application owns the tables the browser sees, and Calque owns
everything between those tables and the screen.

## The cost is paid up front, and measured at the paint

Holding the whole working set costs a transfer before the first complete
paint. Calque optimizes that cost and measures it from a fresh browser profile
with an empty cache to a complete, stable paint of the working set, with
realistic data, a fixed environment, exact start and end events, raw samples,
resource measurements, and every sample's working set checked against
PostgreSQL. Download completion is not success, and there is no best-of-N
reporting. A run that fails its correctness check earns no performance
credit.

## What Calque will not become

It does no remote pagination, no predicate replication and no partial
results. It is not a server-side incremental SQL engine or a hosted analytics
database. It has no public SQL API and gives application code no raw access to
its engine: the generated, typed bindings are the only query, subscription and
mutation surface. Each of these would make a different product.

## When goals conflict

Correctness, authorization and recovery must hold under retries, crashes,
corruption and slow clients. Calque chooses in this order: correctness and
security, end-to-end performance and realtime latency, developer experience,
operational simplicity, implementation convenience. A simpler design loses
when it moves latency, correctness, authorization or operational work onto
application developers.

## Self-hosted, with no private engine

Calque is source-available and free to self-host. It ships as a self-hosted
Docker composition, and its core cannot require a particular cloud provider.
A future managed service runs the same core, not a privileged private engine.
The [README](README.md#license) has the license terms.

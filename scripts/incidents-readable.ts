/**
 * `pnpm incidents --dataset readable`: content-v1's rows, with text a person
 * can read in every column the page shows: service names, people, issue
 * titles, log lines and comments. The README's recording uses it. The
 * benchmark keeps content-v1, whose drawn words compress like a real
 * workspace's text; this rewrite keeps every key, level, time and count, and
 * leaves the sentinel rows alone.
 */

const SERVICES = [
  ["checkout-api", "Checkout API"],
  ["payments", "Payments"],
  ["billing-worker", "Billing worker"],
  ["auth-gateway", "Auth gateway"],
  ["search-indexer", "Search indexer"],
  ["notifications", "Notifications"],
  ["inventory-sync", "Inventory sync"],
  ["shipping-rates", "Shipping rates"],
  ["media-resizer", "Media resizer"],
  ["reports-export", "Reports export"],
  ["webhooks-relay", "Webhooks relay"],
  ["ledger", "Ledger"],
  ["pricing-engine", "Pricing engine"],
  ["catalog-api", "Catalog API"],
  ["sessions", "Sessions"],
  ["email-sender", "Email sender"],
  ["fraud-scoring", "Fraud scoring"],
  ["tax-service", "Tax service"],
  ["order-router", "Order router"],
  ["analytics-ingest", "Analytics ingest"],
  ["admin-console", "Admin console"],
  ["mobile-bff", "Mobile BFF"],
  ["feature-flags", "Feature flags"],
  ["scheduler", "Scheduler"],
  ["audit-log", "Audit log"],
] as const;
const REGIONS = ["eu", "us", "ap", "ca"] as const;
const ZONES = ["eu-west-1", "us-east-1", "ap-southeast-1", "ca-central-1"];
const CLIENTS = [
  'client="Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) Chrome/131.0"',
  'client="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0"',
  'client="Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) Safari/604.1"',
  'client="okhttp/4.12.0 shop-android/4.12.3"',
  'client="python-requests/2.32.3"',
  'client="Go-http-client/2.0"',
];

const FIRST_NAMES = [
  "Ada",
  "Amara",
  "Ben",
  "Camille",
  "Chen",
  "Dana",
  "Diego",
  "Elif",
  "Emma",
  "Farah",
  "Felix",
  "Grace",
  "Hana",
  "Ines",
  "Ivan",
  "Jonas",
  "Kai",
  "Lena",
  "Leo",
  "Lucia",
  "Malik",
  "Maya",
  "Mehdi",
  "Nadia",
  "Noah",
  "Olga",
  "Omar",
  "Priya",
  "Quinn",
  "Rafael",
  "Rosa",
  "Sam",
  "Sara",
  "Tariq",
  "Theo",
  "Uma",
  "Victor",
  "Yara",
  "Yusuf",
  "Zoe",
] as const;
const LAST_NAMES = [
  "Adeyemi",
  "Bauer",
  "Costa",
  "Dubois",
  "Eriksen",
  "Fischer",
  "Garcia",
  "Haddad",
  "Ito",
  "Jensen",
  "Kowalski",
  "Laurent",
  "Moreau",
  "Nakamura",
  "Okafor",
  "Petrov",
  "Rossi",
  "Silva",
  "Tanaka",
  "Weber",
] as const;

/**
 * Each issue's title, and the line its `error` and `fatal` events log. A line
 * fills `{id}`, `{ms}`, `{n}`, `{m}` and `{hex}` from its event.
 */
const ISSUES = [
  [
    "Payment webhook times out after 3 retries",
    "POST /hooks/payments timed out after {ms} ms (attempt 3 of 3)",
  ],
  [
    "Checkout returns 502 when the cart has more than 50 items",
    "upstream pricing-engine returned 502 for cart {id}",
  ],
  [
    "Refund job charges twice when a retry follows a timeout",
    "refund {hex} applied twice to order {id}",
  ],
  [
    "Search results stay stale for minutes after a product update",
    "index {ms} ms behind primary on shard {n}",
  ],
  [
    "Password reset emails land in spam for Outlook users",
    "smtp 550 5.7.1 rejected message {hex} for outlook.com",
  ],
  [
    "Inventory count goes negative under concurrent orders",
    "stock for sku-{id} fell below zero after a concurrent reserve",
  ],
  [
    "Tax rounding differs by one cent from the invoice",
    "line total for order {id} off by 0.01 after rounding",
  ],
  [
    "Login loops on Safari when third-party cookies are blocked",
    "session cookie missing on /auth/callback, redirect {n} of 10",
  ],
  [
    "PDF export fails for reports over 200 pages",
    "renderer ran out of memory at page {m}{n}",
  ],
  [
    "Mobile app crashes on launch after the 4.12 update",
    "NullPointerException in FeedAdapter.bind (build 4.12.{n})",
  ],
  [
    "Shipping rates missing for Canadian postal codes",
    "carrier returned 404 for a rate to postal area {n}",
  ],
  [
    "Order confirmation email sent twice",
    "duplicate delivery of message {hex} to order-confirmed",
  ],
  [
    "Image uploads over 20 MB stall at 99%",
    "multipart upload {hex} stalled after {ms} ms",
  ],
  [
    "Rate limiter blocks the admin console during bulk edits",
    "429 for admin session {hex}: {m}0 requests in 10 s",
  ],
  [
    "Nightly ledger reconciliation is off by a few cents",
    "ledger batch {id} unbalanced by 0.0{n} EUR",
  ],
  [
    "Feature flag changes take minutes to reach the mobile app",
    "flag snapshot {hex} served {ms} ms after publish",
  ],
  [
    "Scheduled reports run twice after the clocks change",
    "job report-{id} fired twice at 02:00 local time",
  ],
  [
    "Fraud scoring times out for gift card payments",
    "fraud-scoring deadline exceeded after {ms} ms for order {id}",
  ],
  [
    "Delivery estimates ignore the customer's time zone",
    "estimate for order {id} computed in UTC, customer at UTC+{n}",
  ],
  [
    "Webhook signatures fail after the key rotation",
    "signature mismatch for delivery {hex}, key version {n}",
  ],
  [
    "Expired coupon codes are still accepted",
    "coupon SPRING{m} redeemed {n} days after expiry",
  ],
  [
    "Session expires mid-checkout on slow networks",
    "token expired {ms} ms before the payment was submitted",
  ],
  [
    "CSV import drops rows with accented names",
    "row {id} skipped: invalid UTF-8 at byte {m}",
  ],
  [
    "Audit log misses actions taken with API tokens",
    "audit write for token {hex} dropped: queue full",
  ],
  [
    "Push notifications arrive late during peak hours",
    "notification queue depth {id}, oldest waiting {ms} ms",
  ],
  [
    "Dashboard totals disagree with the CSV export",
    "export total for account {id} differs from the dashboard by {m}",
  ],
  [
    "Analytics events dropped when a batch exceeds 1 MB",
    "batch {hex} rejected: 413 payload too large",
  ],
  [
    "Two-factor codes rejected when the phone clock drifts",
    "TOTP rejected for user {id}: clock drift {m} s",
  ],
  [
    "Search autocomplete is slow for one-letter queries",
    "suggest query took {ms} ms for a one-letter prefix",
  ],
  [
    "Invoice emails show the previous month's total",
    "invoice {id} rendered from a snapshot {ms} ms old",
  ],
  [
    "Bulk price update times out on large catalogs",
    "price batch {hex} cancelled after {ms} ms",
  ],
  [
    "EU orders routed to the US warehouse",
    "order {id} routed to us-east despite an EU shipping address",
  ],
  [
    "Thumbnails render rotated for iPhone photos",
    "EXIF orientation {n} ignored for asset {hex}",
  ],
  [
    "Sign-up accepts emails with trailing spaces",
    "unique constraint failed on the email of user {id}",
  ],
  [
    "Admin console freezes when opening a large order",
    "long task of {ms} ms rendering order {id}",
  ],
  [
    "Retry storm after the payment provider outage",
    "{m}0 retries in flight to provider payments-eu",
  ],
  [
    "Partial refunds missing from the monthly statement",
    "refund {hex} missing from statement {id}",
  ],
  [
    "Background sync stops after the device sleeps",
    "sync worker idle for {ms} ms after resume",
  ],
  [
    "Swiss customers see prices in euros",
    "order {id} formatted as EUR, expected CHF",
  ],
  [
    "Cart badge shows the wrong count after removing an item",
    "cart {id} badge shows {n} items, cart holds {m}",
  ],
] as const;

const WARNINGS = [
  "slow query on orders took {ms} ms",
  "retrying request {hex} (attempt {n} of 5)",
  "connection pool {m}% saturated",
  "cache miss storm on product:{id}",
  "deprecated API v1 called by client {hex}",
  "queue lag of {ms} ms on email-outbound",
  "payload of {id} bytes exceeds the soft limit",
  "rate limit at {m}% for tenant {id}",
  "TLS certificate expires in {n} days",
  "GC pause of {m} ms on worker-{n}",
  "disk usage at {m}% on /var/lib/data",
  "webhook endpoint answered in {ms} ms",
] as const;

const INFOS = [
  "GET /api/orders/{id} 200 in {m} ms",
  "POST /api/carts/{id}/items 201 in {m} ms",
  "user {id} signed in",
  "order {id} paid",
  "order {id} shipped with carrier {n}",
  "invoice {id} sent",
  "cache warmed for catalog page {n}",
  "deploy {hex} rolled out to {m}% of pods",
  "job report-{id} finished in {ms} ms",
  "refund {hex} approved",
  "GET /api/products?page={n} 200 in {m} ms",
  "session {hex} refreshed",
  "webhook {hex} delivered",
  "search reindexed {id} documents",
] as const;

const COMMENTS = [
  "Reproduced on staging, reading the logs now.",
  "Rolled back the 14:20 deploy and the errors stopped.",
  "Same root cause as last week: the retry budget is too small.",
  "Support has 12 tickets about this one.",
  "I can take it after the standup.",
  "Fix is in review and should ship this afternoon.",
  "Only accounts created before March are affected.",
  "Added a dashboard panel so we can watch the rate.",
  "Cannot reproduce with a fresh session. Can someone share a HAR file?",
  "Raised the priority, this blocks the release.",
  "The provider's status page says they are degraded too.",
  "Deployed the fix to 10% of traffic first.",
  "Closing once the error rate stays at zero overnight.",
  "The alert fired three times since midnight.",
  "We have a failing test for it now.",
  "Wrote up the timeline in the incident doc.",
] as const;

function quoted(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function array(values: readonly string[]): string {
  return `(ARRAY[${values.map(quoted).join(", ")}]::text[])`;
}

/** One element of `values`, chosen by the integer expression `key`. */
function pick(values: readonly string[], key: string): string {
  return `${array(values)}[(1 + (${key}) % ${values.length})::int]`;
}

/** `template` with each placeholder filled from the integer expression `key`. */
function filled(template: string, key: string): string {
  const values: readonly (readonly [string, string])[] = [
    ["{id}", `((${key}) * 7919 % 900000 + 100000)::text`],
    ["{ms}", `(1000 + (${key}) * 104729 % 29000)::text`],
    ["{n}", `(2 + (${key}) * 31 % 8)::text`],
    ["{m}", `(10 + (${key}) * 97 % 90)::text`],
    ["{hex}", `lpad(to_hex((${key}) * 2654435761 % 4294967296), 8, '0')`],
  ];
  return values.reduce(
    (expression, [placeholder, value]) =>
      `replace(${expression}, '${placeholder}', ${value})`,
    template,
  );
}

/** `md5`'s first `length` hex digits for the integer expression `key`. */
function digest(key: string, purpose: string, length = 32): string {
  return `substr(md5((${key})::text || ':${purpose}'), 1, ${length})`;
}

/**
 * The structured fields a log line carries after its text, from the integer
 * expression `key`: ids, a host, a duration and a client.
 */
function fields(key: string): string {
  return [
    `' request_id=' || ${digest(key, "request", 16)}`,
    `' trace_id=' || ${digest(key, "trace")}`,
    `' span_id=' || ${digest(key, "span", 16)}`,
    `' host=' || ${pick(
      SERVICES.map(([slug]) => slug),
      `${key} * 7`,
    )} || '-' || ((${key}) * 13 % 40)::text || '.internal'`,
    `' region=' || ${pick(ZONES, `${key} * 3`)}`,
    `' user_id=' || ((${key}) * 6007 % 900000 + 100000)::text`,
    `' session=' || ${digest(key, "session", 24)}`,
    `' duration_ms=' || ((${key}) * 104729 % 30000)::text`,
    `' ' || ${pick(CLIENTS, `${key} * 11`)}`,
    `' build=' || ${digest(key, "build", 12)}`,
  ].join(" || ");
}

/**
 * One transaction that rewrites the page's text columns. An issue's title and
 * its `error` and `fatal` events' line come from one entry of `ISSUES`, so an
 * issue's latest events read as its title does. A log line keeps at least
 * content-v1's octet length, filled out with structured fields, so the
 * artifact moves about as many bytes as the benchmark's.
 */
export const readableTextSql: string = (() => {
  const issueKey =
    "coalesce((SELECT i.bench_index FROM incidents.issues i WHERE i.id = e.issue_id), e.bench_index) * 37";
  return `BEGIN;
UPDATE incidents.projects SET
  slug = ${pick(
    SERVICES.map(([slug]) => slug),
    "bench_index",
  )} || '-' || ${pick(REGIONS, `bench_index / ${SERVICES.length}`)},
  name = ${pick(
    SERVICES.map(([, name]) => name),
    "bench_index",
  )}
WHERE NOT is_sentinel;
UPDATE incidents.users SET
  display_name = ${pick(FIRST_NAMES, "bench_index")} || ' ' || ${pick(LAST_NAMES, `bench_index / ${FIRST_NAMES.length}`)}
WHERE NOT is_sentinel;
UPDATE incidents.issues SET
  title = ${pick(
    ISSUES.map(([title]) => title),
    "bench_index * 37",
  )}
WHERE NOT is_sentinel;
UPDATE incidents.events e SET message = (
  SELECT left(l.line || ${fields("e.bench_index")}, greatest(octet_length(l.line), octet_length(e.message)))
  FROM (SELECT CASE
  WHEN e.level IN ('error', 'fatal') THEN ${filled(
    pick(
      ISSUES.map(([, line]) => line),
      issueKey,
    ),
    "e.bench_index",
  )}
  WHEN e.level = 'warning' THEN ${filled(pick(WARNINGS, "e.bench_index * 13"), "e.bench_index")}
  ELSE ${filled(pick(INFOS, "e.bench_index * 17"), "e.bench_index")}
  END AS line) l
)
WHERE NOT e.is_sentinel;
UPDATE incidents.comments SET body = ${pick(COMMENTS, "bench_index * 7")}
WHERE NOT is_sentinel;
COMMIT;`;
})();

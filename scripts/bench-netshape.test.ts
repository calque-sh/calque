import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { createServer, type Server } from "node:net";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  DEFAULT_NETWORK,
  emulatedNetwork,
  judgeShaping,
  parseBenchArguments,
  shapingLine,
  unjudgedReasons,
  withConditions,
} from "./bench-conditions.ts";
import { CADDY } from "./bench-caddy.ts";
import { runPasses } from "./bench-results.ts";
import {
  availableShapedPort,
  bootstrapPreflightMs,
  judgeKernel,
  judgePreflight,
  kernelLine,
  KernelShaper,
  type KernelProbe,
  type LedgerRequest,
  NETSHAPE,
  NETSHAPE_PROBE,
  NETSHAPE_SOURCE,
  parseKernelProbe,
  parseShapedNetwork,
  preflightLine,
  type RunCommand,
  SHAPED_PORT_MAX,
  SHAPED_PORT_MIN,
  shaperDisabled,
  shaperStatusProblems,
} from "./bench-netshape.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const CORPORATE = {
  shaper: "calque-netshape",
  megabitsPerSecond: 500,
  rttMs: 20,
} as const;

// The option ----------------------------------------------------------------------

test("--network shaped: is the kernel shaper, and Chrome emulates nothing under it", () => {
  const options = parseBenchArguments(["--", "--network", "shaped:500/20"]);
  assert.deepEqual(options.network, CORPORATE);
  assert.equal(emulatedNetwork(options.network), null);
  assert.deepEqual(emulatedNetwork(DEFAULT_NETWORK), DEFAULT_NETWORK);
  assert.equal(emulatedNetwork(null), null);
  assert.deepEqual(withConditions(options, { kind: "probe", k: 0 }), {
    kind: "probe",
    k: 0,
    conditions: {
      dataset: "content-v1",
      network: CORPORATE,
      renderedPlan: false,
      edge: {
        caddy: "2.11.4",
        binarySha256: CADDY.binarySha256,
        protocols: ["h1", "h2"],
      },
    },
  });
  assert.throws(
    () =>
      parseBenchArguments(["--network", "shaped:500/20", "--network", "off"]),
    /usage: pnpm bench/u,
  );
});

test("a shaped run is never judged, as a diagnostic run or one on a native PostgreSQL is not", () => {
  const reasons = (...arguments_: string[]) =>
    unjudgedReasons(parseBenchArguments(arguments_));
  assert.deepEqual(reasons(), []);
  assert.deepEqual(reasons("--network", "off"), []);
  assert.deepEqual(reasons("--network", "100/30"), []);
  assert.deepEqual(reasons("--network", "shaped:500/20"), [
    "kernel-shaped second reading",
  ]);
  assert.deepEqual(reasons("--diagnostic", "--network", "shaped:500/20"), [
    "diagnostic run",
    "kernel-shaped second reading",
  ]);
  assert.deepEqual(reasons("--realtime-only"), ["realtime only"]);
  assert.deepEqual(reasons("--realtime-only", "--network", "shaped:500/20"), [
    "kernel-shaped second reading",
    "realtime only",
  ]);
  assert.deepEqual(reasons("--postgres-bin", "pg/bin"), [
    "PostgreSQL outside its container",
  ]);
  assert.deepEqual(reasons("--no-edge"), ["no production edge"]);
  // The rows alone would pass; the reason is what makes the run not pass.
  assert.equal(runPasses([], false, false), true);
  assert.equal(
    runPasses([], false, reasons("--network", "shaped:500/20").length > 0),
    false,
  );
});

test("shaped: takes the bounds calque-netshape accepts and refuses the rest before a run", () => {
  assert.deepEqual(parseShapedNetwork("1/0"), {
    shaper: "calque-netshape",
    megabitsPerSecond: 1,
    rttMs: 0,
  });
  assert.deepEqual(parseShapedNetwork("2000/1000"), {
    shaper: "calque-netshape",
    megabitsPerSecond: 2_000,
    rttMs: 1_000,
  });
  for (const bad of [
    "",
    "500",
    "500/",
    "0/20",
    "2001/20",
    "500/21",
    "500/1002",
    "12.5/20",
    "500/-2",
    "0500/20",
    "500/020",
    "500/20/1",
  ])
    assert.throws(
      () => parseBenchArguments(["--network", `shaped:${bad}`]),
      /--network shaped: takes <Mbit\/s>\/<RTT ms> as calque-netshape accepts them/u,
      bad,
    );
});

// The shaping witness --------------------------------------------------------------

test("under the kernel shaper the fetches after the probes are held to the RTT less dummynet's clock", () => {
  assert.deepEqual(judgeShaping(CORPORATE, [25.2, 18, 30.4]), {
    rttMs: 20,
    floorMs: 18,
    fetchMs: [25.2, 18, 30.4],
    fastestMs: 18,
    shaped: true,
  });
  const loopback = judgeShaping(CORPORATE, [21.3, 2.4]);
  assert.equal(loopback.shaped, false);
  assert.equal(judgeShaping(CORPORATE, [17.99]).shaped, false);
  assert.equal(judgeShaping(CORPORATE, []).shaped, false);
  assert.equal(
    shapingLine(judgeShaping(CORPORATE, [21.04])),
    "network: the fastest of 1 uncached fetches after the probes took 21.0 ms, against a floor of 18 ms under the kernel shaper's 20 ms RTT",
  );
  // 2 ms of RTT is all dummynet's clock: no floor to reach, so no witness.
  const flat = judgeShaping(
    { shaper: "calque-netshape", megabitsPerSecond: 500, rttMs: 2 },
    [0.4],
  );
  assert.equal(flat.shaped, null);
  assert.equal(
    shapingLine(flat),
    "network: the kernel shaper's 2 ms RTT leaves no floor above dummynet's clock, so the shaping witness does not apply",
  );
});

// The preflight --------------------------------------------------------------------

/**
 * One cold sample's ledger: the document, the bootstrap's GET, a long-poll's
 * preflight answered in 0.5 ms, and, unless null, the bootstrap's preflight
 * taking `preflightMs`.
 */
function ledger(preflightMs: number | null, ended = true): LedgerRequest[] {
  const requests: LedgerRequest[] = [
    {
      url: "http://127.0.0.1:45601/",
      type: "Document",
      status: 200,
      sentMs: 1_000,
      endedMs: 1_030,
    },
    {
      url: "http://127.0.0.1:45602/v1/bootstrap",
      type: "Fetch",
      status: 200,
      sentMs: 1_100,
      endedMs: 1_180,
    },
    {
      url: "http://127.0.0.1:45602/v1/changes",
      type: "Other",
      status: 204,
      sentMs: 1_300,
      endedMs: 1_300.5,
    },
  ];
  if (preflightMs !== null)
    requests.splice(2, 0, {
      url: "http://127.0.0.1:45602/v1/bootstrap",
      type: "Other",
      status: 204,
      sentMs: 1_101,
      endedMs: ended ? 1_101 + preflightMs : null,
    });
  return requests;
}

const shapedRun = () =>
  parseBenchArguments(["--network", "shaped:500/20"]).network;

test("under the kernel shaper the preflight before /v1/bootstrap takes at least the connect floor in every judged cold sample", () => {
  const witness = judgePreflight(shapedRun(), [
    { name: "cold-0", requests: ledger(41.25) },
    { name: "cold-1", requests: ledger(40.5) },
  ]);
  assert.deepEqual(witness, {
    shaped: true,
    floorMs: 18,
    fastestMs: 40.5,
    samples: [
      { name: "cold-0", ms: 41.25 },
      { name: "cold-1", ms: 40.5 },
    ],
    reason: null,
  });
  assert.equal(
    preflightLine(witness),
    "preflight: the OPTIONS to /v1/bootstrap took 40.5 ms at the fastest of 2 judged cold samples, against a floor of 18 ms",
  );
});

test("a preflight under the connect floor was not shaped", () => {
  const witness = judgePreflight(shapedRun(), [
    { name: "cold-0", requests: ledger(41.25) },
    { name: "cold-1", requests: ledger(0.75) },
  ]);
  assert.equal(witness.shaped, false);
  assert.equal(witness.fastestMs, 0.75);
  assert.equal(witness.reason, null);
  assert.equal(
    preflightLine(witness),
    "preflight: the OPTIONS to /v1/bootstrap took 0.8 ms at the fastest of 2 judged cold samples, against a floor of 18 ms, so the preflight was not shaped",
  );
});

test("a ledger without the bootstrap's preflight, or with one that never ended, leaves the preflight not shown", () => {
  // The long-poll's 0.5 ms preflight and the bootstrap's GET are not it.
  assert.equal(bootstrapPreflightMs(ledger(null)), null);
  assert.equal(bootstrapPreflightMs(ledger(41.25, false)), null);
  const missing = judgePreflight(shapedRun(), [
    { name: "cold-0", requests: ledger(41.25) },
    { name: "cold-1", requests: ledger(null) },
  ]);
  assert.deepEqual(missing, {
    shaped: null,
    floorMs: 18,
    fastestMs: 41.25,
    samples: [
      { name: "cold-0", ms: 41.25 },
      { name: "cold-1", ms: null },
    ],
    reason:
      "1 of 2 judged cold samples' ledgers show no ended OPTIONS to /v1/bootstrap answered 204",
  });
  assert.equal(
    preflightLine(missing),
    "preflight: not shown: 1 of 2 judged cold samples' ledgers show no ended OPTIONS to /v1/bootstrap answered 204",
  );
  assert.equal(
    judgePreflight(shapedRun(), [
      { name: "cold-0", requests: ledger(41.25, false) },
    ]).shaped,
    null,
  );
  assert.equal(judgePreflight(shapedRun(), []).reason, "no judged cold sample");
});

test("outside a kernel-shaped run the preflight witness is null, with its reason", () => {
  for (const network of ["100/30", "off"]) {
    const witness = judgePreflight(
      parseBenchArguments(["--network", network]).network,
      [{ name: "cold-0", requests: ledger(0.75) }],
    );
    assert.deepEqual(witness, {
      shaped: null,
      floorMs: null,
      fastestMs: null,
      samples: [],
      reason: "not a kernel-shaped run",
    });
  }
});

// The kernel check -----------------------------------------------------------------

const SHAPED_READING: KernelProbe = {
  shaped: [
    { port: 45_601, connectMs: [21.4, 20.6, 20.9], mss: [1_448, 1_448, 1_448] },
    { port: 45_602, connectMs: [20.8, 21.1, 20.7], mss: [1_448, 1_448, 1_448] },
  ],
  control: {
    port: 52_113,
    connectMs: [4.1, 0.3, 0.2],
    mss: [16_332, 16_332, 16_332],
  },
};

test("the kernel check passes when the listed ports, and only they, are clamped and delayed", () => {
  const check = judgeKernel(CORPORATE, [45_601, 45_602], SHAPED_READING);
  assert.deepEqual(check.problems, []);
  assert.equal(check.passed, true);
  assert.equal(check.floorMs, 18);
  assert.equal(
    kernelLine("before the first sample", check),
    "kernel check before the first sample: held: port 45601 MSS 1448/1448/1448, fastest connect 20.6 ms; port 45602 MSS 1448/1448/1448, fastest connect 20.7 ms, against a floor of 18 ms; control port 52113 MSS 16332/16332/16332",
  );
});

test("the kernel check fails an unclamped or undelayed port, a clamped control, and a port it was not asked about", () => {
  const [first, second] = SHAPED_READING.shaped;
  assert.ok(first && second);
  const judged = (probe: KernelProbe, ports = [45_601, 45_602]) =>
    judgeKernel(CORPORATE, ports, probe).problems;
  assert.deepEqual(
    judged({
      ...SHAPED_READING,
      shaped: [{ ...first, mss: [1_448, 16_332, 1_448] }, second],
    }),
    ["port 45601: MSS 16332, above the clamp's 1460"],
  );
  assert.deepEqual(
    judged({
      ...SHAPED_READING,
      shaped: [first, { ...second, connectMs: [21.1, 0.4, 20.9] }],
    }),
    ["port 45602: the fastest connect took 0.400 ms, under 18 ms"],
  );
  // The leak a `port = N` scrub rule causes: the clamp on every connection.
  assert.deepEqual(
    judged({
      ...SHAPED_READING,
      control: { ...SHAPED_READING.control, mss: [1_448, 1_448, 1_448] },
    }),
    [
      "the control on port 52113 settled on MSS 1448, 1448, 1448: the clamp reaches an unshaped port",
    ],
  );
  assert.deepEqual(
    judged({
      ...SHAPED_READING,
      control: { ...SHAPED_READING.control, port: 45_650 },
    }),
    ["the control's port 45650 is inside the shaper's range"],
  );
  assert.deepEqual(judged(SHAPED_READING, [45_601, 45_603]), [
    "the probe reported ports 45601, 45602, not 45601, 45603",
  ]);
  assert.deepEqual(
    judged({
      ...SHAPED_READING,
      shaped: [{ ...first, connectMs: [], mss: [] }, second],
    }),
    ["port 45601: no connect"],
  );
});

test("a probe report without its fields is refused, not judged", () => {
  assert.deepEqual(
    parseKernelProbe(JSON.stringify(SHAPED_READING)),
    SHAPED_READING,
  );
  for (const bad of [
    "{}",
    JSON.stringify({ shaped: [], control: null }),
    JSON.stringify({ ...SHAPED_READING, shaped: [{ port: 45_601, mss: [] }] }),
    JSON.stringify({
      ...SHAPED_READING,
      control: { port: "52113", connectMs: [], mss: [] },
    }),
  ])
    assert.throws(() => parseKernelProbe(bad), /the probe reported/u, bad);
});

test("the probe itself, over unshaped loopback, gives a reading the kernel check fails", async () => {
  const server = createServer((socket) => socket.destroy());
  await listen(server, 0);
  const address = server.address();
  assert.ok(address && typeof address === "object");
  try {
    const result = spawnSync(
      "python3",
      [join(root, NETSHAPE_PROBE), "witness", "2", String(address.port)],
      { encoding: "utf8", timeout: 30_000 },
    );
    assert.equal(result.status, 0, result.stderr);
    const probe = parseKernelProbe(result.stdout);
    assert.equal(probe.shaped[0]?.port, address.port);
    assert.equal(probe.shaped[0]?.mss.length, 2);
    assert.equal(probe.control.connectMs.length, 2);
    const check = judgeKernel(CORPORATE, [address.port], probe);
    assert.equal(check.passed, false);
    assert.ok(
      check.problems.some((problem) =>
        problem.startsWith(`port ${String(address.port)}: MSS `),
      ),
      check.problems.join("; "),
    );
    assert.ok(
      !check.problems.some((problem) => problem.includes("control")),
      check.problems.join("; "),
    );
  } finally {
    await new Promise((done) => server.close(done));
  }
});

// The shaper's status --------------------------------------------------------------

const STATUS_UP = `scrub out on lo0 proto tcp from any to any port 45600 >< 45602 max-mss 1460 fragment reassemble
scrub out on lo0 proto tcp from any port 45600 >< 45602 to any max-mss 1460 fragment reassemble
scrub out on lo0 proto tcp from any to any port 45601 >< 45603 max-mss 1460 fragment reassemble
scrub out on lo0 proto tcp from any port 45601 >< 45603 to any max-mss 1460 fragment reassemble
dummynet out quick on lo0 proto tcp from any to any port = 45601 pipe 55601
dummynet out quick on lo0 proto tcp from any port = 45601 to any pipe 55602
dummynet out quick on lo0 proto tcp from any to any port = 45602 pipe 55601
dummynet out quick on lo0 proto tcp from any port = 45602 to any pipe 55602
55601: 500.000 Mbit/s   10 ms 1024 KB 0 queues (1 buckets) droptail
    mask: 0x00 0x00000000/0x0000 -> 0x00000000/0x0000
enable token: 9359096354202116286
`;

test("status after up must show pipe 55601 at the rate and half the RTT, and a pf reference", () => {
  assert.deepEqual(shaperStatusProblems(STATUS_UP, CORPORATE), []);
  assert.deepEqual(
    shaperStatusProblems(STATUS_UP, { ...CORPORATE, megabitsPerSecond: 100 }),
    ["pipe 55601 runs at 500.000 Mbit/s, not 100"],
  );
  assert.deepEqual(
    shaperStatusProblems(STATUS_UP, { ...CORPORATE, rttMs: 30 }),
    ["pipe 55601 delays 10 ms, not 15"],
  );
  assert.deepEqual(shaperStatusProblems("enable token: none\n", CORPORATE), [
    "no pipe 55601",
    "pf holds no enable token from it",
  ]);
  assert.equal(
    shaperStatusProblems(
      STATUS_UP.replace("55601: 500.000", "55601: 1000.000"),
      { ...CORPORATE, megabitsPerSecond: 1_000 },
    ).length,
    0,
  );
  assert.equal(shaperDisabled("enable token: none\n"), true);
  assert.equal(shaperDisabled(STATUS_UP), false);
  assert.equal(
    shaperDisabled(
      "dummynet out quick on lo0 proto tcp from any to any port = 45601 pipe 55601\nenable token: none\n",
    ),
    false,
  );
});

// The shaper's lifecycle -----------------------------------------------------------

interface Fake {
  readonly calls: string[];
  readonly shaper: KernelShaper;
}

function fakeShaper(
  options: {
    readonly installed?: string;
    readonly statusBefore?: string;
    readonly probe?: KernelProbe;
    readonly failing?: string;
  } = {},
): Fake {
  const calls: string[] = [];
  let up = false;
  const run: RunCommand = (command, arguments_) => {
    const line = [command, ...arguments_].join(" ");
    calls.push(line);
    const ok = (stdout: string) => ({
      status: 0,
      stdout,
      stderr: "",
      error: null,
    });
    if (options.failing !== undefined && line.includes(options.failing))
      return { status: 1, stdout: "", stderr: "refused\n", error: null };
    if (command === "python3" && arguments_[0] === "--version")
      return ok("Python 3.9.6\n");
    if (command === "python3")
      return ok(JSON.stringify(options.probe ?? SHAPED_READING));
    const verb = arguments_[2];
    if (verb === "up") up = true;
    if (verb === "down") up = false;
    if (verb === "status")
      return ok(
        up ? STATUS_UP : (options.statusBefore ?? "enable token: none\n"),
      );
    return ok(`calque-netshape: ${verb ?? ""}\n`);
  };
  const read = (path: string) =>
    Buffer.from(
      path === NETSHAPE ? (options.installed ?? "#!/bin/sh\n") : "#!/bin/sh\n",
    );
  return {
    calls,
    shaper: new KernelShaper(CORPORATE, "source", "probe.py", { run, read }),
  };
}

const SUDO = `sudo -n ${NETSHAPE}`;

test("a shaped run goes up on its ports, checks the kernel, and comes down once, reading status back each time", () => {
  const { calls, shaper } = fakeShaper();
  shaper.preflight();
  shaper.engage([45_601, 45_602]);
  assert.equal(shaper.checkAfter(), null);
  assert.equal(shaper.down(), true);
  assert.equal(shaper.down(), null);
  assert.deepEqual(calls, [
    `${SUDO} status`,
    "python3 --version",
    `${SUDO} up 500 20 45601 45602`,
    `${SUDO} status`,
    "python3 probe.py witness 3 45601 45602",
    "python3 probe.py witness 3 45601 45602",
    `${SUDO} down`,
    `${SUDO} status`,
  ]);
  const record = shaper.record();
  assert.equal(
    record.sha256,
    createHash("sha256").update("#!/bin/sh\n").digest("hex"),
  );
  assert.equal(record.python, "Python 3.9.6");
  assert.deepEqual(record.ports, [45_601, 45_602]);
  assert.equal(record.statusAfterUp, STATUS_UP);
  assert.equal(record.statusAfterDown, "enable token: none\n");
  assert.equal(record.kernelChecks.before?.passed, true);
  assert.equal(record.kernelChecks.after?.passed, true);
  assert.deepEqual(shaper.checks, record.kernelChecks);
});

test("an installed script that is not the reviewed source is refused before any sudo", () => {
  const { calls, shaper } = fakeShaper({ installed: "#!/bin/sh\nexit 0\n" });
  assert.throws(
    () => shaper.preflight(),
    /^Error: \/usr\/local\/libexec\/calque-netshape has SHA-256 [0-9a-f]{64} and scripts\/netshape\/calque-netshape has [0-9a-f]{64}: install the reviewed script/u,
  );
  assert.throws(() => shaper.engage([45_601]), /has SHA-256/u);
  assert.deepEqual(calls, []);
  assert.equal(shaper.down(), null);
  assert.deepEqual(calls, []);
});

test("a shaper already up is refused at preflight, and nothing tears it down", () => {
  const { calls, shaper } = fakeShaper({ statusBefore: STATUS_UP });
  assert.throws(() => shaper.preflight(), /calque-netshape is already up/u);
  assert.equal(shaper.down(), null);
  assert.deepEqual(calls, [`${SUDO} status`]);
});

test("a kernel check that fails before the first sample stops the run, and down still runs", () => {
  const unclamped: KernelProbe = {
    ...SHAPED_READING,
    shaped: SHAPED_READING.shaped.map((reading) => ({
      ...reading,
      mss: [16_332, 16_332, 16_332],
    })),
  };
  const { calls, shaper } = fakeShaper({ probe: unclamped });
  assert.throws(
    () => shaper.engage([45_601, 45_602]),
    /^Error: the kernel check before the first sample failed: port 45601: MSS 16332, 16332, 16332, above the clamp's 1460/u,
  );
  assert.equal(shaper.down(), true);
  assert.deepEqual(calls.slice(-2), [`${SUDO} down`, `${SUDO} status`]);
});

test("an up that fails still comes down, and a failed check after the last sample is the run's shaping failure", () => {
  const failedUp = fakeShaper({ failing: " up " });
  assert.throws(
    () => failedUp.shaper.engage([45_601]),
    /^Error: sudo -n \/usr\/local\/libexec\/calque-netshape up 500 20 45601 failed: refused$/u,
  );
  assert.equal(failedUp.shaper.down(), true);
  assert.deepEqual(failedUp.calls.slice(-2), [
    `${SUDO} down`,
    `${SUDO} status`,
  ]);

  const probeFails = fakeShaper({ failing: "witness" });
  assert.throws(() => probeFails.shaper.engage([45_601]), /the probe failed/u);
  assert.match(
    probeFails.shaper.checkAfter() ?? "",
    /^the kernel check after the last sample failed: the probe failed/u,
  );
});

// Ports ----------------------------------------------------------------------------

function listen(server: Server, port: number): Promise<void> {
  return new Promise((done, fail) => {
    server.once("error", fail);
    server.listen({ host: "127.0.0.1", port }, done);
  });
}

test("a shaped listener's port is free, inside 45600-45699, and not one already taken", async () => {
  const port = await availableShapedPort();
  assert.ok(port >= SHAPED_PORT_MIN && port <= SHAPED_PORT_MAX);
  const busy = createServer();
  await listen(busy, port);
  try {
    const others = Array.from(
      { length: SHAPED_PORT_MAX - SHAPED_PORT_MIN + 1 },
      (_, index) => SHAPED_PORT_MIN + index,
    ).filter((candidate) => candidate !== port);
    const left = others.pop();
    assert.ok(left !== undefined);
    // Every port but the busy one and `left` is taken.
    assert.equal(await availableShapedPort(new Set(others)), left);
    await assert.rejects(
      availableShapedPort(new Set([...others, left])),
      /no port in 45600-45699 is free on 127\.0\.0\.1/u,
    );
  } finally {
    await new Promise((done) => busy.close(done));
  }
});

// The installed instrument ---------------------------------------------------------

test("the install procedure names the committed script's SHA-256", () => {
  const digest = createHash("sha256")
    .update(readFileSync(join(root, NETSHAPE_SOURCE)))
    .digest("hex");
  const readme = readFileSync(join(root, "scripts/netshape/README.md"), "utf8");
  assert.deepEqual(
    [...readme.matchAll(/\b[0-9a-f]{64}\b/gu)].map((match) => match[0]),
    [digest],
  );
});

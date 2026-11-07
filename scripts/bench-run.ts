/** A run's phases in order: seed, prepare, hold, tail, reference pass, cold samples, realtime session, and the summary they feed. */
import { cpSync, mkdirSync, readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { brotliSummary, plannedArtifact } from "./bench-brotli.ts";
import {
  destroyState,
  holdFailure,
  holdText,
  holdUntilLogged,
  startCalque,
  waitForReady,
} from "./bench-calque.ts";
import {
  type ColdResult,
  coldSample,
  referencePass,
  settleCold,
  withRetryMarks,
} from "./bench-cold.ts";
import { recutArguments, unjudgedReasons } from "./bench-conditions.ts";
import {
  createContentGenerator,
  defaultContentFixtureSeams,
  seedContentFixture,
} from "./bench-content-fixture.ts";
import {
  expectedObject,
  referenceOf,
  type Run,
  servedBrotli,
} from "./bench-context.ts";
import {
  evaluateValidationReport,
  FIXTURE_DATABASE_ENV,
} from "./bench-fixture.ts";
import { emptyPagePeaks } from "./bench-baseline.ts";
import { hrtimeMs, readEnvironment } from "./bench-host.ts";
import { judgePreflight, kernelLine, preflightLine } from "./bench-netshape.ts";
import {
  BENCH_TABLES,
  benchCountProblems,
  benchLoadDdl,
  benchSeedSql,
  benchValidationSql,
  DATASETS,
  QUERY_VIEW,
  readQueryExpectation,
  readSourceState,
  SEED_ORDER,
  tableCountsSql,
} from "./bench-oracle.ts";
import { requestPlan } from "./bench-page-server.ts";
import { runSql } from "./bench-postgres.ts";
import { realtimePasses, realtimeSession } from "./bench-realtime.ts";
import { log, redactedUrl, writeRecord } from "./bench-record.ts";
import {
  artifactObjectLine,
  artifactObjectReasons,
  directionalReasons,
  distribution,
  edgeProtocolLine,
  judgeArtifactObject,
  judgeEdgeProtocol,
  judgePageCoding,
  pageCodingLine,
  rankedValue,
  runPasses,
  thresholdRows,
  wireByPurpose,
} from "./bench-results.ts";
import { RETAKES, takeSamples } from "./bench-samples.ts";
import {
  readRecut,
  type RecutRecord,
  RecutSampler,
  recutWindows,
  settleRecuts,
  type TailRecord,
  writeTail,
} from "./bench-tail.ts";
import { delay } from "./demo-process.ts";
import { parsePrivateClientEnvironment, stopChild } from "./demo.ts";

const AUTOVACUUM_TIMEOUT_MS = 600_000;

/** `measure`, then the run's `calque dev` stopped and its state destroyed. */
export async function runBench(run: Run) {
  try {
    return await measure(run);
  } finally {
    if (run.server) {
      await stopChild(run.server.handle);
      run.servers.delete(run.server.handle);
    }
    run.server = null;
    try {
      destroyState(run.calque, run.stateDirectory, run.serviceEnvironment);
    } catch (error) {
      log(error instanceof Error ? error.message : String(error));
    }
  }
}

/**
 * The 100k rows from `--dataset`, the sentinels and the load table, refused
 * unless v1's checks at the cut (`benchValidationChecks`) pass. A content-v1
 * cut is seeded by the variant's own seed, which asks the variant's checks
 * too.
 */
async function seedSource(
  run: Pick<Run, "options" | "sourceEnvironment" | "serviceEnvironment">,
): Promise<void> {
  if (run.options.dataset === "v1") {
    runSql(benchSeedSql(), run.sourceEnvironment);
    const validated = evaluateValidationReport(
      runSql(benchValidationSql(), run.sourceEnvironment),
    );
    if (!validated.valid)
      throw new Error(
        `the v1 cut failed v1's checks: ${validated.failedChecks.join(", ")}`,
      );
    return;
  }
  await seedContentFixture(
    {
      ...run.sourceEnvironment,
      [FIXTURE_DATABASE_ENV]: run.serviceEnvironment.CALQUE_DATABASE_URL,
    },
    defaultContentFixtureSeams,
    BENCH_TABLES,
    createContentGenerator(),
    benchValidationSql(),
  );
  runSql(benchLoadDdl, run.sourceEnvironment);
}

async function measure(run: Run) {
  log(`seeding ${DATASETS[run.options.dataset]}`);
  const seedStarted = hrtimeMs();
  await seedSource(run);
  const seedMs = hrtimeMs() - seedStarted;
  const counted = new Map(
    runSql(tableCountsSql, run.sourceEnvironment)
      .split("\n")
      .filter((line) => line.length > 0)
      .map((line) => {
        const [table = "", count = ""] = line.split("\t");
        return [table, Number(count)] as const;
      }),
  );
  const countProblems = benchCountProblems(counted);
  if (countProblems.length > 0)
    throw new Error(`the seed is wrong: ${countProblems.join("; ")}`);
  // The vacuum and the autovacuum wait keep preparation times comparable
  // across runs.
  runSql(
    `VACUUM (ANALYZE) ${[...SEED_ORDER, "bench_load"].map((table) => `incidents.${table}`).join(", ")};\n`,
    run.sourceEnvironment,
  );
  const vacuumStarted = hrtimeMs();
  for (;;) {
    const workers = Number(
      runSql(
        "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'autovacuum worker';\n",
        run.adminEnvironment,
      ).trim(),
    );
    if (workers === 0) break;
    if (hrtimeMs() - vacuumStarted > AUTOVACUUM_TIMEOUT_MS)
      throw new Error("an autovacuum worker was still running after 600 s");
    await delay(500);
  }
  const autovacuumWaitMs = hrtimeMs() - vacuumStarted;

  const preparations: (number | null)[] = [];
  for (let n = 0; n < run.plan.preparations; n += 1) {
    const last = n === run.plan.preparations - 1;
    destroyState(run.calque, run.stateDirectory, run.serviceEnvironment);
    mkdirSync(dirname(run.stateDirectory), { recursive: true });
    const before = readEnvironment(run.postgres);
    const logPath = join(run.directory, "logs", `prepare-${String(n)}.log`);
    const started = hrtimeMs();
    const server = startCalque(
      run.calque,
      run.stateDirectory,
      run.calquePort,
      run.calqueMetricsPort,
      run.pageOrigin,
      run.calquePublicBaseUrl,
      run.serviceEnvironment,
      logPath,
      recutArguments(run.options),
    );
    run.server = server;
    run.servers.add(server.handle);
    const ready = await waitForReady(server, started);
    const after = readEnvironment(run.postgres);
    run.readings.push(before, after);
    const ms = "ms" in ready ? ready.ms : null;
    preparations.push(ms);
    writeRecord(run, `prepare-${String(n)}`, {
      kind: "prepare",
      n,
      verdict: ms === null ? "DNC" : "PASS",
      failure: "failure" in ready ? ready.failure : null,
      server_preparation_ms: ms,
      log: relative(run.directory, logPath),
      environment: { before, after },
    });
    log(
      `preparation ${String(n)}: ${ms === null ? `DNC (${"failure" in ready ? ready.failure : ""})` : `${ms.toFixed(0)} ms`}`,
    );
    if (!last || ms === null) {
      await stopChild(server.handle);
      run.servers.delete(server.handle);
      run.server = null;
    }
    if (last && ms === null)
      throw new Error("the last preparation did not complete");
  }
  if (!run.server) throw new Error("the last preparation did not complete");
  let tail: TailRecord | null = null;
  const holdFor = holdText(run.options);
  log(`holding until calque logs "${holdFor}"`);
  const hold = await holdUntilLogged(
    run.server.log,
    run.server.handle.exited,
    holdFor,
  );
  log(
    `held ${hold.heldMs.toFixed(0)} ms: ${hold.logged ? "logged" : "not logged"}`,
  );
  const failure = holdFailure(run.options, hold);
  if (failure !== null) throw new Error(failure);
  // The artifacts every cold sample fetches, before teardown destroys them.
  if (run.options.diagnostic)
    cpSync(
      join(run.stateDirectory, "objects"),
      join(run.directory, "objects"),
      { recursive: true },
    );
  const settings = parsePrivateClientEnvironment(
    readFileSync(join(run.stateDirectory, "client.env"), "utf8"),
  );
  if (!run.server || settings.endpoint !== run.server.endpoint)
    throw new Error(
      "client.env names another endpoint than the running calque dev",
    );
  run.connection = {
    // client.env names calque's loopback address; the page reaches it through
    // the edge when there is one.
    endpoint: `${run.server.publicEndpoint}/`,
    bearerToken: settings.bearerToken,
  };
  // Outside the `try`: an answer that is no plan ends the run.
  const plan = await requestPlan(run.connection, `${run.server.endpoint}/`);
  try {
    run.calqueArtifact = plannedArtifact(plan);
    log(
      `calque plans its ${run.calqueArtifact.encoding} object of ${String(run.calqueArtifact.encodedBytes)} bytes`,
    );
  } catch (error) {
    log(
      `the run's own plan names no single compressed artifact: ${String(error)}`,
    );
  }

  let recut: RecutRecord | null = null;
  if (run.options.tail !== undefined) {
    if (!run.server) throw new Error("--tail needs the serving calque");
    const recutsBefore = (await readRecut(run.server.metricsEndpoint)).recuts;
    const sampler = new RecutSampler(run.server, run.stateDirectory);
    sampler.start();
    try {
      tail = await writeTail(run.server.metricsEndpoint, run, run.options.tail);
      const settled = await settleRecuts(run.server, recutsBefore);
      await sampler.stop();
      recut = { ...settled, windows: recutWindows(sampler.samples) };
    } finally {
      await sampler.stop();
    }
    log(
      `re-cut ${String(recut.recuts)} times; ${String(recut.tailEntries)} entries past the newest cut ${(recut.settleMs / 1000).toFixed(0)} s after the tail`,
    );
    if (sampler.errors.length > 0)
      log(`re-cut sampler: ${String(sampler.errors.length)} failed samples`);
    // Every page after this plans the newest cut, so the run judges its object.
    if (recut.recuts > 0) {
      const replanned = await requestPlan(
        run.connection,
        `${run.server.endpoint}/`,
      );
      run.calqueArtifact = plannedArtifact(replanned);
      log(
        `calque now plans its ${run.calqueArtifact.encoding} object of ${String(run.calqueArtifact.encodedBytes)} bytes`,
      );
    }
  }

  log("reading PostgreSQL");
  run.source = readSourceState(run.tables, run.sourceEnvironment);
  run.queryExpectation = readQueryExpectation(run.sourceEnvironment);
  if (run.shaper) {
    const ports = [...run.shapedPorts];
    log(`shaping loopback ports ${ports.join(", ")} in the kernel`);
    run.shaper.engage(ports);
    const before = run.shaper.checks.before;
    if (before) log(kernelLine("before the first sample", before));
  }
  log("reference pass");
  await referencePass(run);

  const warmups: ColdResult[] = [];
  const traced: ColdResult[] = [];
  for (let n = 0; n < run.plan.warmups; n += 1) {
    const sample = await coldSample(run, "warmup", n);
    settleCold(run, sample, null);
    warmups.push(sample);
  }
  const taken = await takeSamples(
    {
      warmups: 0,
      judged: run.plan.cold,
      retakes: RETAKES,
      valueMs: (sample: ColdResult) => sample.reachedMs,
    },
    (n) => coldSample(run, "cold", n),
    (sample, { retake }) => settleCold(run, sample, retake),
  );
  const cold = taken.judged;
  for (let n = 0; n < run.plan.traced; n += 1) {
    const sample = await coldSample(run, "traced", n);
    settleCold(run, sample, null);
    traced.push(sample);
  }

  const afterCold = readSourceState(run.tables, run.sourceEnvironment);
  const movedTables = Object.keys(run.source.digests).filter(
    (collection) =>
      afterCold.digests[collection] !== run.source?.digests[collection],
  );
  log("empty pages");
  const emptyPageRss = await emptyPagePeaks(run);
  log("realtime");
  const realtime = await realtimeSession(run);
  const kernelFailure = run.shaper?.checkAfter() ?? null;
  const after = run.shaper?.checks.after ?? null;
  if (after) log(kernelLine("after the last sample", after));
  else if (kernelFailure !== null) log(kernelFailure);
  const preflight = judgePreflight(
    run.options.network,
    cold.map((sample) => ({ name: sample.name, requests: sample.requests })),
  );
  if (run.shaper) log(preflightLine(preflight));
  const pageCoding = judgePageCoding(
    run.pageOrigin,
    cold.map((sample) => ({ name: sample.name, requests: sample.requests })),
  );
  log(pageCodingLine(pageCoding));
  const edgeProtocol = judgeEdgeProtocol(
    run.options.edge,
    cold.map((sample) => ({ name: sample.name, requests: sample.requests })),
  );
  log(edgeProtocolLine(edgeProtocol));
  const artifactObject = judgeArtifactObject(
    expectedObject(run),
    cold.map((sample) => ({ name: sample.name, requests: sample.requests })),
  );
  log(artifactObjectLine(artifactObject));
  const shapingFailure =
    [realtime.shapingFailure, kernelFailure]
      .filter((failure) => failure !== null)
      .join("; ") || null;

  const incorrect =
    cold.some((sample) => sample.verdict === "INCORRECT") ||
    cold.some((sample) => sample.query?.verdict === "INCORRECT") ||
    traced.some((sample) => sample.verdict === "INCORRECT") ||
    realtime.probes.some((sample) => sample.verdict === "INCORRECT") ||
    realtime.finalProblems.length > 0;
  const correctnessFailed = incorrect || movedTables.length > 0;
  const passedCold = cold.filter((sample) => sample.verdict === "PASS");
  const measurements = {
    preparationsMs: preparations,
    coldMs: cold.map((sample) =>
      rankedValue({ verdict: sample.verdict, valueMs: sample.coldMs }),
    ),
    coldRetaken: taken.retaken,
    commitMs: realtime.probes.map((sample) =>
      rankedValue({ verdict: sample.verdict, valueMs: sample.upperMs }),
    ),
    commitRetaken: realtime.retaken,
    // A sample taken again measured the page as well as any other.
    peakRssBytes: [
      ...taken.samples.map((sample) => sample.peakRssBytes),
      realtime.peakRssBytes,
    ],
    longestTaskMs: [
      ...taken.samples.map((sample) => sample.longestTaskMs),
      realtime.longestTaskMs,
    ],
    achievedRowChangesPerSecond: realtime.rowChangesPerSecond,
    correctnessFailed,
    realtimeVoided: realtime.voidReason !== null,
  };
  const rows = thresholdRows(measurements);
  // Reported, not judged: no threshold reads any of the three.
  const queryMs = cold.map((sample) =>
    rankedValue({
      verdict: sample.query?.verdict ?? "DNC",
      valueMs: sample.query?.queryMs ?? null,
    }),
  );
  const wire = passedCold.map((sample) => wireByPurpose(sample.requests));
  const milestone = (name: string) =>
    distribution(
      passedCold.map((sample) => {
        const value = sample.coldLoad?.[name];
        return typeof value === "number" ? value : null;
      }),
    );
  const reasons = directionalReasons(run.readings);
  const unjudged = [
    ...unjudgedReasons(run.options),
    ...artifactObjectReasons(artifactObject),
  ];
  return {
    rows: Object.fromEntries(
      SEED_ORDER.map((table) => [table, counted.get(table) ?? 0]),
    ),
    seed: { ms: seedMs, autovacuumWaitMs },
    correctness: {
      failed: correctnessFailed,
      incorrectSamples: incorrect,
      tablesMovedDuringColdSamples: movedTables,
      finalOracle: realtime.finalOracle,
      finalProblems: realtime.finalProblems,
      finalFailure: realtime.finalFailure,
    },
    verdicts: {
      cold: cold.map((sample) => sample.verdict),
      query: cold.map((sample) => sample.query?.verdict ?? "DNC"),
      traced: traced.map((sample) => sample.verdict),
      probes: realtime.probes.map((sample) => sample.verdict),
    },
    retaken: { cold: taken.retaken, probes: realtime.retaken },
    artifactRetries: {
      warmup: withRetryMarks(warmups),
      cold: withRetryMarks(taken.samples),
      traced: withRetryMarks(traced),
      realtime: realtime.artifactRetryMarks.length > 0 ? 1 : 0,
    },
    browserSessions: {
      sessions: run.launches.length,
      relaunched: run.launches.filter((launches) => launches > 1).length,
    },
    server_preparation_ms: distribution(preparations),
    hold,
    tail,
    recut,
    // What calque planned for the run's own request after the hold, or after
    // the tail's re-cuts.
    calqueArtifact:
      run.calqueArtifact === null
        ? null
        : {
            artifactId: run.calqueArtifact.artifactId,
            encoding: run.calqueArtifact.encoding,
            encodedBytes: run.calqueArtifact.encodedBytes,
            objectUrl: redactedUrl(run.calqueArtifact.downloadUrl),
          },
    complete_stable_paint_ms: distribution(measurements.coldMs),
    commit_to_stable_paint_ms: distribution(measurements.commitMs),
    milestones: {
      readyAt: milestone("readyAt"),
      firstWindowAt: milestone("firstWindowAt"),
      gridPaintedAt: milestone("gridPaintedAt"),
      chartAt: milestone("chartAt"),
      issueAt: milestone("issueAt"),
    },
    // `unfinishedWire` is the part of `wire` that is a lower bound.
    bytes: {
      wire: distribution(passedCold.map((sample) => sample.wireBytes)),
      unfinishedWire: distribution(
        passedCold.map((sample) => sample.unfinishedWireBytes),
      ),
      declaredBody: distribution(
        passedCold.map((sample) => sample.declaredBodyBytes),
      ),
      decodedBody: distribution(
        passedCold.map((sample) => sample.decodedBodyBytes),
      ),
      // Encoded bytes by what they carried, one figure per passed cold sample.
      byPurpose: {
        plan: distribution(wire.map((bytes) => bytes.plan)),
        artifact: distribution(wire.map((bytes) => bytes.artifact)),
        catchUp: distribution(wire.map((bytes) => bytes.catchUp)),
        page: distribution(wire.map((bytes) => bytes.page)),
        total: distribution(wire.map((bytes) => bytes.total)),
      },
    },
    // One interaction on each judged cold sample's loaded page, from its first
    // input event to the stable paint of its result; reported, not judged.
    post_load_query_ms: distribution(queryMs),
    post_load_query: {
      interaction: QUERY_VIEW,
      expectedMatches: run.queryExpectation?.rows ?? null,
    },
    traced: traced.map((sample) => sample.trace),
    browser_process_tree_peak_rss_bytes: measurements.peakRssBytes,
    // The same browser on a blank page, sampled the same way; reported.
    empty_page_peak_rss_bytes: emptyPageRss,
    main_thread_long_task_ms: measurements.longestTaskMs,
    achievedRowChangesPerSecond: realtime.rowChangesPerSecond,
    // `shaped: null` where the witness does not apply.
    shaping: realtime.shaping,
    shapingFailure,
    engagement: realtime.engagement,
    // Why the network memory guard voided the realtime session, or null.
    realtimeVoid: realtime.voidReason,
    // `shaped: null`, with its reason, outside a kernel-shaped run or where a
    // judged cold sample's ledger does not show the preflight.
    preflight,
    // `brotli: null`, with its reason, where a judged cold sample's ledger
    // shows no page response.
    pageCoding,
    // `h2: null`, with its reason, without the edge or where a judged cold
    // sample's ledger shows no response.
    edgeProtocol,
    // `one: null`, with its reason, where the run expects no artifact object.
    artifactObject,
    screenshotCalibration: referenceOf(run).screenshotCalibration,
    thresholds: rows,
    passes:
      runPasses(rows, correctnessFailed, unjudged.length > 0) &&
      realtimePasses({ finalFailure: realtime.finalFailure, shapingFailure }),
    directional: {
      label: run.options.smoke || unjudged.length > 0 || reasons.length > 0,
      reasons: [
        ...(run.options.smoke ? ["smoke run"] : []),
        ...unjudged,
        ...reasons,
      ],
    },
    ...(servedBrotli(run) === null ? {} : { brotli: brotliSummary(cold) }),
  };
}

import assert from "node:assert/strict";
import test from "node:test";

import {
  mapEpochSecondsToMonotonicMilliseconds,
  STABLE_PAINT_RULE,
  StableFrameDetector,
} from "./bench-frames.ts";

const expectedHash = "a".repeat(64);
const otherHash = "b".repeat(64);

/**
 * Chrome numbers the screencast session once per `StartScreencast`, from 1,
 * and sends that `sessionId` on every frame. The frames below carry it the
 * way Chrome does.
 */
const SCREENCAST_SESSION = 1;

function frame(
  monotonicTimestampMilliseconds: number,
  roiSha256: string,
  pageContractQualified = true,
): {
  readonly sessionId: number;
  readonly monotonicTimestampMilliseconds: number;
  readonly roiSha256: string;
  readonly pageContractQualified: boolean;
} {
  return {
    sessionId: SCREENCAST_SESSION,
    monotonicTimestampMilliseconds,
    roiSha256,
    pageContractQualified,
  };
}

test("the stable-paint rule is the first uncontradicted frame with a 2,000 ms hold", () => {
  assert.deepEqual(STABLE_PAINT_RULE, {
    kind: "first-uncontradicted-presented-frame",
    holdWindowMilliseconds: 2_000,
  });
  assert.ok(Object.isFrozen(STABLE_PAINT_RULE));
});

test("a frame from another screencast session is retained and permanently invalidates the detector", () => {
  const detector = new StableFrameDetector(expectedHash);
  detector.observe(frame(100, expectedHash));
  // A second StartScreencast would hand out session 2; its frames are not
  // the run's frames.
  assert.equal(
    detector.observe({ ...frame(132, expectedHash), sessionId: 2 }),
    null,
  );
  assert.deepEqual(detector.droppedFrameSignals, [
    { kind: "session-id-changed", expectedSessionId: 1, sessionId: 2 },
  ]);
  assert.equal(detector.invalid, true);
  assert.equal(
    detector.observe({ ...frame(149, expectedHash), sessionId: 2 }),
    null,
  );
});

test("a frame-swap timestamp that does not advance is retained as lost delivery", () => {
  const detector = new StableFrameDetector(expectedHash);
  detector.observe(frame(100, expectedHash));
  assert.equal(detector.observe(frame(100, expectedHash)), null);
  assert.deepEqual(detector.droppedFrameSignals, [
    {
      kind: "non-increasing-timestamp",
      previousTimestampMilliseconds: 100,
      timestampMilliseconds: 100,
    },
  ]);
  assert.equal(detector.invalid, true);
});

test("epoch frame-swap timestamps map onto the CDP monotonic clock", () => {
  assert.equal(
    mapEpochSecondsToMonotonicMilliseconds(1_800_000_001.25, -1_700_000_000),
    100_000_001_250,
  );
  assert.throws(() =>
    mapEpochSecondsToMonotonicMilliseconds(Number.NaN, -1_700_000_000),
  );
});

test("T_present is the first uncontradicted qualifying frame from the first uncontradicted qualifying frame", () => {
  // The complete grid paint is often the page's last compositor
  // damage, so no second reference-equal frame arrives after it.
  const detector = new StableFrameDetector(expectedHash);
  assert.equal(detector.observe(frame(1_000, otherHash, false)), null);
  assert.equal(detector.observe(frame(1_213, expectedHash)), 1_213);
  assert.equal(detector.matchTimestampMilliseconds, 1_213);
  assert.equal(detector.holdWindowContradiction, null);
  assert.equal(detector.holdWindowFrameCount, 0);
  assert.equal(detector.invalid, false);
});

test("the match cannot be taken from a frame presented before T_nav", () => {
  // The run discards a T_present earlier than T_nav. The rule has a single
  // match, so latching on a frame the run discards would spend it and leave
  // every later frame able only to contradict: a sample refused for
  // contradicting a T_present it never had.
  const detector = new StableFrameDetector(expectedHash);
  assert.equal(
    detector.observe({
      ...frame(900, expectedHash),
      navigationTimestampMilliseconds: 1_000,
    }),
    null,
  );
  assert.equal(detector.matchTimestampMilliseconds, null);
  // Before navigation has been timed at all, no frame can be the match.
  assert.equal(
    detector.observe({
      ...frame(950, expectedHash),
      navigationTimestampMilliseconds: null,
    }),
    null,
  );
  assert.equal(detector.matchTimestampMilliseconds, null);
  // The first qualifying frame after T_nav still matches, and the hold window
  // is judged from it rather than from either discarded frame.
  assert.equal(
    detector.observe({
      ...frame(1_213, expectedHash),
      navigationTimestampMilliseconds: 1_000,
    }),
    1_213,
  );
  assert.equal(detector.matchTimestampMilliseconds, 1_213);
  assert.equal(detector.holdWindowFrameCount, 0);
  assert.equal(detector.holdWindowContradiction, null);
  // A navigation timestamp that is not a number is a broken observation.
  assert.throws(
    () =>
      new StableFrameDetector(expectedHash).observe({
        ...frame(1_213, expectedHash),
        navigationTimestampMilliseconds: Number.NaN,
      }),
    /invalid screencast frame observation/u,
  );
});

test("a frame inside the hold window that contradicts the reference refuses the sample", () => {
  const detector = new StableFrameDetector(expectedHash);
  assert.equal(detector.observe(frame(1_213, expectedHash)), 1_213);
  // 1,999 ms later: still inside the 2,000 ms window.
  assert.equal(detector.observe(frame(3_212, otherHash)), null);
  const contradiction = detector.holdWindowContradiction;
  assert.notEqual(contradiction, null);
  assert.deepEqual(contradiction, {
    holdWindowMilliseconds: 2_000,
    matchTimestampMilliseconds: 1_213,
    contradictingTimestampMilliseconds: 3_212,
    roiSha256: otherHash,
    pageContractQualified: true,
  });
  assert.equal(detector.holdWindowFrameCount, 1);
  // A dropped frame is a different refusal.
  assert.equal(detector.invalid, false);
  // The first contradiction is retained; a later one does not overwrite the
  // evidence of what took the paint away.
  detector.observe(frame(3_213, "c".repeat(64)));
  assert.deepEqual(detector.holdWindowContradiction, contradiction);
});

test("a reference-equal frame whose page contract regressed contradicts too", () => {
  const detector = new StableFrameDetector(expectedHash);
  assert.equal(detector.observe(frame(1_213, expectedHash)), 1_213);
  assert.equal(detector.observe(frame(1_400, expectedHash, false)), null);
  assert.equal(detector.holdWindowContradiction?.pageContractQualified, false);
  assert.equal(detector.holdWindowContradiction?.roiSha256, expectedHash);
});

test("the hold window is measured in the frames' own presentation timeline", () => {
  const detector = new StableFrameDetector(expectedHash);
  assert.equal(detector.observe(frame(1_213, expectedHash)), 1_213);
  // At the boundary: still inside, and a repeat of the reference confirms
  // rather than contradicts.
  assert.equal(detector.observe(frame(3_213, expectedHash)), null);
  assert.equal(detector.holdWindowFrameCount, 1);
  assert.equal(detector.holdWindowContradiction, null);
  // One millisecond past the window: outside, and not a contradiction even
  // though it is a different paint. The sample's number is already final.
  assert.equal(detector.observe(frame(3_214, otherHash)), null);
  assert.equal(detector.holdWindowFrameCount, 1);
  assert.equal(detector.holdWindowContradiction, null);
});

test("a dropped frame inside the hold window is still a dropped frame", () => {
  const detector = new StableFrameDetector(expectedHash);
  assert.equal(detector.observe(frame(1_213, expectedHash)), 1_213);
  detector.invalidate({ kind: "screencast-hidden" });
  assert.equal(detector.invalid, true);
  // The screencast stopped showing the page: the next frame cannot qualify,
  // so it contradicts the held match rather than confirming it.
  assert.equal(detector.observe(frame(1_400, expectedHash)), null);
  assert.equal(detector.holdWindowContradiction?.pageContractQualified, true);
  assert.deepEqual(detector.droppedFrameSignals, [
    { kind: "screencast-hidden" },
  ]);
});

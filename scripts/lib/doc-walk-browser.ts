// What the doc walks share in the page they drive: waiting for the task list,
// and counting the requests a load makes and the ones that failed.

import type { BrowserContext, Page, Request } from "playwright";

const CHANGE_TIMEOUT_MS = 60_000;

/** Waits for `#tasks` to read `expected`, failing at once on an error status. */
export async function expectTasks(
  opened: Page,
  step: string,
  expected: readonly string[],
): Promise<void> {
  const want = JSON.stringify(expected);
  try {
    const settled = await opened.waitForFunction(
      (target) => {
        const status = document.getElementById("status")?.textContent ?? "";
        if (/^(?:error|locked):/u.test(status)) return { failed: status };
        const tasks = [...document.querySelectorAll("#tasks li")].map(
          (item) => item.textContent,
        );
        return JSON.stringify(tasks) === target ? { failed: null } : false;
      },
      want,
      { timeout: CHANGE_TIMEOUT_MS, polling: 100 },
    );
    const { failed } = (await settled.jsonValue()) as {
      readonly failed: string | null;
    };
    if (failed !== null) throw new Error(`the page reports "${failed}"`);
  } catch (error) {
    const seen = await opened.evaluate(() => ({
      status: document.getElementById("status")?.textContent ?? "",
      tasks: [...document.querySelectorAll("#tasks li")].map(
        (item) => item.textContent,
      ),
    }));
    throw new Error(
      `${step}: expected tasks ${want}; the page shows ${JSON.stringify(seen.tasks)} with status "${seen.status}" (${String(error).split("\n")[0] ?? ""})`,
    );
  }
}

// `calque` serves the bootstrap plan and each artifact `no-store, private`,
// and the client reads both through a stream. Chromium ends such a request as
// canceled whenever the page's last read comes after the network's last byte,
// although the page received every byte, so a request answered 200 can end
// this way on a load that worked. The steps say the artifact arrived, and the
// one bootstrap request the run allows says the plan did: the client asks
// again for a plan whose body the network cut.
const STREAMED_BODY_END = "net::ERR_ABORTED";

// A path may sit under a proxy's prefix, such as `/_calque/`.
function isBootstrapRequest(method: string, url: string): boolean {
  return method === "POST" && new URL(url).pathname.endsWith("/v1/bootstrap");
}

function isStreamedRequest(method: string, url: string): boolean {
  return (
    isBootstrapRequest(method, url) ||
    (method === "GET" &&
      new URL(url).pathname.includes("/v1/artifact-objects/"))
  );
}

export interface RequestWatch {
  /** Every request that failed or was answered 400 or above. */
  readonly failedRequests: readonly string[];
  readonly bootstrapRequests: () => number;
  readonly streamedReadsEndedCanceled: () => number;
}

/** Counts what the pages of `context` request, from before they open. */
export function watchRequests(context: BrowserContext): RequestWatch {
  const failedRequests: string[] = [];
  const answeredStreams = new WeakSet<Request>();
  let streamedReadsEndedCanceled = 0;
  let bootstrapRequests = 0;
  context.on("requestfailed", (request) => {
    const error = request.failure()?.errorText ?? "unknown";
    if (error === STREAMED_BODY_END && answeredStreams.has(request)) {
      streamedReadsEndedCanceled += 1;
      return;
    }
    failedRequests.push(`${request.method()} ${request.url()}: ${error}`);
  });
  context.on("request", (request) => {
    if (isBootstrapRequest(request.method(), request.url()))
      bootstrapRequests += 1;
  });
  context.on("response", (response) => {
    if (
      response.status() === 200 &&
      isStreamedRequest(response.request().method(), response.url())
    )
      answeredStreams.add(response.request());
    if (response.status() >= 400)
      failedRequests.push(
        `${response.request().method()} ${response.url()}: HTTP ${String(response.status())}`,
      );
  });
  return {
    failedRequests,
    bootstrapRequests: () => bootstrapRequests,
    streamedReadsEndedCanceled: () => streamedReadsEndedCanceled,
  };
}

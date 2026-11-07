import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { request } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { brotliDecompressSync, gunzipSync } from "node:zlib";

import {
  renderConnection,
  reservedPort,
  servePage,
  type ServedPage,
  staticCoding,
} from "./serve-e2e.ts";

/** One raw response: headers and the bytes as sent, nothing decoded. */
function get(
  port: number,
  path: string,
  acceptEncoding: string | null,
): Promise<{
  status: number;
  headers: Record<string, string | string[] | undefined>;
  body: Buffer;
}> {
  return new Promise((done, fail) => {
    const outgoing = request(
      {
        host: "127.0.0.1",
        port,
        path,
        headers:
          acceptEncoding === null ? {} : { "accept-encoding": acceptEncoding },
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk: Buffer) => chunks.push(chunk));
        response.on("end", () =>
          done({
            status: response.statusCode ?? 0,
            headers: response.headers,
            body: Buffer.concat(chunks),
          }),
        );
      },
    );
    outgoing.on("error", fail);
    outgoing.end();
  });
}

/** Each `Accept-Encoding` the harness relies on, and the coding it gets. */
const NEGOTIATED: readonly [string | null, "br" | "gzip" | null][] = [
  ["gzip, deflate, br, zstd", "br"],
  ["gzip", "gzip"],
  ["br;q=0, gzip", "gzip"],
  // Parameter names are case-insensitive. negotiator 1.0.0 reads only a
  // lowercase q and would send brotli here, which is why it is 1.1.0.
  ["br;Q=0, gzip", "gzip"],
  ["br;q=0.5, gzip", "gzip"],
  ["gzip;q=0.8, br;q=0.8", "br"],
  ["*", "br"],
  ["identity", null],
  ["", null],
  [null, null],
];

test("the page server answers each Accept-Encoding in its coding, and every body decodes to the stored file", async () => {
  const root = mkdtempSync(join(tmpdir(), "serve-page-"));
  const script = `export const rows = ${JSON.stringify(Array.from({ length: 500 }, (_, n) => `row ${n}`))};\n`;
  const html = `<!doctype html><script type="module" src="/assets/main-0a1b2c3d.js"></script>${" ".repeat(400)}\n`;
  const picture = Buffer.from([0x89, 0x50, 0x4e, 0x47, 1, 2, 3, 4]);
  const module = Buffer.concat([
    Buffer.from("\0asm\u0001\0\0\0"),
    Buffer.alloc(2_000, 0x41),
  ]);
  mkdirSync(join(root, "assets"));
  writeFileSync(join(root, "index.html"), html);
  writeFileSync(join(root, "assets", "main-0a1b2c3d.js"), script);
  writeFileSync(join(root, "assets", "logo-0a1b2c3d.png"), picture);
  writeFileSync(join(root, "assets", "decoder-0a1b2c3d.wasm"), module);
  const port = await reservedPort();
  const server = servePage(port, root);
  try {
    for (const [field, coding] of NEGOTIATED) {
      const sent = await get(port, "/assets/main-0a1b2c3d.js", field);
      const label = JSON.stringify(field);
      assert.equal(sent.status, 200, label);
      assert.equal(
        sent.headers["content-encoding"],
        coding ?? undefined,
        label,
      );
      assert.equal(sent.headers.vary, "Accept-Encoding", label);
      assert.equal(
        sent.headers["content-length"],
        String(sent.body.byteLength),
        label,
      );
      const decoded =
        coding === "br"
          ? brotliDecompressSync(sent.body)
          : coding === "gzip"
            ? gunzipSync(sent.body)
            : sent.body;
      assert.equal(decoded.toString(), script, label);
      if (coding !== null)
        assert.ok(sent.body.byteLength < Buffer.byteLength(script), label);
    }

    const page = await get(port, "/", "br");
    assert.equal(page.headers["content-type"], "text/html; charset=utf-8");
    assert.equal(brotliDecompressSync(page.body).toString(), html);

    const binary = await get(port, "/assets/logo-0a1b2c3d.png", "br");
    assert.equal(binary.headers["content-encoding"], undefined);
    assert.equal(binary.headers.vary, undefined);
    assert.deepEqual(binary.body, picture);

    // A WebAssembly module goes under the type `instantiateStreaming` asks
    // for, precompressed as a script is.
    const compiled = await get(port, "/assets/decoder-0a1b2c3d.wasm", "br");
    assert.equal(compiled.headers["content-type"], "application/wasm");
    assert.equal(compiled.headers["content-encoding"], "br");
    assert.equal(compiled.headers.vary, "Accept-Encoding");
    assert.ok(compiled.body.byteLength < module.byteLength);
    assert.deepEqual(brotliDecompressSync(compiled.body), module);
    const stored = await get(port, "/assets/decoder-0a1b2c3d.wasm", "identity");
    assert.equal(stored.headers["content-type"], "application/wasm");
    assert.equal(stored.headers["content-encoding"], undefined);
    assert.deepEqual(stored.body, module);

    assert.equal((await get(port, "/../index.html", "br")).status, 200);
    assert.equal((await get(port, "/missing.js", "br")).status, 404);
  } finally {
    await server.close();
    rmSync(root, { recursive: true, force: true });
  }
});

test("staticCoding picks by the request's weights, brotli before gzip at equal weight", () => {
  for (const [field, coding] of NEGOTIATED)
    assert.equal(
      staticCoding(field ?? undefined),
      coding,
      JSON.stringify(field),
    );
});

/** The connection a page's `calque-connection` element holds, or null. */
function renderedElement(html: string): unknown {
  const match =
    /<script type="application\/json" id="calque-connection">(.*?)<\/script>/su.exec(
      html,
    );
  return match?.[1] === undefined ? null : JSON.parse(match[1]);
}

test("the page server renders the connection into the page, renders a head per request after it, and sends that page no-store", async () => {
  const root = mkdtempSync(join(tmpdir(), "serve-page-"));
  mkdirSync(join(root, "assets"));
  writeFileSync(
    join(root, "index.html"),
    '<!doctype html><html><head><meta charset="UTF-8" /><title>page</title></head><body><script type="module" src="/assets/main.js"></script></body></html>\n',
  );
  writeFileSync(
    join(root, "assets", "main.js"),
    'export const page = "page";\n',
  );
  const connection = {
    endpoint: "http://127.0.0.1:55602/",
    bearerToken: "grant</script><script>",
  };
  let heads = 0;
  const codings: (string | undefined)[] = [];
  const head = async (acceptEncoding: string | undefined) => {
    heads += 1;
    codings.push(acceptEncoding);
    await new Promise((settled) => setTimeout(settled, 20));
    return `<link rel="preload" href="/plan-${String(heads)}">`;
  };
  let served: ServedPage | null = { connection, head: null };
  const port = await reservedPort();
  const server = servePage(port, {
    root,
    current: () => {
      if (served === null) throw new Error("no control");
      return served;
    },
  });
  try {
    for (const path of ["/", "/index.html"]) {
      const page = await get(port, path, "br");
      const html = brotliDecompressSync(page.body).toString();
      assert.equal(page.headers["cache-control"], "no-store", path);
      assert.equal(page.headers["content-encoding"], "br", path);
      assert.equal(page.headers.vary, "Accept-Encoding", path);
      assert.deepEqual(renderedElement(html), connection, path);
      assert.equal(html.split("calque-connection").length, 2, path);
      // The bearer's `</script>` cannot close the element.
      assert.equal(html.split("</script>").length, 3, path);
      assert.ok(
        html.indexOf("calque-connection") < html.indexOf("</head>"),
        path,
      );
      assert.ok(!html.includes("preload"), path);
    }
    const plain = await get(port, "/", null);
    assert.deepEqual(
      renderedElement(plain.body.toString()),
      connection,
      "identity",
    );
    const script = await get(port, "/assets/main.js", null);
    assert.equal(script.body.toString(), 'export const page = "page";\n');
    assert.equal(script.headers["cache-control"], undefined);

    served = { connection, head };
    for (const [n, coding] of [
      [1, "br"],
      [2, "gzip"],
      [3, null],
    ] as const) {
      const page = await get(port, "/", coding);
      const html = (
        coding === "br"
          ? brotliDecompressSync(page.body)
          : coding === "gzip"
            ? gunzipSync(page.body)
            : page.body
      ).toString();
      const label = String(coding);
      assert.equal(page.headers["cache-control"], "no-store", label);
      assert.equal(page.headers["content-encoding"], coding ?? undefined);
      assert.equal(page.headers.vary, "Accept-Encoding", label);
      assert.deepEqual(renderedElement(html), connection, label);
      // Each request renders its own head, after the connection.
      const link = `<link rel="preload" href="/plan-${String(n)}">`;
      assert.equal(html.split(link).length, 2, label);
      assert.ok(html.indexOf("calque-connection") < html.indexOf(link));
      assert.ok(html.indexOf(link) < html.indexOf("</head>"));
    }
    // Each head was given its own request's Accept-Encoding.
    assert.deepEqual(codings, ["br", "gzip", undefined]);
    // A script is not the page, and renders no head.
    await get(port, "/assets/main.js", "br");
    assert.equal(heads, 3);

    served = {
      connection,
      head: () => Promise.reject(new Error("the backend failed")),
    };
    assert.equal((await get(port, "/", "br")).status, 500);

    served = { connection: null, head };
    const stored = await get(port, "/", null);
    assert.equal(renderedElement(stored.body.toString()), null);
    assert.equal(stored.headers["cache-control"], undefined);
    assert.equal(heads, 3);

    served = null;
    assert.equal((await get(port, "/", "br")).status, 500);
  } finally {
    await server.close();
    rmSync(root, { recursive: true, force: true });
  }
});

test("a page is rendered only where it has one head and no calque-connection element", () => {
  const connection = { endpoint: "https://sync.test/", bearerToken: "grant" };
  assert.equal(
    renderConnection("<head><title>t</title></head><body></body>", connection),
    '<head><title>t</title><script type="application/json" id="calque-connection">{"endpoint":"https://sync.test/","bearerToken":"grant"}</script></head><body></body>',
  );
  // The inline starter's own text names the element, and is left alone.
  assert.ok(
    renderConnection(
      '<head><script type="module">rendered("calque-connection")</script></head>',
      connection,
    ).includes('id="calque-connection"'),
  );
  for (const html of [
    "<body></body>",
    "<head></head><head></head>",
    '<head><script type="application/json" id="calque-connection">{}</script></head>',
  ])
    assert.throws(() => renderConnection(html, connection), /exactly one/u);
});

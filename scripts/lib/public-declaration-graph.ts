import { createHash } from "node:crypto";
import { dirname, relative, resolve, sep } from "node:path";

// The compiler API is injected so this module walks the declarations with the
// TypeScript that emitted them, the client package's: the root's 7.x exports
// no JavaScript compiler API.
import type * as TypeScriptApi from "../../client/packages/client/node_modules/typescript";

type TypeScriptCompilerApi = typeof TypeScriptApi;

/**
 * A directory whose declarations count as owned, and the prefix its files are
 * reported under. Declarations outside every root are skipped.
 */
interface DeclarationRoot {
  readonly root: string;
  readonly prefix: string;
}

/** One declaration reached from the entry module, in its reporting form. */
interface DeclarationRecord {
  readonly path: string;
  readonly symbol: string;
  readonly text: string;
}

export interface PublicDeclarationGraph {
  readonly records: readonly DeclarationRecord[];
  readonly sha256: string;
}

interface PublicDeclarationGraphRequest {
  readonly ts: TypeScriptCompilerApi;
  readonly entryPath: string;
  readonly declarationRoots: readonly DeclarationRoot[];
  readonly label: string;
}

interface AssertPublicDeclarationGraphRequest extends PublicDeclarationGraphRequest {
  readonly expectedSha256: string;
}

const PROHIBITED_MECHANISMS =
  /SyncWorker|GeneratedClientSchemaDescriptor|WorkerInitialization|schemaGeneration|createSql|\bDDL\b|progress[- ]handler|canonical cursor|canonicalCursor|transport bytes|SQLite writer|artifactmemorytrace|runtimememorysnapshot|residen|measurement|EngineWorkerClient|ColumnarMountState|collectionGenerations|QuerySpecification/iu;

const FORBIDDEN_DECLARATION_FILES =
  /(?:generated-schema|worker-contract|client-options|client-heap|diagnostics)\.d\.ts$/u;

export function assertPublicDeclarationGraph({
  ts,
  entryPath,
  declarationRoots,
  expectedSha256,
  label,
}: AssertPublicDeclarationGraphRequest): PublicDeclarationGraph {
  const graph = collectPublicDeclarationGraph({
    ts,
    entryPath,
    declarationRoots,
    label,
  });
  if (graph.sha256 !== expectedSha256) {
    throw new Error(
      `${label} declaration graph drifted:\nactual=${graph.sha256}\nexpected=${expectedSha256}\nsymbols=${graph.records
        .map((record) => `${record.path}:${record.symbol}`)
        .join(",")}`,
    );
  }
  const leaked = graph.records.filter((record) =>
    PROHIBITED_MECHANISMS.test(record.text),
  );
  if (leaked.length > 0) {
    throw new Error(
      `${label} declaration graph leaks mechanisms: ${leaked
        .map((record) => `${record.path}:${record.symbol}`)
        .join(", ")}`,
    );
  }
  const forbiddenFiles = graph.records.filter((record) =>
    FORBIDDEN_DECLARATION_FILES.test(record.path),
  );
  if (forbiddenFiles.length > 0) {
    throw new Error(
      `${label} declarations reach internal ownership modules: ${forbiddenFiles
        .map((record) => record.path)
        .join(", ")}`,
    );
  }
  return graph;
}

export function collectPublicDeclarationGraph({
  ts,
  entryPath,
  declarationRoots,
  label,
}: PublicDeclarationGraphRequest): PublicDeclarationGraph {
  const roots = declarationRoots.map(({ root, prefix }) => ({
    absolute: resolve(root),
    prefix,
  }));
  const program = ts.createProgram({
    rootNames: [entryPath],
    options: {
      target: ts.ScriptTarget.ESNext,
      module: ts.ModuleKind.ESNext,
      moduleResolution: ts.ModuleResolutionKind.Bundler,
      strict: true,
      skipLibCheck: false,
    },
  });
  const diagnostics = ts.getPreEmitDiagnostics(program);
  if (diagnostics.length > 0) {
    throw new Error(
      `${label} declaration graph has TypeScript diagnostics:\n${ts.formatDiagnosticsWithColorAndContext(
        diagnostics,
        {
          getCanonicalFileName: (path: string) => path,
          getCurrentDirectory: () => dirname(entryPath),
          getNewLine: () => "\n",
        },
      )}`,
    );
  }
  const checker = program.getTypeChecker();
  const entry = program.getSourceFile(entryPath);
  const module = entry && checker.getSymbolAtLocation(entry);
  if (!entry || !module) {
    throw new Error(`${label} declaration module is missing`);
  }
  const visited = new Set<TypeScriptApi.Symbol>();
  const records: DeclarationRecord[] = [];
  const visitSymbol = (candidate: TypeScriptApi.Symbol): void => {
    const symbol =
      candidate.flags & ts.SymbolFlags.Alias
        ? checker.getAliasedSymbol(candidate)
        : candidate;
    if (visited.has(symbol)) return;
    visited.add(symbol);
    for (const declaration of symbol.declarations ?? []) {
      const file = declaration.getSourceFile();
      const absolute = resolve(file.fileName);
      const owner = roots.find(
        ({ absolute: root }) =>
          absolute === root || absolute.startsWith(`${root}${sep}`),
      );
      if (!owner) continue;
      const path = `${owner.prefix}/${relative(owner.absolute, absolute)
        .split(sep)
        .join("/")}`;
      // `getText` skips the leading trivia where a declaration's own doc
      // comment sits, so the comment is prepended for the digest to pin it.
      const docs = ts
        .getJSDocCommentsAndTags(declaration)
        .filter((node) => ts.isJSDoc(node))
        .map((node) => node.getText(file));
      const text = [...docs, declaration.getText(file)]
        .join(" ")
        .replaceAll(/\s+/gu, " ")
        .trim();
      records.push({ path, symbol: symbol.getName(), text });
      const visitNode = (node: TypeScriptApi.Node): void => {
        if (ts.isIdentifier(node)) {
          const referenced = checker.getSymbolAtLocation(node);
          if (referenced && referenced !== symbol) visitSymbol(referenced);
        }
        ts.forEachChild(node, visitNode);
      };
      ts.forEachChild(declaration, visitNode);
    }
  };
  for (const exportedSymbol of checker.getExportsOfModule(module)) {
    visitSymbol(exportedSymbol);
  }
  records.sort((left, right) =>
    `${left.path}\u0000${left.symbol}\u0000${left.text}`.localeCompare(
      `${right.path}\u0000${right.symbol}\u0000${right.text}`,
    ),
  );
  const unique = records.filter(
    (record, index) =>
      index === 0 ||
      JSON.stringify(record) !== JSON.stringify(records[index - 1]),
  );
  return Object.freeze({
    records: Object.freeze(unique.map((record) => Object.freeze(record))),
    sha256: createHash("sha256").update(JSON.stringify(unique)).digest("hex"),
  });
}

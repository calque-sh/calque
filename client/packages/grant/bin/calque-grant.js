#!/usr/bin/env node
import process from "node:process";
import { run } from "../dist/cli.js";

process.exitCode = run(process.argv.slice(2), {
  stdout: process.stdout,
  stderr: process.stderr,
  nowUnixSeconds: () => Math.floor(Date.now() / 1000),
});

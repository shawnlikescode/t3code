// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeFSP from "node:fs/promises";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/unstable/process";
import { describe, expect } from "vite-plus/test";

import { KiloSettings } from "@t3tools/contracts";

import {
  buildInitialKiloProviderSnapshot,
  checkKiloProviderStatus,
  parseKiloModelsOutput,
} from "./KiloProvider.ts";

const decodeKiloSettings = Schema.decodeSync(KiloSettings);

async function makeMockKiloWrapper(extraEnv?: Record<string, string>) {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-provider-mock-"));
  const wrapperPath = NodePath.join(dir, "fake-kilo.sh");
  const envExports = Object.entries(extraEnv ?? {})
    .map(([key, value]) => `export ${key}=${JSON.stringify(value)}`)
    .join("\n");
  const mockVersion = extraEnv?.T3_KILO_MOCK_VERSION ?? "7.4.23";
  const script = `#!/bin/sh
${envExports}
if [ "$1" = "--version" ]; then
  echo ${JSON.stringify(`kilo ${mockVersion}`)}
  exit 0
fi
if [ "$1" = "models" ]; then
  if [ -n "$T3_KILO_MODELS_ENV_LOG" ]; then
    printf "%s|%s|%s|%s|%s\n" "$KILO_PURE" "$KILO_DISABLE_PROJECT_CONFIG" "$KILO_DISABLE_EXTERNAL_SKILLS" "$KILO_DISABLE_SKILL_SHELL" "$*" >> "$T3_KILO_MODELS_ENV_LOG"
  fi
  if [ "$T3_KILO_MODELS_MODE" = "nonzero" ]; then
    echo "provider/model" >&2
    exit 17
  fi
  if [ "$T3_KILO_MODELS_MODE" = "empty" ]; then
    exit 0
  fi
  if [ "$T3_KILO_MODELS_MODE" = "malformed" ]; then
    printf "/model\nprovider/\n"
    exit 0
  fi
  printf "kilo/openrouter/free\nopenai/gpt-5\nopenai/gpt-5\n"
  exit 0
fi
if [ -n "$T3_KILO_UNEXPECTED_COMMAND_LOG" ]; then
  printf "%s\n" "$*" >> "$T3_KILO_UNEXPECTED_COMMAND_LOG"
fi
exit 91
`;
  await NodeFSP.writeFile(wrapperPath, script, "utf8");
  await NodeFSP.chmod(wrapperPath, 0o755);
  return wrapperPath;
}

describe("KiloProvider", () => {
  it.effect("builds a disabled initial snapshot", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialKiloProviderSnapshot(decodeKiloSettings({}));
      expect(snapshot.displayName).toBe("Kilo Code");
      expect(snapshot.badgeLabel).toBe("Early Access");
      expect(snapshot.supportsTextGeneration).toBe(false);
      expect(snapshot.enabled).toBe(false);
      expect(snapshot.status).toBe("disabled");
    }),
  );

  it.effect("builds a checking initial snapshot when enabled", () =>
    Effect.gen(function* () {
      const snapshot = yield* buildInitialKiloProviderSnapshot(
        decodeKiloSettings({ enabled: true }),
      );
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("warning");
      expect(snapshot.message).toContain("Checking Kilo CLI availability");
    }),
  );

  it.effect("reports an error when the CLI is missing", () =>
    checkKiloProviderStatus(
      decodeKiloSettings({
        enabled: true,
        binaryPath: NodePath.join(NodeOS.tmpdir(), "kilo-does-not-exist"),
      }),
    ).pipe(
      Effect.provide(NodeServices.layer),
      Effect.map((snapshot) => {
        expect(snapshot.installed).toBe(false);
        expect(snapshot.status).toBe("error");
        expect(snapshot.message).toContain("npm install -g @kilocode/cli");
      }),
    ),
  );

  it.effect("rejects Kilo versions older than the verified minimum", () =>
    Effect.gen(function* () {
      const binaryPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({ T3_KILO_MOCK_VERSION: "7.3.16" }),
      );
      const snapshot = yield* checkKiloProviderStatus(
        decodeKiloSettings({ enabled: true, binaryPath }),
      ).pipe(Effect.provide(NodeServices.layer));

      expect(snapshot.installed).toBe(true);
      expect(snapshot.version).toBe("7.3.16");
      expect(snapshot.status).toBe("error");
      expect(snapshot.models).toHaveLength(0);
      expect(snapshot.message).toContain("Upgrade to v7.4.23 or newer");
      expect(snapshot.message).toContain("kilo upgrade");
    }),
  );

  it.effect("rejects prereleases of the minimum version instead of rounding them up", () =>
    Effect.gen(function* () {
      const binaryPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({ T3_KILO_MOCK_VERSION: "7.4.23-beta.1" }),
      );
      const snapshot = yield* checkKiloProviderStatus(
        decodeKiloSettings({ enabled: true, binaryPath }),
      ).pipe(Effect.provide(NodeServices.layer));

      expect(snapshot.version).toBe("7.4.23-beta.1");
      expect(snapshot.status).toBe("error");
      expect(snapshot.message).toContain("Upgrade to v7.4.23 or newer");
    }),
  );

  it.effect("discovers newline-delimited models without creating an ACP session", () =>
    Effect.gen(function* () {
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-provider-models-")),
      );
      const environmentLogPath = NodePath.join(tempDir, "models-env.txt");
      const unexpectedCommandLogPath = NodePath.join(tempDir, "unexpected-command.txt");
      const binaryPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_KILO_MODELS_ENV_LOG: environmentLogPath,
          T3_KILO_UNEXPECTED_COMMAND_LOG: unexpectedCommandLogPath,
        }),
      );
      const snapshot = yield* checkKiloProviderStatus(
        decodeKiloSettings({ enabled: true, binaryPath }),
      ).pipe(Effect.provide(NodeServices.layer));
      expect(snapshot.version).toBe("7.4.23");
      expect(snapshot.status).toBe("ready");

      const slugs = snapshot.models.map((model) => model.slug);
      expect(slugs).toContain("kilo/openrouter/free");
      expect(slugs).toContain("openai/gpt-5");
      expect(slugs.filter((slug) => slug === "openai/gpt-5")).toHaveLength(1);
      const defaults = snapshot.models.filter((model) => model.isDefault === true);
      expect(defaults).toHaveLength(1);
      expect(defaults[0]?.name).toBe("Kilo provider default");
      expect(
        (yield* Effect.promise(() => NodeFSP.readFile(environmentLogPath, "utf8"))).trim(),
      ).toBe("1|1|1|1|models");
      expect(
        yield* Effect.promise(() =>
          NodeFSP.stat(unexpectedCommandLogPath).then(
            () => true,
            () => false,
          ),
        ),
      ).toBe(false);
    }),
  );

  it.effect("uses the command catalog as authoritative and ignores legacy custom models", () =>
    Effect.gen(function* () {
      const binaryPath = yield* Effect.promise(() => makeMockKiloWrapper());
      // Stale persisted blobs may still carry a customModels key; the schema
      // has no such field, so decoding must drop it and the catalog stays
      // command-backed.
      const snapshot = yield* checkKiloProviderStatus(
        decodeKiloSettings({
          enabled: true,
          binaryPath,
          customModels: ["kilo/custom/my-model"],
        } as Record<string, unknown>),
      ).pipe(Effect.provide(NodeServices.layer));
      expect(snapshot.hasAuthoritativeModelCatalog).toBe(true);
      const slugs = snapshot.models.map((model) => model.slug);
      expect(slugs).toContain("kilo/openrouter/free");
      expect(slugs).not.toContain("kilo/custom/my-model");
      expect(snapshot.models.every((model) => model.isCustom === false)).toBe(true);
    }),
  );

  it.each([
    ["", "empty model catalog"],
    ["/", "malformed model ID"],
    ["/model", "malformed model ID"],
    ["provider/", "malformed model ID"],
    ["provider/model with-space", "malformed model ID"],
  ])("rejects unsafe model command output %j", (stdout, expectedMessage) => {
    const result = parseKiloModelsOutput(stdout);
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.message).toContain(expectedMessage);
  });

  it.effect("reports non-zero, empty, and malformed model command results", () =>
    Effect.gen(function* () {
      for (const [mode, expected] of [
        ["nonzero", "exited unsuccessfully"],
        ["empty", "empty model catalog"],
        ["malformed", "malformed model ID"],
      ] as const) {
        const binaryPath = yield* Effect.promise(() =>
          makeMockKiloWrapper({ T3_KILO_MODELS_MODE: mode }),
        );
        const snapshot = yield* checkKiloProviderStatus(
          decodeKiloSettings({ enabled: true, binaryPath }),
        ).pipe(Effect.provide(NodeServices.layer));
        expect(snapshot.status).toBe("error");
        expect(snapshot.models).toEqual([]);
        expect(snapshot.message).toContain(expected);
      }
    }),
  );

  it.effect("times out a stuck model command without waiting on wall-clock time", () =>
    Effect.gen(function* () {
      const spawnCount = yield* Ref.make(0);
      const modelsSpawned = yield* Deferred.make<void>();
      const spawner = ChildProcessSpawner.make(() =>
        Ref.getAndUpdate(spawnCount, (count) => count + 1).pipe(
          Effect.flatMap((count) => {
            if (count === 0) {
              return Effect.succeed(
                ChildProcessSpawner.makeHandle({
                  pid: ChildProcessSpawner.ProcessId(1),
                  exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
                  isRunning: Effect.succeed(false),
                  kill: () => Effect.void,
                  unref: Effect.succeed(Effect.void),
                  stdin: Sink.drain,
                  stdout: Stream.encodeText(Stream.make("kilo 7.4.23")),
                  stderr: Stream.empty,
                  all: Stream.empty,
                  getInputFd: () => Sink.drain,
                  getOutputFd: () => Stream.empty,
                }),
              );
            }
            const stuck = ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(2),
              exitCode: Effect.never,
              isRunning: Effect.succeed(true),
              kill: () => Effect.void,
              unref: Effect.succeed(Effect.void),
              stdin: Sink.drain,
              stdout: Stream.never,
              stderr: Stream.never,
              all: Stream.never,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
            });
            return Deferred.succeed(modelsSpawned, undefined).pipe(Effect.as(stuck));
          }),
        ),
      );
      const fiber = yield* checkKiloProviderStatus(
        decodeKiloSettings({ enabled: true, binaryPath: "kilo" }),
      ).pipe(
        Effect.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner)),
        Effect.forkChild,
      );
      yield* Deferred.await(modelsSpawned);
      yield* TestClock.adjust("15 seconds");
      const snapshot = yield* Fiber.join(fiber);
      expect(snapshot.status).toBe("error");
      expect(snapshot.message).toContain("timed out");
    }),
  );
});

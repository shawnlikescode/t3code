// @effect-diagnostics nodeBuiltinImport:off
/**
 * Optional integration check against a real `kilo acp` install.
 * Enable only with isolated XDG config/data/cache/state directories plus
 * T3_KILO_ACP_PROBE=1. The probe refuses flag-only runs because Kilo persists
 * every ACP-created session in its own history database.
 *
 * Startup/model checks do not require credentials. Set the separate
 * `T3_KILO_ACP_PROMPT_PROBE=1` flag to exercise a prompt against Kilo's
 * advertised free OpenRouter model from an isolated XDG home.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import { ChildProcessSpawner } from "effect/unstable/process";
import { describe, expect } from "vite-plus/test";

import {
  applyKiloAcpModelSelection,
  currentKiloModelIdFromSessionSetup,
  hardenKiloProbeEnvironment,
  kiloModelsFromSessionConfigOptions,
  startKiloAcpRuntime,
} from "./KiloAcpSupport.ts";

const hasExplicitIsolatedXdg = [
  "XDG_CONFIG_HOME",
  "XDG_DATA_HOME",
  "XDG_CACHE_HOME",
  "XDG_STATE_HOME",
].every((name) => {
  const value = process.env[name]?.trim();
  return (
    value !== undefined && !NodePath.resolve(value).startsWith(`${NodeOS.homedir()}${NodePath.sep}`)
  );
});

const startProbeRuntime = <E = never, R = never>(
  configureRuntime?: Parameters<typeof startKiloAcpRuntime<E, R>>[1],
) =>
  Effect.gen(function* () {
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    return yield* startKiloAcpRuntime(
      {
        kiloSettings: { binaryPath: "kilo" },
        environment: hardenKiloProbeEnvironment(process.env),
        childProcessSpawner,
        cwd: process.cwd(),
        clientInfo: { name: "t3-kilo-probe", version: "0.0.0" },
        requestLogger: (event) =>
          Effect.logInfo("Kilo ACP probe request", {
            method: event.method,
            status: event.status,
          }),
      },
      configureRuntime,
    );
  });

describe.runIf(process.env.T3_KILO_ACP_PROBE === "1" && hasExplicitIsolatedXdg)(
  "Kilo ACP CLI probe",
  () => {
    it.effect("initialize, authenticate, and create a session against real kilo acp", () =>
      Effect.gen(function* () {
        const { started } = yield* startProbeRuntime();
        expect(started.initializeResult).toBeDefined();
        expect(typeof started.sessionId).toBe("string");
        // kilo-login is a no-op authenticate; sessions boot without credentials.
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );

    it.effect("session/new advertises a model select in configOptions", () =>
      Effect.gen(function* () {
        const { started } = yield* startProbeRuntime();

        const models = kiloModelsFromSessionConfigOptions(started.sessionSetupResult);
        expect(models.length).toBeGreaterThan(0);

        const current = currentKiloModelIdFromSessionSetup(started.sessionSetupResult);
        expect(current).toBeDefined();
        if (current === undefined) return;
        expect(models.some((model) => model.slug === current)).toBe(true);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );

    it.effect("no-op model selection succeeds against the live catalog", () =>
      Effect.gen(function* () {
        const { runtime, started } = yield* startProbeRuntime();

        // Selecting the model the session already runs on must resolve without
        // issuing a session/set_config_option round-trip against every Kilo
        // build that implements config-option selects.
        const selected = yield* applyKiloAcpModelSelection({
          runtime,
          requestedModelId: currentKiloModelIdFromSessionSetup(started.sessionSetupResult),
          mapError: (cause) => cause,
        });
        expect(selected).toBeDefined();
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );

    it.effect("starts a second ACP process while the first remains active", () =>
      Effect.gen(function* () {
        yield* startProbeRuntime();

        yield* startProbeRuntime();
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );

    it.effect("serializes overlapping ACP process startup", () =>
      Effect.all(
        [
          Effect.gen(function* () {
            yield* startProbeRuntime();
          }),
          Effect.gen(function* () {
            yield* startProbeRuntime();
          }),
        ],
        { concurrency: "unbounded", discard: true },
      ).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );

    it.effect.runIf(process.env.T3_KILO_ACP_PROMPT_PROBE === "1")(
      "completes a prompt through the unauthenticated free model",
      () =>
        Effect.gen(function* () {
          const outputRef = yield* Ref.make("");
          const { runtime, started } = yield* startProbeRuntime((runtime) =>
            runtime.handleSessionUpdate((notification) => {
              const update = notification.update;
              if (
                update.sessionUpdate !== "agent_message_chunk" ||
                update.content.type !== "text"
              ) {
                return Effect.void;
              }
              const text = update.content.text;
              return Ref.update(outputRef, (current) => current + text);
            }),
          );
          const freeModel = kiloModelsFromSessionConfigOptions(started.sessionSetupResult).find(
            (model) => model.slug === "kilo/openrouter/free",
          );
          expect(freeModel).toBeDefined();
          if (!freeModel) return;

          yield* applyKiloAcpModelSelection({
            runtime,
            requestedModelId: freeModel.slug,
            mapError: (cause) => cause,
          });
          const promptResult = yield* runtime
            .prompt({
              prompt: [{ type: "text", text: "Reply with the single word PONG." }],
            })
            .pipe(Effect.timeoutOption("30 seconds"));
          expect(Option.isSome(promptResult)).toBe(true);
          expect((yield* Ref.get(outputRef)).trim().length).toBeGreaterThan(0);
        }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
      40_000,
    );
  },
);

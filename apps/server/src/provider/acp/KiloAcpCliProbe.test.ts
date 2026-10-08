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
import * as NodeHttp from "node:http";
import * as NodeFSP from "node:fs/promises";
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
  buildKiloChildAgentPolicyEnvironment,
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
    it.effect(
      "full-access child reads an env schema without an invisible approval",
      () =>
        Effect.gen(function* () {
          const cwd = yield* Effect.promise(() =>
            NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-kilo-child-read-")),
          );
          yield* Effect.promise(() =>
            NodeFSP.writeFile(NodePath.join(cwd, ".env.schema"), "FIXTURE_ONLY=child-read-ok\n"),
          );
          let requests = 0;
          let childRead = false;
          const server = NodeHttp.createServer(async (request, response) => {
            const chunks: Buffer[] = [];
            for await (const chunk of request) chunks.push(Buffer.from(chunk));
            const body = JSON.parse(Buffer.concat(chunks).toString()) as {
              messages: Array<{ role: string; content?: string }>;
              tools?: Array<{ function: { name: string } }>;
            };
            requests += 1;
            const child = !(body.tools ?? []).some((tool) => tool.function.name === "task");
            const result = body.messages.find((message) => message.role === "tool");
            if (child && result?.content?.includes("child-read-ok")) childRead = true;
            const tool = child ? "read" : "task";
            const args = child
              ? { filePath: NodePath.join(cwd, ".env.schema") }
              : {
                  description: "Read fixture",
                  prompt: "Read .env.schema",
                  subagent_type: "general",
                };
            const delta = result
              ? { content: child ? "CHILD_DONE" : "PARENT_DONE" }
              : {
                  tool_calls: [
                    {
                      index: 0,
                      id: `call_${requests}`,
                      type: "function",
                      function: { name: tool, arguments: JSON.stringify(args) },
                    },
                  ],
                };
            response.writeHead(200, { "Content-Type": "text/event-stream" });
            for (const choice of [
              { delta: { role: "assistant" }, finish_reason: null },
              { delta, finish_reason: null },
              { delta: {}, finish_reason: result ? "stop" : "tool_calls" },
            ])
              response.write(
                `data: ${JSON.stringify({
                  id: "fixture",
                  object: "chat.completion.chunk",
                  created: 1,
                  model: "fixture",
                  choices: [{ index: 0, ...choice }],
                })}\n\n`,
              );
            response.end("data: [DONE]\n\n");
          });
          yield* Effect.promise(
            () => new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve)),
          );
          try {
            const address = server.address();
            if (!address || typeof address === "string") throw new Error("No fixture port");
            const policy = buildKiloChildAgentPolicyEnvironment({
              environment: {
                ...hardenKiloProbeEnvironment(process.env),
                KILO_CONFIG_CONTENT: JSON.stringify({
                  model: "fixture/model",
                  provider: {
                    fixture: {
                      npm: "@ai-sdk/openai-compatible",
                      options: {
                        baseURL: `http://127.0.0.1:${address.port}/v1`,
                        apiKey: "fixture",
                      },
                      models: {
                        model: { name: "fixture", limit: { context: 100000, output: 1000 } },
                      },
                    },
                  },
                }),
              },
              nonce: "child-read-fixture",
              policy: "full-access",
              label: "Fixture",
              prompt: "Run fixture",
            });
            if (!policy.ok) throw new Error(policy.message);
            yield* Effect.gen(function* () {
              const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
              const { runtime } = yield* startKiloAcpRuntime({
                kiloSettings: { binaryPath: "kilo" },
                environment: policy.environment,
                childProcessSpawner,
                cwd,
                clientInfo: { name: "t3-child-read-fixture", version: "1" },
              });
              yield* runtime.setMode(policy.modeId);
              const result = yield* runtime
                .prompt({ prompt: [{ type: "text", text: "Run the task fixture." }] })
                .pipe(Effect.timeoutOption("10 seconds"));
              expect(Option.isSome(result)).toBe(true);
              expect(childRead).toBe(true);
              expect(requests).toBeGreaterThanOrEqual(4);
            }).pipe(Effect.scoped);
          } finally {
            yield* Effect.promise(
              () =>
                new Promise<void>((resolve, reject) =>
                  server.close((error) => (error ? reject(error) : resolve())),
                ),
            );
            yield* Effect.promise(() => NodeFSP.rm(cwd, { recursive: true, force: true }));
          }
        }).pipe(Effect.provide(NodeServices.layer)),
      30_000,
    );

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

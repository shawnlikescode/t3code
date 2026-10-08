// @effect-diagnostics nodeBuiltinImport:off
/** Native Kilo integration against a loopback fixture; no paid model or real history. */
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeHttp from "node:http";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { describe, expect } from "vite-plus/test";
import {
  KiloSettings,
  ApprovalRequestId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { ServerConfig } from "../../config.ts";
import { makeKiloSdkAdapter } from "./KiloSdkAdapter.ts";
import {
  startKiloAcpRuntime,
  buildKiloChildAgentPolicyEnvironment,
  hardenKiloProbeEnvironment,
  KILO_PROVIDER_DEFAULT_MODEL_ID,
} from "../acp/KiloAcpSupport.ts";
const fixtureSettings = Schema.decodeSync(KiloSettings)({ binaryPath: "kilo" });
const encodeFixtureConfig = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
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
describe.runIf(process.env.T3_KILO_SDK_PROBE === "1" && hasExplicitIsolatedXdg)(
  "Kilo SDK CLI probe",
  () => {
    it.live(
      "migrates ACP history and supports native progress, models, approvals, questions, rewind, and Stop",
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
          const models: string[] = [];
          let signalWaitingStarted = () => {};
          const waitingStarted = new Promise<void>((resolve) => {
            signalWaitingStarted = resolve;
          });
          const server = NodeHttp.createServer(async (request, response) => {
            const chunks: Buffer[] = [];
            for await (const chunk of request) chunks.push(Buffer.from(chunk));
            const body = JSON.parse(Buffer.concat(chunks).toString()) as {
              model: string;
              messages: Array<{ role: string; content?: string }>;
              tools?: Array<{ function: { name: string } }>;
            };
            requests += 1;
            models.push(body.model);
            const child = !(body.tools ?? []).some((tool) => tool.function.name === "task");
            const result = body.messages.find((message) => message.role === "tool");
            if (child && result?.content?.includes("child-read-ok")) childRead = true;
            if (body.model === "waiting") {
              response.writeHead(200, { "Content-Type": "text/event-stream" });
              response.write(
                `data: ${JSON.stringify({ id: "waiting", object: "chat.completion.chunk", choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] })}\n\n`,
              );
              signalWaitingStarted();
              return;
            }
            const interactive = body.model === "interactive";
            const toolResults = body.messages.filter((message) => message.role === "tool");
            const interactiveDone = interactive && toolResults.length >= 2;
            const tool = interactive
              ? toolResults.length
                ? "question"
                : "bash"
              : child
                ? "read"
                : "task";
            const args = interactive
              ? toolResults.length
                ? {
                    questions: [
                      {
                        header: "Scope",
                        question: "Choose the fixture scope",
                        options: [{ label: "Workspace", description: "Use this workspace" }],
                      },
                    ],
                  }
                : { command: "pwd", description: "Print fixture path" }
              : child
                ? { filePath: NodePath.join(cwd, ".env.schema") }
                : {
                    description: "Read fixture",
                    prompt: "Read .env.schema",
                    subagent_type: "general",
                  };
            const legacySeed = JSON.stringify(
              body.messages.findLast((message) => message.role === "user")?.content,
            ).includes("Seed legacy");
            const done = legacySeed || (interactive ? interactiveDone : Boolean(result));
            const delta = done
              ? {
                  content: legacySeed
                    ? "LEGACY_DONE"
                    : interactive
                      ? "INTERACTIVE_DONE"
                      : child
                        ? "CHILD_DONE"
                        : "PARENT_DONE",
                }
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
              { delta: {}, finish_reason: done ? "stop" : "tool_calls" },
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
            const environment = {
              ...hardenKiloProbeEnvironment(process.env),
              KILO_CONFIG_CONTENT: encodeFixtureConfig({
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
                      switched: { name: "switched", limit: { context: 100000, output: 1000 } },
                      interactive: {
                        name: "interactive",
                        limit: { context: 100000, output: 1000 },
                      },
                      waiting: { name: "waiting", limit: { context: 100000, output: 1000 } },
                    },
                  },
                },
              }),
            };
            const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
            const policy = buildKiloChildAgentPolicyEnvironment({
              environment,
              nonce: "migration-fixture",
              policy: "full-access",
              label: "Full access fixture",
              prompt: "Run fixture",
            });
            if (!policy.ok) throw new Error(policy.message);
            const legacy = yield* Effect.gen(function* () {
              const { started, runtime } = yield* startKiloAcpRuntime({
                kiloSettings: { binaryPath: "kilo" },
                environment: policy.environment,
                cwd,
                childProcessSpawner,
                clientInfo: { name: "t3-sdk-migration-fixture", version: "1" },
              });
              yield* runtime.setMode(policy.modeId);
              yield* runtime
                .prompt({ prompt: [{ type: "text", text: "Seed legacy history." }] })
                .pipe(Effect.timeout("15 seconds"));
              return { schemaVersion: 1, sessionId: started.sessionId };
            }).pipe(Effect.scoped);
            const adapter = yield* makeKiloSdkAdapter(fixtureSettings, { environment });
            const threadId = ThreadId.make("kilo-sdk-fixture");
            const events: ProviderRuntimeEvent[] = [];
            let completion = yield* Deferred.make<void>();
            yield* adapter.streamEvents.pipe(
              Stream.runForEach((event) =>
                Effect.gen(function* () {
                  events.push(event);
                  if (event.type === "request.opened" && event.requestId)
                    yield* adapter.respondToRequest(
                      event.threadId,
                      ApprovalRequestId.make(event.requestId),
                      "accept",
                    );
                  if (event.type === "user-input.requested" && event.requestId)
                    yield* adapter.respondToUserInput(
                      event.threadId,
                      ApprovalRequestId.make(event.requestId),
                      { Scope: "Workspace" },
                    );
                  if (event.type === "turn.completed" || event.type === "turn.aborted")
                    yield* Deferred.succeed(completion, undefined);
                }),
              ),
              Effect.forkChild,
            );
            const session = yield* adapter.startSession({
              provider: ProviderDriverKind.make("kilo"),
              threadId,
              cwd,
              runtimeMode: "full-access",
              resumeCursor: legacy,
              modelSelection: {
                instanceId: ProviderInstanceId.make("kilo"),
                model: KILO_PROVIDER_DEFAULT_MODEL_ID,
              },
            });
            expect(session.resumeCursor).toEqual(legacy);
            yield* adapter.sendTurn({ threadId, input: "Run the task fixture." });
            yield* Deferred.await(completion).pipe(Effect.timeout("15 seconds"));
            expect(events.find((event) => event.type === "turn.completed")?.payload).toMatchObject({
              state: "completed",
            });
            expect(childRead).toBe(true);
            expect(events.some((event) => event.type === "task.started")).toBe(true);
            expect(
              events.some(
                (event) => event.type === "task.progress" && event.payload.lastToolName === "read",
              ),
            ).toBe(true);
            expect(
              events.some(
                (event) =>
                  event.type === "task.progress" &&
                  event.payload.description.includes("CHILD_DONE"),
              ),
            ).toBe(true);
            expect(
              events.some(
                (event) =>
                  event.type === "content.delta" && event.payload.delta.includes("CHILD_DONE"),
              ),
            ).toBe(false);
            expect(events.some((event) => event.type === "item.completed")).toBe(true);
            expect(
              events.some(
                (event) =>
                  event.type === "content.delta" && event.payload.delta.includes("PARENT_DONE"),
              ),
            ).toBe(true);
            expect(events.every((event) => event.provider === "kilo")).toBe(true);
            expect(events.filter((event) => event.type === "turn.completed")).toHaveLength(1);
            expect(events.some((event) => event.type === "request.opened")).toBe(false);
            // ACP and SDK use the same version-1 native session cursor.
            const resumed = yield* adapter.startSession({
              provider: ProviderDriverKind.make("kilo"),
              threadId,
              cwd,
              runtimeMode: "full-access",
              resumeCursor: session.resumeCursor,
            });
            expect(resumed.resumeCursor).toEqual(session.resumeCursor);
            completion = yield* Deferred.make<void>();
            yield* adapter.sendTurn({
              threadId,
              input: "Continue.",
              modelSelection: {
                instanceId: ProviderInstanceId.make("kilo"),
                model: "fixture/switched",
              },
            });
            yield* Deferred.await(completion).pipe(Effect.timeout("15 seconds"));
            expect(models).toContain("switched");
            const beforeRollback = yield* adapter.readThread(threadId);
            const afterRollback = yield* adapter.rollbackThread(threadId, 1);
            expect(afterRollback.turns.length).toBeLessThan(beforeRollback.turns.length);
            yield* adapter.stopSession(threadId);
            const supervisedThread = ThreadId.make("kilo-sdk-supervised-fixture");
            completion = yield* Deferred.make<void>();
            yield* adapter.startSession({
              threadId: supervisedThread,
              cwd,
              runtimeMode: "approval-required",
            });
            yield* adapter.sendTurn({
              threadId: supervisedThread,
              input: "Run interactive fixture.",
              modelSelection: {
                instanceId: ProviderInstanceId.make("kilo"),
                model: "fixture/interactive",
              },
            });
            yield* Deferred.await(completion).pipe(Effect.timeout("15 seconds"));
            expect(
              events.some(
                (event) => event.type === "request.opened" && event.threadId === supervisedThread,
              ),
            ).toBe(true);
            expect(
              events.some(
                (event) =>
                  event.type === "user-input.requested" && event.threadId === supervisedThread,
              ),
            ).toBe(true);
            expect(
              events.some(
                (event) =>
                  event.type === "user-input.resolved" && event.threadId === supervisedThread,
              ),
            ).toBe(true);
            completion = yield* Deferred.make<void>();
            const waiting = yield* adapter.sendTurn({
              threadId: supervisedThread,
              input: "Wait.",
              modelSelection: {
                instanceId: ProviderInstanceId.make("kilo"),
                model: "fixture/waiting",
              },
            });
            yield* Effect.promise(() => waitingStarted);
            yield* adapter.interruptTurn(supervisedThread, waiting.turnId);
            yield* Deferred.await(completion).pipe(Effect.timeout("5 seconds"));
            expect(
              events.some(
                (event) => event.type === "turn.aborted" && event.turnId === waiting.turnId,
              ),
            ).toBe(true);
            yield* adapter.stopSession(supervisedThread);
          } finally {
            server.closeAllConnections();
            yield* Effect.promise(
              () =>
                new Promise<void>((resolve, reject) =>
                  server.close((error) => (error ? reject(error) : resolve())),
                ),
            );
            yield* Effect.promise(() => NodeFSP.rm(cwd, { recursive: true, force: true }));
          }
        }).pipe(
          Effect.scoped,
          Effect.provide(
            ServerConfig.layerTest(process.cwd(), process.cwd()).pipe(
              Layer.provideMerge(NodeServices.layer),
            ),
          ),
        ),
      45_000,
    );
  },
);

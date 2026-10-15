// @effect-diagnostics nodeBuiltinImport:off
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeHttp from "node:http";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect } from "vite-plus/test";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import {
  ThreadId,
  ProviderInstanceId,
  ProviderSessionId,
  ProjectId,
  RunId,
  RunAttemptId,
  NodeId,
  MessageId,
  type OrchestrationV2ProviderThread,
  type ProviderTurnId,
} from "@t3tools/contracts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import { KiloSettings } from "../settings.ts";
import { makeKiloSdkAdapter } from "./adapter.ts";
import { hardenKiloProbeEnvironment, KILO_PROVIDER_DEFAULT_MODEL_ID } from "./support.ts";
const fixtureSettings = Schema.decodeSync(KiloSettings)({ binaryPath: "kilo" });
const encodeFixtureConfig = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const isolated = ["XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME"].every(
  (key) => {
    const value = process.env[key];
    return value && !NodePath.resolve(value).startsWith(NodeOS.homedir() + NodePath.sep);
  },
);
const layer = Layer.mergeAll(
  IdAllocator.layer,
  McpProviderSessions.layer,
  TestProviderHost.layer(),
).pipe(Layer.provideMerge(NodeServices.layer));
describe.runIf(process.env.T3_KILO_SDK_PROBE === "1" && isolated)("Kilo native V2 CLI", () => {
  it.live(
    "resumes legacy agents and streams child progress, approvals, questions, model switches, steering, and Stop",
    () =>
      Effect.gen(function* () {
        const cwd = yield* Effect.promise(() =>
          NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-kilo-main-probe-")),
        );
        yield* Effect.promise(() =>
          NodeFSP.writeFile(NodePath.join(cwd, ".env.schema"), "FIXTURE_ONLY=child-read-ok\n"),
        );
        let requests = 0;
        let childRead = false;
        const models: string[] = [];
        let releaseBusySteering = () => {};
        let signalBusySteeringStarted = () => {};
        const busySteeringStarted = new Promise<void>((resolve) => {
          signalBusySteeringStarted = resolve;
        });
        let signalWaitingStarted = () => {};
        const waitingStarted = new Promise<void>((resolve) => {
          signalWaitingStarted = resolve;
        });
        let signalChildWaitingStarted = () => {};
        const childWaitingStarted = new Promise<void>((resolve) => {
          signalChildWaitingStarted = resolve;
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
          if (body.model === "waiting" || (body.model === "childwaiting" && child)) {
            response.writeHead(200, { "Content-Type": "text/event-stream" });
            response.write(
              `data: ${JSON.stringify({ id: "waiting", object: "chat.completion.chunk", choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }] })}\n\n`,
            );
            if (body.model === "childwaiting") signalChildWaitingStarted();
            else signalWaitingStarted();
            return;
          }
          const steering = body.model === "steering";
          const steered =
            steering &&
            JSON.stringify(
              body.messages.findLast((message) => message.role === "user")?.content,
            ).includes("STEER_NOW");
          const busySteering = body.model === "steering-busy";
          const busySteered =
            busySteering &&
            JSON.stringify(
              body.messages.findLast((message) => message.role === "user")?.content,
            ).includes("STEER_BUSY");
          if (busySteering && !busySteered) {
            response.writeHead(200, { "Content-Type": "text/event-stream" });
            response.write(
              `data: ${JSON.stringify({ id: "busy", object: "chat.completion.chunk", choices: [{ index: 0, delta: { role: "assistant", content: "OLD_RESPONSE" }, finish_reason: null }] })}\n\n`,
            );
            releaseBusySteering = () =>
              response.end(
                `data: ${JSON.stringify({ id: "busy", object: "chat.completion.chunk", choices: [{ index: 0, delta: {}, finish_reason: "stop" }] })}\n\ndata: [DONE]\n\n`,
              );
            signalBusySteeringStarted();
            return;
          }
          const interactive = body.model === "interactive";
          const toolResults = body.messages.filter((message) => message.role === "tool");
          const interactiveDone = interactive && toolResults.length >= 2;
          const tool = steering
            ? "question"
            : interactive
              ? toolResults.length
                ? "question"
                : "bash"
              : child
                ? "read"
                : "task";
          const args = steering
            ? {
                questions: [
                  {
                    header: "Scope",
                    question: "Choose the fixture scope",
                    options: [{ label: "Workspace", description: "Use this workspace" }],
                  },
                ],
              }
            : interactive
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
          const compaction = !body.tools?.length;
          const done =
            compaction ||
            busySteered ||
            legacySeed ||
            (steering ? steered : interactive ? interactiveDone : Boolean(result));
          const delta = done
            ? {
                content:
                  compaction && !legacySeed
                    ? "COMPACTED_FIXTURE"
                    : busySteered
                      ? "STEERING_BUSY_DONE"
                      : steered
                        ? "STEERING_DONE"
                        : legacySeed
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
          const config = {
            model: "fixture/model",
            provider: {
              fixture: {
                npm: "@ai-sdk/openai-compatible",
                options: { baseURL: `http://127.0.0.1:${address.port}/v1`, apiKey: "fixture" },
                models: Object.fromEntries(
                  [
                    "model",
                    "switched",
                    "interactive",
                    "waiting",
                    "childwaiting",
                    "steering",
                    "steering-busy",
                  ].map((name) => [name, { name, limit: { context: 100000, output: 1000 } }]),
                ),
              },
            },
          };
          const environment = {
            ...hardenKiloProbeEnvironment(process.env),
            KILO_CONFIG_CONTENT: encodeFixtureConfig(config),
          };
          const instanceId = ProviderInstanceId.make("kilo");
          const threadId = ThreadId.make("kilo-main-fixture");
          const open = (
            stage: string,
            env: NodeJS.ProcessEnv,
            existing?: OrchestrationV2ProviderThread,
          ) =>
            Effect.gen(function* () {
              const adapter = yield* makeKiloSdkAdapter(fixtureSettings, {
                environment: env,
                instanceId,
              });
              const policy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
                runtimeMode: stage === "interactive" ? "approval-required" : "full-access",
                interactionMode: "default",
                cwd,
                approvalPolicy: stage === "interactive" ? "on-request" : "never",
              });
              let selection = {
                instanceId,
                model: KILO_PROVIDER_DEFAULT_MODEL_ID,
                options:
                  stage === "legacy"
                    ? [{ id: "agent", value: "t3-full-access-migration-fixture" }]
                    : [],
              };
              const runtime = yield* adapter.openSession({
                threadId,
                providerSessionId: ProviderSessionId.make(`kilo-${stage}`),
                modelSelection: selection,
                runtimePolicy: policy,
              });
              const providerThread = yield* runtime.ensureThread({
                threadId,
                modelSelection: selection,
                runtimePolicy: policy,
                ...(existing ? { existingProviderThread: existing } : {}),
              });
              const events: ProviderAdapter.ProviderAdapterV2Event[] = [];
              const steeringQuestion = yield* Deferred.make<void>();
              let completion = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
              let activeProviderTurnId: ProviderTurnId | undefined;
              const childStopped = yield* Deferred.make<void>();
              yield* runtime.events.pipe(
                Stream.runForEach((event) =>
                  Effect.gen(function* () {
                    events.push(event);
                    if (
                      event.type === "runtime_request.updated" &&
                      event.runtimeRequest.status === "pending" &&
                      selection.model === "fixture/steering"
                    ) {
                      yield* Deferred.succeed(steeringQuestion, undefined);
                      return;
                    }
                    if (
                      event.type === "subagent.updated" &&
                      ["completed", "failed", "interrupted", "cancelled"].includes(
                        event.subagent.status,
                      )
                    )
                      yield* Deferred.succeed(childStopped, undefined);
                    if (
                      event.type === "provider_turn.updated" &&
                      event.providerTurn.providerThreadId === providerThread.id
                    )
                      activeProviderTurnId = event.providerTurn.id;
                    if (
                      event.type === "runtime_request.updated" &&
                      event.runtimeRequest.status === "pending"
                    )
                      yield* runtime.respondToRuntimeRequest({
                        requestId: event.runtimeRequest.id,
                        ...(event.runtimeRequest.kind === "user_input"
                          ? { answers: { Scope: "Workspace" } }
                          : { decision: "accept" as const }),
                      });
                    if (
                      event.type === "turn.terminal" &&
                      event.providerThreadId === providerThread.id
                    )
                      yield* Deferred.succeed(completion, event);
                  }),
                ),
                Effect.forkChild,
              );
              let ordinal = 0;
              const start = (text: string, model = selection.model, supervised = false) =>
                Effect.gen(function* () {
                  ordinal++;
                  completion = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2Event>();
                  selection = { ...selection, model };
                  const now = yield* DateTime.now;
                  const mode = supervised
                    ? ("approval-required" as const)
                    : ("full-access" as const);
                  const turnPolicy = {
                    ...policy,
                    runtimeMode: mode,
                    approvalPolicy: supervised ? ("on-request" as const) : ("never" as const),
                  };
                  yield* runtime.startTurn({
                    appThread: {
                      id: threadId,
                      projectId: ProjectId.make("fixture"),
                      title: "Fixture",
                      providerInstanceId: instanceId,
                      modelSelection: selection,
                      runtimeMode: mode,
                      interactionMode: "default",
                      branch: null,
                      worktreePath: null,
                      activeProviderThreadId: providerThread.id,
                      lineage: {
                        parentThreadId: null,
                        relationshipToParent: null,
                        rootThreadId: threadId,
                      },
                      forkedFrom: null,
                      createdBy: "user",
                      creationSource: "web",
                      createdAt: now,
                      updatedAt: now,
                      archivedAt: null,
                      settledOverride: null,
                      settledAt: null,
                      lastVisitedAt: null,
                      deletedAt: null,
                    },
                    threadId,
                    runId: RunId.make(`run-${stage}-${ordinal}`),
                    runOrdinal: ordinal,
                    providerTurnOrdinal: ordinal,
                    attemptId: RunAttemptId.make(`attempt-${stage}-${ordinal}`),
                    rootNodeId: NodeId.make(`node-${stage}-${ordinal}`),
                    providerThread,
                    message: {
                      createdBy: "user",
                      creationSource: "web",
                      messageId: MessageId.make(`message-${stage}-${ordinal}`),
                      text,
                      attachments: [],
                    },
                    modelSelection: selection,
                    runtimePolicy: turnPolicy,
                  });
                });
              const finish = () => Deferred.await(completion).pipe(Effect.timeout("20 seconds"));
              return {
                runtime,
                threadId,
                runId: () => RunId.make(`run-${stage}-${ordinal}`),
                providerThread,
                events,
                start,
                finish,
                activeTurn: () => activeProviderTurnId,
                waitForSteeringQuestion: () =>
                  Deferred.await(steeringQuestion).pipe(Effect.timeout("15 seconds")),
                waitForChildStop: () =>
                  Deferred.await(childStopped).pipe(Effect.timeout("15 seconds")),
              };
            });
          const legacy = yield* Effect.gen(function* () {
            const seed = yield* open("legacy", {
              ...environment,
              KILO_CONFIG_CONTENT: encodeFixtureConfig({
                ...config,
                agent: {
                  "t3-full-access-migration-fixture": {
                    mode: "primary",
                    prompt: "Run fixture",
                    permission: { "*": "allow" },
                  },
                },
              }),
            });
            yield* seed.start("Seed legacy history.");
            const done = yield* seed.finish();
            expect(done.type === "turn.terminal" && done.status === "completed").toBe(true);
            return seed.providerThread;
          }).pipe(Effect.scoped);
          yield* Effect.gen(function* () {
            const harness = yield* open("native", environment, legacy);
            expect(harness.providerThread.nativeThreadRef?.nativeId).toBe(
              legacy.nativeThreadRef?.nativeId,
            );
            yield* harness.start("Run fixture task.");
            const done = yield* harness.finish();
            expect(done.type === "turn.terminal" && done.status === "completed").toBe(true);
            expect(childRead).toBe(true);
            expect(harness.events.every((event) => event.driver === "kilo")).toBe(true);
            expect(harness.events.some((event) => event.type === "subagent.updated")).toBe(true);
            expect(
              harness.events.some(
                (event) =>
                  event.type === "message.updated" && event.message.text.includes("CHILD_DONE"),
              ),
            ).toBe(true);
            const children = new Map(
              harness.events
                .filter((event) => event.type === "subagent.updated")
                .map((event) => [event.subagent.id, event.subagent.status]),
            );
            expect(
              [...children.values()].every((status) =>
                ["completed", "failed", "interrupted", "cancelled"].includes(status),
              ),
            ).toBe(true);
            yield* harness.start("Switch model.", "fixture/switched");
            yield* harness.finish();
            expect(models).toContain("switched");
            yield* harness.start("/compact", "fixture/model");
            yield* harness.finish();
            const estimate = harness.runtime.estimateNativeContext;
            expect(estimate).toBeDefined();
            const compactedContext = yield* estimate!({ providerThread: harness.providerThread });
            expect(compactedContext).not.toBeNull();
            expect(compactedContext).toBeGreaterThan(0);
            expect(compactedContext).toBeLessThan(16_000);
          }).pipe(Effect.scoped);
          yield* Effect.gen(function* () {
            const harness = yield* open("stop-child", environment);
            // The runtime has one consumer, so observe its collected child receipts below.
            yield* harness.start("Run a waiting child.", "fixture/childwaiting");
            yield* Effect.promise(() => childWaitingStarted).pipe(Effect.timeout("15 seconds"));
            const turnId = harness.activeTurn();
            if (!turnId) throw new Error("No parent turn");
            yield* harness.runtime.steerTurn({
              threadId: harness.threadId,
              runId: harness.runId(),
              providerThread: harness.providerThread,
              providerTurnId: turnId,
              message: {
                messageId: MessageId.make("steer-before-stop"),
                createdBy: "user",
                creationSource: "web",
                text: "A queued steer that must stop too.",
                attachments: [],
              },
            });
            yield* harness.runtime.interruptTurn({
              providerThread: harness.providerThread,
              providerTurnId: turnId,
            });
            const stopped = yield* harness.finish();
            expect(stopped.type === "turn.terminal" && stopped.status === "interrupted").toBe(true);
            yield* harness.waitForChildStop();
            const children = new Map(
              harness.events
                .filter((event) => event.type === "subagent.updated")
                .map((event) => [event.subagent.id, event.subagent.status]),
            );
            expect(children.size).toBeGreaterThan(0);
            expect(
              [...children.values()].every(
                (status) => !["pending", "running", "waiting"].includes(status),
              ),
            ).toBe(true);
          }).pipe(Effect.scoped);
          yield* Effect.gen(function* () {
            const harness = yield* open("interactive", environment);
            yield* harness.start("Run interactive fixture.", "fixture/interactive", true);
            yield* harness.finish();
            expect(
              harness.events.some(
                (event) =>
                  event.type === "runtime_request.updated" &&
                  event.runtimeRequest.kind === "user_input",
              ),
            ).toBe(true);
            expect(
              harness.events.some(
                (event) =>
                  event.type === "runtime_request.updated" &&
                  event.runtimeRequest.kind !== "user_input",
              ),
            ).toBe(true);
            yield* harness.start("Ask before proceeding.", "fixture/steering");
            yield* harness.waitForSteeringQuestion();
            const steeringTurnId = harness.activeTurn();
            if (!steeringTurnId) throw new Error("No steering turn");
            yield* harness.runtime.steerTurn({
              threadId: harness.threadId,
              runId: harness.runId(),
              providerThread: harness.providerThread,
              providerTurnId: steeringTurnId,
              message: {
                messageId: MessageId.make("steer-question"),
                createdBy: "user",
                creationSource: "web",
                text: "STEER_NOW use the workspace and continue.",
                attachments: [],
              },
            });
            yield* harness.finish();
            expect(
              harness.events.some(
                (event) =>
                  event.type === "message.updated" && event.message.text.includes("STEERING_DONE"),
              ),
            ).toBe(true);
            yield* harness.start("Begin a long response.", "fixture/steering-busy");
            yield* Effect.promise(() => busySteeringStarted).pipe(Effect.timeout("15 seconds"));
            const busyTurnId = harness.activeTurn();
            if (!busyTurnId) throw new Error("No busy steering turn");
            yield* harness.runtime.steerTurn({
              threadId: harness.threadId,
              runId: harness.runId(),
              providerThread: harness.providerThread,
              providerTurnId: busyTurnId,
              message: {
                messageId: MessageId.make("steer-busy"),
                createdBy: "user",
                creationSource: "web",
                text: "STEER_BUSY continue with the new instructions.",
                attachments: [],
              },
            });
            releaseBusySteering();
            yield* harness.finish();
            expect(
              harness.events.some(
                (event) =>
                  event.type === "message.updated" &&
                  event.message.text.includes("STEERING_BUSY_DONE"),
              ),
            ).toBe(true);
            yield* harness.start("Wait.", "fixture/waiting");
            yield* Effect.promise(() => waitingStarted);
            const turnId = harness.activeTurn();
            if (!turnId) throw new Error("No active turn");
            yield* harness.runtime.steerTurn({
              threadId: harness.threadId,
              runId: harness.runId(),
              providerThread: harness.providerThread,
              providerTurnId: turnId,
              message: {
                messageId: MessageId.make("steer-before-stop"),
                createdBy: "user",
                creationSource: "web",
                text: "A queued steer that must stop too.",
                attachments: [],
              },
            });
            yield* harness.runtime.interruptTurn({
              providerThread: harness.providerThread,
              providerTurnId: turnId,
            });
            const stopped = yield* harness.finish();
            expect(stopped.type === "turn.terminal" && stopped.status === "interrupted").toBe(true);
          }).pipe(Effect.scoped);
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
      }).pipe(Effect.scoped, Effect.provide(layer)),
    90_000,
  );
});

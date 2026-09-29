// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";

import {
  ApprovalRequestId,
  KiloSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  type RuntimeMode,
  ThreadId,
  type ProviderRuntimeEvent,
  TurnId,
} from "@t3tools/contracts";

import { attachmentRelativePath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import { ProviderAdapterRequestError } from "../Errors.ts";
import type { AcpSessionRuntimeEvent } from "../acp/AcpSessionRuntime.ts";
import { KILO_PROVIDER_DEFAULT_MODEL_ID, startKiloAcpRuntime } from "../acp/KiloAcpSupport.ts";
import {
  type KiloThreadLockRegistry,
  makeKiloAdapter,
  makeKiloThreadLockRegistry,
  resolveKiloRequestedModeId,
} from "./KiloAdapter.ts";
const decodeKiloSettings = Schema.decodeSync(KiloSettings);
const encodeUnknownJsonString = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const isProviderAdapterRequestError = Schema.is(ProviderAdapterRequestError);
const isAcpTransportError = Schema.is(EffectAcpErrors.AcpTransportError);
const decodeKiloEnvironmentLog = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      KILO_PURE: Schema.String,
      KILO_DISABLE_PROJECT_CONFIG: Schema.String,
      KILO_DISABLE_EXTERNAL_SKILLS: Schema.String,
      KILO_DISABLE_SKILL_SHELL: Schema.String,
    }),
  ),
);

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockAgentPath = NodePath.join(__dirname, "../../../scripts/acp-mock-agent.ts");
const mockAgentCommand = process.execPath;

async function makeMockKiloWrapper(extraEnv?: Record<string, string>) {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-acp-mock-"));
  const wrapperPath = NodePath.join(dir, "fake-kilo.sh");
  const envExports = Object.entries(extraEnv ?? {})
    .map(([key, value]) => `export ${key}=${JSON.stringify(value)}`)
    .join("\n");
  const script = `#!/bin/sh
${envExports}
if [ "$1" = "--version" ]; then
  if [ -n "$T3_KILO_VERSION_LOG_PATH" ]; then
    printf "version\n" >> "$T3_KILO_VERSION_LOG_PATH"
  fi
  echo "kilo \${T3_KILO_MOCK_VERSION:-7.4.23}"
  exit 0
fi
exec ${JSON.stringify(mockAgentCommand)} ${JSON.stringify(mockAgentPath)} "$@"
`;
  await NodeFSP.writeFile(wrapperPath, script, "utf8");
  await NodeFSP.chmod(wrapperPath, 0o755);
  return wrapperPath;
}

async function readJsonLines(filePath: string) {
  const raw = await NodeFSP.readFile(filePath, "utf8");
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

const waitForPathCreation = (filePath: string) =>
  Effect.gen(function* () {
    const exists = yield* Effect.promise(() =>
      NodeFSP.access(filePath).then(
        () => true,
        () => false,
      ),
    );
    if (exists) return;

    yield* Effect.callback<void>((resume) => {
      const watcher = NodeFS.watch(NodePath.dirname(filePath), (event, filename) => {
        if (event === "rename" && String(filename) === NodePath.basename(filePath)) {
          watcher.close();
          resume(Effect.void);
        }
      });
      watcher.once("error", (error) => resume(Effect.die(error)));
      return Effect.sync(() => watcher.close());
    });
  }).pipe(
    Effect.timeoutOrElse({ duration: "2 seconds", orElse: () => Effect.die("marker timeout") }),
  );

const kiloAdapterTestLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-kilo-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

const makeTestAdapter = (binaryPath: string, options?: Parameters<typeof makeKiloAdapter>[1]) =>
  makeKiloAdapter(decodeKiloSettings({ binaryPath }), options).pipe(Effect.orDie);

const makeDrainOverridingStarter =
  (drainEvents: Effect.Effect<void>): typeof startKiloAcpRuntime =>
  (input, configureRuntime) =>
    startKiloAcpRuntime(input, configureRuntime).pipe(
      Effect.map(({ runtime, started }) => ({
        runtime: { ...runtime, drainEvents },
        started,
      })),
    );

const runPermissionScenario = (input: {
  readonly name: string;
  readonly runtimeMode: "approval-required" | "auto-accept-edits";
  readonly scenario?:
    | "same-execute"
    | "different-execute"
    | "different-other"
    | "malformed"
    | "edit";
  readonly turns?: number;
}) =>
  Effect.gen(function* () {
    const threadId = ThreadId.make(`kilo-permission-${input.name}`);
    const tempDir = yield* Effect.promise(() =>
      NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-acp-permission-scenario-")),
    );
    const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
    const wrapperPath = yield* Effect.promise(() =>
      makeMockKiloWrapper({
        T3_ACP_REQUEST_LOG_PATH: requestLogPath,
        T3_ACP_EMIT_TOOL_CALLS: "1",
        T3_ACP_PERMISSION_SCENARIO: input.scenario ?? "same-execute",
      }),
    );
    const adapter = yield* makeTestAdapter(wrapperPath);
    const runtimeEvents: ProviderRuntimeEvent[] = [];
    const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
      Effect.sync(() => {
        runtimeEvents.push(event);
      }).pipe(
        Effect.andThen(
          event.type === "request.opened"
            ? adapter.respondToRequest(
                threadId,
                ApprovalRequestId.make(String(event.requestId)),
                "acceptForSession",
              )
            : Effect.void,
        ),
      ),
    ).pipe(Effect.forkChild);

    yield* adapter.startSession({
      threadId,
      provider: ProviderDriverKind.make("kilo"),
      cwd: process.cwd(),
      runtimeMode: input.runtimeMode,
    });
    for (let turn = 0; turn < (input.turns ?? 2); turn += 1) {
      yield* adapter.sendTurn({ threadId, input: `permission turn ${turn}`, attachments: [] });
    }

    yield* Fiber.interrupt(eventsFiber);
    yield* adapter.stopSession(threadId);
    return {
      runtimeEvents,
      requests: yield* Effect.promise(() => readJsonLines(requestLogPath)),
    };
  });

it.effect("resolves only T3's injected agent ids and fails closed otherwise", () =>
  Effect.sync(() => {
    const injectedIds = { supervisedModeId: "t3-supervised", planModeId: "t3-plan" };
    // A Kilo-native mode that merely looks like a plan mode must never be
    // selected: running it would drop T3's child permission policy.
    assert.isUndefined(
      resolveKiloRequestedModeId({
        interactionMode: "plan",
        modeState: {
          currentModeId: "plan",
          availableModes: [
            { id: "plan", name: "Plan" },
            { id: "architect", name: "Architect", description: "Use plan for this turn" },
          ],
        },
        ...injectedIds,
      }),
    );
    assert.equal(
      resolveKiloRequestedModeId({
        interactionMode: "plan",
        modeState: {
          currentModeId: "plan",
          availableModes: [
            { id: "plan", name: "Plan" },
            { id: "t3-plan", name: "T3 read-only plan" },
          ],
        },
        ...injectedIds,
      }),
      "t3-plan",
    );
    assert.equal(
      resolveKiloRequestedModeId({
        interactionMode: undefined,
        modeState: {
          currentModeId: "code",
          availableModes: [
            { id: "code", name: "Code" },
            { id: "t3-supervised", name: "T3 supervised code" },
          ],
        },
        ...injectedIds,
      }),
      "t3-supervised",
    );
    assert.isUndefined(
      resolveKiloRequestedModeId({
        interactionMode: undefined,
        modeState: {
          currentModeId: "code",
          availableModes: [{ id: "code", name: "Code" }],
        },
        ...injectedIds,
      }),
    );
    assert.isUndefined(
      resolveKiloRequestedModeId({
        interactionMode: "plan",
        modeState: undefined,
        ...injectedIds,
      }),
    );
  }),
);

it.layer(kiloAdapterTestLayer)("KiloAdapterLive", (it) => {
  it.effect("isolates non-full interactive children while preserving full-access extensions", () =>
    Effect.gen(function* () {
      const cases: ReadonlyArray<{
        readonly mode: RuntimeMode;
        readonly expectedPure: string;
        readonly expectedProjectConfig: string;
      }> = [
        { mode: "approval-required", expectedPure: "1", expectedProjectConfig: "1" },
        { mode: "auto-accept-edits", expectedPure: "1", expectedProjectConfig: "1" },
        { mode: "auto", expectedPure: "1", expectedProjectConfig: "1" },
        { mode: "full-access", expectedPure: "0", expectedProjectConfig: "0" },
      ];

      for (const testCase of cases) {
        const tempDir = yield* Effect.promise(() =>
          NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-mode-environment-")),
        );
        const environmentLogPath = NodePath.join(tempDir, "environment.json");
        const wrapperPath = yield* Effect.promise(() =>
          makeMockKiloWrapper({ T3_ACP_ENV_LOG_PATH: environmentLogPath }),
        );
        const adapter = yield* makeTestAdapter(wrapperPath, {
          environment: {
            KILO_PURE: "0",
            KILO_DISABLE_PROJECT_CONFIG: "0",
            KILO_DISABLE_EXTERNAL_SKILLS: "0",
            KILO_DISABLE_SKILL_SHELL: "0",
          },
        });
        const threadId = ThreadId.make(`kilo-mode-environment-${testCase.mode}`);
        yield* adapter.startSession({
          threadId,
          provider: ProviderDriverKind.make("kilo"),
          cwd: process.cwd(),
          runtimeMode: testCase.mode,
        });
        const environment = decodeKiloEnvironmentLog(
          yield* Effect.promise(() => NodeFSP.readFile(environmentLogPath, "utf8")),
        );
        assert.equal(environment.KILO_PURE, testCase.expectedPure);
        assert.equal(environment.KILO_DISABLE_PROJECT_CONFIG, testCase.expectedProjectConfig);
        assert.equal(environment.KILO_DISABLE_EXTERNAL_SKILLS, "0");
        assert.equal(environment.KILO_DISABLE_SKILL_SHELL, "0");
        yield* adapter.stopSession(threadId);
      }
    }),
  );

  it.effect("prunes thread locks after repeated sessions and queued waiters", () =>
    Effect.gen(function* () {
      const threadLocks = yield* makeKiloThreadLockRegistry;
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-version-cache-")),
      );
      const versionLogPath = NodePath.join(tempDir, "versions.log");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({ T3_KILO_VERSION_LOG_PATH: versionLogPath }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath, { threadLockRegistry: threadLocks });

      for (let index = 0; index < 3; index += 1) {
        const threadId = ThreadId.make(`kilo-lock-cycle-${index}`);
        yield* adapter.startSession({
          threadId,
          provider: ProviderDriverKind.make("kilo"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });
        yield* adapter.stopSession(threadId);
        assert.equal(yield* threadLocks.activeKeyCount, 0);
        assert.equal(yield* threadLocks.activeUserCount, 0);
      }
      assert.equal(
        (yield* Effect.promise(() => NodeFSP.readFile(versionLogPath, "utf8"))).trim().split("\n")
          .length,
        1,
      );

      const firstEntered = yield* Deferred.make<void>();
      const releaseFirst = yield* Deferred.make<void>();
      const secondEntered = yield* Deferred.make<void>();
      const first = yield* threadLocks
        .withLock(
          "shared-thread",
          Deferred.succeed(firstEntered, undefined).pipe(
            Effect.andThen(Deferred.await(releaseFirst)),
          ),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(firstEntered);
      const second = yield* threadLocks
        .withLock("shared-thread", Deferred.succeed(secondEntered, undefined))
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;

      assert.equal(yield* threadLocks.activeKeyCount, 1);
      assert.equal(yield* threadLocks.activeUserCount, 2);
      assert.isFalse(yield* Deferred.isDone(secondEntered));
      yield* Deferred.succeed(releaseFirst, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(second);
      assert.isTrue(yield* Deferred.isDone(secondEntered));
      assert.equal(yield* threadLocks.activeKeyCount, 0);
      assert.equal(yield* threadLocks.activeUserCount, 0);
    }),
  );

  it.effect("rejects an unsupported Kilo version before spawning ACP", () =>
    Effect.gen(function* () {
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-unsupported-version-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
          T3_KILO_MOCK_VERSION: "7.3.16",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const result = yield* adapter
        .startSession({
          threadId: ThreadId.make("kilo-unsupported-version"),
          provider: ProviderDriverKind.make("kilo"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
        })
        .pipe(Effect.exit);

      assert.isTrue(Exit.isFailure(result));
      if (Exit.isFailure(result)) {
        assert.include(String(Cause.squash(result.cause)), "Upgrade to v7.4.23 or newer");
      }
      assert.isFalse(
        yield* Effect.promise(() =>
          NodeFSP.access(requestLogPath).then(
            () => true,
            () => false,
          ),
        ),
      );
    }),
  );

  it.effect("shares one failing version preflight across concurrent session starts", () =>
    Effect.gen(function* () {
      const releaseVersion = yield* Deferred.make<void>();
      const versionSpawned = yield* Deferred.make<void>();
      const spawnCount = yield* Ref.make(0);
      const spawner = ChildProcessSpawner.make(() =>
        Ref.updateAndGet(spawnCount, (count) => count + 1).pipe(
          Effect.flatMap((count) =>
            Deferred.succeed(versionSpawned, undefined).pipe(
              Effect.as(
                ChildProcessSpawner.makeHandle({
                  pid: ChildProcessSpawner.ProcessId(700 + count),
                  exitCode: Deferred.await(releaseVersion).pipe(
                    Effect.as(ChildProcessSpawner.ExitCode(0)),
                  ),
                  isRunning: Effect.succeed(true),
                  kill: () => Effect.void,
                  unref: Effect.succeed(Effect.void),
                  stdin: Sink.drain,
                  stdout: Stream.fromEffect(Deferred.await(releaseVersion)).pipe(
                    Stream.map(() => new TextEncoder().encode("kilo 7.3.16")),
                  ),
                  stderr: Stream.empty,
                  all: Stream.empty,
                  getInputFd: () => Sink.drain,
                  getOutputFd: () => Stream.empty,
                }),
              ),
            ),
          ),
        ),
      );
      const wrapperPath = yield* Effect.promise(() => makeMockKiloWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      const start = (threadId: ThreadId) =>
        adapter
          .startSession({
            threadId,
            provider: ProviderDriverKind.make("kilo"),
            cwd: process.cwd(),
            runtimeMode: "full-access",
          })
          .pipe(Effect.exit);
      const first = yield* start(ThreadId.make("kilo-shared-version-first")).pipe(Effect.forkChild);
      const second = yield* start(ThreadId.make("kilo-shared-version-second")).pipe(
        Effect.forkChild,
      );

      yield* Deferred.await(versionSpawned);
      for (let attempt = 0; attempt < 4; attempt += 1) yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(spawnCount), 1);
      yield* Deferred.succeed(releaseVersion, undefined);
      const exits = yield* Effect.all([Fiber.join(first), Fiber.join(second)]);
      assert.isTrue(exits.every(Exit.isFailure));
      assert.equal(yield* Ref.get(spawnCount), 1);
    }),
  );

  it.effect("starts a session and maps mock ACP prompt flow to runtime events", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-mock-thread");
      const wrapperPath = yield* Effect.promise(() => makeMockKiloWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);

      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnCompleted = yield* Deferred.make<void>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed"
              ? Deferred.succeed(turnCompleted, undefined)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      assert.equal(session.provider, "kilo");
      assert.deepStrictEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "mock-session-1",
      });

      yield* adapter.sendTurn({
        threadId,
        input: "hello kilo",
        attachments: [],
      });

      yield* Deferred.await(turnCompleted);
      yield* Fiber.interrupt(runtimeEventsFiber);
      const types = runtimeEvents.map((e) => e.type);

      assert.includeMembers(types, [
        "session.started",
        "session.state.changed",
        "thread.started",
        "turn.started",
        "content.delta",
        "turn.completed",
      ] as const);

      const delta = runtimeEvents.find((e) => e.type === "content.delta");
      assert.isDefined(delta);
      if (delta?.type === "content.delta") {
        assert.equal(delta.payload.delta, "hello from mock");
        assert.equal(delta.turnId, runtimeEvents.find((e) => e.type === "turn.completed")?.turnId);
      }
      assert.isBelow(types.indexOf("content.delta"), types.indexOf("turn.completed"));

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("retains only attachment counts in the in-memory turn snapshot", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-lightweight-turn-snapshot");
      const wrapperPath = yield* Effect.promise(() => makeMockKiloWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const { attachmentsDir } = yield* ServerConfig;
      const attachment = {
        type: "image" as const,
        id: "kilo-lightweight-turn-12345678-1234-1234-1234-123456789abc",
        name: "diagram.png",
        mimeType: "image/png",
        sizeBytes: 4,
      };
      const attachmentPath = NodePath.join(attachmentsDir, attachmentRelativePath(attachment)!);
      yield* Effect.promise(async () => {
        await NodeFSP.mkdir(NodePath.dirname(attachmentPath), { recursive: true });
        await NodeFSP.writeFile(attachmentPath, Uint8Array.from([1, 2, 3, 4]));
      });

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({
        threadId,
        input: "inspect this image",
        attachments: [attachment],
      });

      const snapshot = yield* adapter.readThread(threadId);
      assert.deepInclude(snapshot.turns[0]?.items[0], {
        prompt: { textBlockCount: 1, imageBlockCount: 1 },
      });
      const snapshotJson = encodeUnknownJsonString(snapshot);
      assert.notInclude(snapshotJson, "AQIDBA==");
      assert.notInclude(snapshotJson, "inspect this image");
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("selects models through ACP config options on start and steer", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-model-config-thread");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-acp-request-log-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.jsonl");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "auto",
        modelSelection: { instanceId: ProviderInstanceId.make("kilo"), model: "composer-2" },
      });

      yield* adapter.sendTurn({
        threadId,
        input: "switch models mid-thread",
        attachments: [],
        modelSelection: { instanceId: ProviderInstanceId.make("kilo"), model: "gpt-5.4" },
      });

      // The mock resolves prompts immediately; stopping the session gives a
      // deterministic observation point before reading what the mock logged.
      yield* adapter.stopSession(threadId);

      const logged = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const setConfigRequests = logged
        .filter((entry) => entry.method === "session/set_config_option")
        .map((entry) => entry.params as Record<string, unknown>);

      // Model writes happen at session start and again when the steer lands;
      // the adapter may interleave a mode write between them.
      assert.deepStrictEqual(
        setConfigRequests
          .filter((params) => params.configId === "model")
          .map((params) => params.value),
        ["composer-2", "gpt-5.4"],
      );

      const modeRequest = setConfigRequests.find((params) => params.configId === "mode");
      // The mock exposes a plan/code/ask mode set; auto runtime mode must land
      // on an implementation mode rather than plan.
      if (modeRequest) {
        assert.notEqual(modeRequest.value, "plan");
      }
    }),
  );

  it.effect("normalizes the provider-default sentinel before exposing session state", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-provider-default-model");
      const wrapperPath = yield* Effect.promise(() => makeMockKiloWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: {
          instanceId: ProviderInstanceId.make("kilo"),
          model: KILO_PROVIDER_DEFAULT_MODEL_ID,
        },
      });

      assert.equal(session.model, "default");
      assert.notEqual(session.model, KILO_PROVIDER_DEFAULT_MODEL_ID);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("rejects an unadvertised explicit model before sending a config write", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-unadvertised-model");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-unadvertised-model-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({ T3_ACP_REQUEST_LOG_PATH: requestLogPath }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      const result = yield* adapter
        .startSession({
          threadId,
          provider: ProviderDriverKind.make("kilo"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
          modelSelection: {
            instanceId: ProviderInstanceId.make("kilo"),
            model: "kilo/custom/unadvertised",
          },
        })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      assert.isFalse(
        requests.some((entry) => {
          if (entry.method !== "session/set_config_option") return false;
          const params = entry.params as Record<string, unknown> | undefined;
          return params?.configId === "model" && params.value === "kilo/custom/unadvertised";
        }),
      );
    }),
  );

  it.effect("auto-approves tool permissions in full-access mode", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-full-access-thread");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-acp-full-access-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
          T3_ACP_EMIT_TOOL_CALLS: "1",
          T3_ACP_ALLOW_ONCE_OPTION_ID: "kilo-allow-once",
          T3_ACP_ALLOW_ALWAYS_OPTION_ID: "kilo-allow-always",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const turnCompleted = yield* Deferred.make<void>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "turn.completed"
              ? Deferred.succeed(turnCompleted, undefined)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.sendTurn({ threadId, input: "run it", attachments: [] });
      yield* Deferred.await(turnCompleted);
      yield* Fiber.interrupt(runtimeEventsFiber);

      const approvalOpened = runtimeEvents.some((e) => e.type === "request.opened");
      assert.isFalse(approvalOpened);
      const completed = runtimeEvents.find((e) => e.type === "turn.completed");
      if (completed?.type !== "turn.completed") {
        return assert.fail("expected a turn.completed runtime event");
      }
      assert.equal(completed.payload.state, "completed");

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      assert.isTrue(
        requests.some((entry) => {
          if (entry.method !== "session/set_config_option") return false;
          const params = entry.params as Record<string, unknown> | undefined;
          return (
            params?.configId === "mode" &&
            typeof params.value === "string" &&
            params.value.startsWith("t3-full-access-")
          );
        }),
      );
      assert.isTrue(
        requests.some((entry) => {
          if ("method" in entry || typeof entry.result !== "object" || entry.result === null) {
            return false;
          }
          const outcome = (entry.result as Record<string, unknown>).outcome as
            | Record<string, unknown>
            | undefined;
          return outcome?.outcome === "selected" && outcome.optionId === "kilo-allow-once";
        }),
      );
    }),
  );

  it.effect("prompts again when the same broad kind identifies a different exact tool", () =>
    Effect.gen(function* () {
      const { runtimeEvents } = yield* runPermissionScenario({
        name: "different-execute",
        runtimeMode: "approval-required",
        scenario: "different-execute",
      });
      assert.equal(runtimeEvents.filter((event) => event.type === "request.opened").length, 2);
    }),
  );

  it.effect("separates exact identities for tools Kilo classifies as other", () =>
    Effect.gen(function* () {
      const { runtimeEvents } = yield* runPermissionScenario({
        name: "different-other",
        runtimeMode: "approval-required",
        scenario: "different-other",
      });
      assert.equal(runtimeEvents.filter((event) => event.type === "request.opened").length, 2);
    }),
  );

  it.effect("does not cache malformed permission identities", () =>
    Effect.gen(function* () {
      const { runtimeEvents } = yield* runPermissionScenario({
        name: "malformed",
        runtimeMode: "approval-required",
        scenario: "malformed",
      });
      assert.equal(runtimeEvents.filter((event) => event.type === "request.opened").length, 2);
    }),
  );

  it.effect("auto-accept-edits approves a well-formed edit without opening a request", () =>
    Effect.gen(function* () {
      const { runtimeEvents } = yield* runPermissionScenario({
        name: "auto-edit",
        runtimeMode: "auto-accept-edits",
        scenario: "edit",
        turns: 1,
      });
      assert.equal(runtimeEvents.filter((event) => event.type === "request.opened").length, 0);
    }),
  );

  it.effect("auto-accept-edits still prompts for command execution", () =>
    Effect.gen(function* () {
      const { runtimeEvents } = yield* runPermissionScenario({
        name: "auto-command",
        runtimeMode: "auto-accept-edits",
        turns: 1,
      });
      assert.equal(runtimeEvents.filter((event) => event.type === "request.opened").length, 1);
    }),
  );

  it.effect("routes tool permissions through approvals in approval-required mode", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-approval-thread");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-acp-approval-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
          T3_ACP_EMIT_TOOL_CALLS: "1",
          T3_ACP_ALLOW_ONCE_OPTION_ID: "agent-defined-approval-id",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          runtimeEvents.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "request.opened"
              ? adapter.respondToRequest(
                  threadId,
                  ApprovalRequestId.make(String(event.requestId)),
                  "acceptForSession",
                )
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });
      yield* adapter.sendTurn({ threadId, input: "approve this", attachments: [] });
      yield* adapter.sendTurn({ threadId, input: "same permission again", attachments: [] });

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);

      assert.equal(runtimeEvents.filter((e) => e.type === "request.opened").length, 1);
      assert.equal(runtimeEvents.filter((e) => e.type === "request.resolved").length, 1);
      assert.isTrue(runtimeEvents.some((e) => e.type === "content.delta"));

      // The mock advertises a custom allow-once option id; accepting must
      // select that agent-provided option, not a client-invented id.
      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      assert.isTrue(
        requests.some((entry) => {
          if (entry.method !== "session/set_config_option") return false;
          const params = entry.params as Record<string, unknown> | undefined;
          return (
            params?.configId === "mode" &&
            typeof params.value === "string" &&
            params.value.startsWith("t3-ask-")
          );
        }),
      );
      assert.isTrue(
        requests.some((entry) => {
          if ("method" in entry || typeof entry.result !== "object" || entry.result === null) {
            return false;
          }
          const result = entry.result as Record<string, unknown>;
          const outcome = result.outcome as Record<string, unknown> | undefined;
          return (
            outcome?.outcome === "selected" && outcome.optionId === "agent-defined-approval-id"
          );
        }),
      );
    }),
  );

  it.effect("does not register a permission after its turn was interrupted during logging", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-permission-registration-vs-interrupt");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({ T3_ACP_EMIT_TOOL_CALLS: "1" }),
      );
      const permissionLoggingStarted = yield* Deferred.make<void>();
      const releasePermissionLogging = yield* Deferred.make<void>();
      const cancellationWaiting = yield* Deferred.make<void>();
      const respondToRequests = yield* Ref.make(false);
      const secondRequestOpened = yield* Deferred.make<ApprovalRequestId>();
      const cancellationAwareStarter: typeof startKiloAcpRuntime = (input, configureRuntime) =>
        startKiloAcpRuntime(
          {
            ...input,
            beforeCancelSettlementWait: Deferred.succeed(cancellationWaiting, undefined),
          },
          configureRuntime,
        );
      const adapter = yield* makeTestAdapter(wrapperPath, {
        startAcpRuntime: cancellationAwareStarter,
        nativeEventLogger: {
          filePath: "memory://kilo-permission-registration-race",
          write: (record: unknown) => {
            const encoded = JSON.stringify(record);
            return encoded.includes('"kind":"notification"') &&
              encoded.includes('"method":"session/request_permission"')
              ? Deferred.succeed(permissionLoggingStarted, undefined).pipe(
                  Effect.andThen(Deferred.await(releasePermissionLogging)),
                )
              : Effect.void;
          },
          close: () => Effect.void,
        },
      });
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "request.opened"
              ? Ref.get(respondToRequests).pipe(
                  Effect.flatMap((shouldRespond) =>
                    shouldRespond
                      ? Deferred.succeed(
                          secondRequestOpened,
                          ApprovalRequestId.make(String(event.requestId)),
                        ).pipe(
                          Effect.andThen(
                            adapter.respondToRequest(
                              threadId,
                              ApprovalRequestId.make(String(event.requestId)),
                              "decline",
                            ),
                          ),
                        )
                      : Effect.void,
                  ),
                )
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });
      const firstTurn = yield* adapter
        .sendTurn({ threadId, input: "interrupt before approval registration", attachments: [] })
        .pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(permissionLoggingStarted);
      const interrupt = yield* adapter
        .interruptTurn(threadId)
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(cancellationWaiting);
      yield* Deferred.succeed(releasePermissionLogging, undefined);
      assert.isTrue(Exit.isSuccess(yield* Fiber.join(interrupt)));
      yield* Fiber.join(firstTurn);

      assert.equal(runtimeEvents.filter((event) => event.type === "request.opened").length, 0);
      assert.equal(runtimeEvents.filter((event) => event.type === "request.resolved").length, 0);

      yield* Ref.set(respondToRequests, true);
      const secondTurn = yield* adapter
        .sendTurn({ threadId, input: "permission after cancelled turn", attachments: [] })
        .pipe(Effect.forkChild);
      yield* Deferred.await(secondRequestOpened);
      yield* Fiber.join(secondTurn);
      assert.equal(runtimeEvents.filter((event) => event.type === "request.opened").length, 1);

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("completes a claimed approval even when its responder is interrupted", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-claimed-approval-interruption");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({ T3_ACP_EMIT_TOOL_CALLS: "1" }),
      );
      const approvalClaimed = yield* Deferred.make<void>();
      const releaseApproval = yield* Deferred.make<void>();
      const adapter = yield* makeTestAdapter(wrapperPath, {
        afterApprovalClaim: () =>
          Deferred.succeed(approvalClaimed, undefined).pipe(
            Effect.andThen(Deferred.await(releaseApproval)),
          ),
      });
      const opened = yield* Deferred.make<ApprovalRequestId>();
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "request.opened"
              ? Deferred.succeed(opened, ApprovalRequestId.make(String(event.requestId))).pipe(
                  Effect.ignore,
                )
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });
      const firstTurn = yield* adapter
        .sendTurn({ threadId, input: "claim approval", attachments: [] })
        .pipe(Effect.forkChild);
      const requestId = yield* Deferred.await(opened);
      const response = yield* adapter
        .respondToRequest(threadId, requestId, "acceptForSession")
        .pipe(Effect.forkChild);
      yield* Deferred.await(approvalClaimed);
      const interruptResponse = yield* Fiber.interrupt(response).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Deferred.succeed(releaseApproval, undefined);
      yield* Fiber.join(interruptResponse);
      yield* Fiber.join(firstTurn);

      const duplicate = yield* adapter
        .respondToRequest(threadId, requestId, "decline")
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(duplicate));
      yield* adapter.sendTurn({ threadId, input: "same permission", attachments: [] });
      assert.equal(runtimeEvents.filter((event) => event.type === "request.opened").length, 1);

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("cancel wins over a delayed accept-for-session response", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-cancel-approval-race");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({ T3_ACP_EMIT_TOOL_CALLS: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const opened = yield* Deferred.make<ApprovalRequestId>();
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "request.opened"
              ? Deferred.succeed(opened, ApprovalRequestId.make(String(event.requestId))).pipe(
                  Effect.ignore,
                )
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });
      const firstTurn = yield* adapter
        .sendTurn({ threadId, input: "first permission", attachments: [] })
        .pipe(Effect.exit, Effect.forkChild);
      const firstRequestId = yield* Deferred.await(opened);
      yield* adapter.interruptTurn(threadId);
      yield* Fiber.join(firstTurn);

      const staleResponse = yield* adapter
        .respondToRequest(threadId, firstRequestId, "acceptForSession")
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(staleResponse));

      const openedAgain = yield* Deferred.make<ApprovalRequestId>();
      const secondEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        event.type === "request.opened"
          ? Deferred.succeed(openedAgain, ApprovalRequestId.make(String(event.requestId))).pipe(
              Effect.ignore,
            )
          : Effect.void,
      ).pipe(Effect.forkChild);
      const secondTurn = yield* adapter
        .sendTurn({ threadId, input: "same permission again", attachments: [] })
        .pipe(Effect.forkChild);
      const secondRequestId = yield* Deferred.await(openedAgain);
      yield* adapter.respondToRequest(threadId, secondRequestId, "accept");
      yield* Fiber.join(secondTurn);

      assert.equal(runtimeEvents.filter((event) => event.type === "request.opened").length, 2);
      yield* Fiber.interrupt(secondEventsFiber);
      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("serializes an approval claim with interrupt cancellation", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-approval-claim-vs-interrupt");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({ T3_ACP_EMIT_TOOL_CALLS: "1" }),
      );
      const approvalClaimed = yield* Deferred.make<void>();
      const releaseApproval = yield* Deferred.make<void>();
      const adapter = yield* makeTestAdapter(wrapperPath, {
        afterApprovalClaim: () =>
          Deferred.succeed(approvalClaimed, undefined).pipe(
            Effect.andThen(Deferred.await(releaseApproval)),
          ),
      });
      const opened = yield* Deferred.make<ApprovalRequestId>();
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        event.type === "request.opened"
          ? Deferred.succeed(opened, ApprovalRequestId.make(String(event.requestId))).pipe(
              Effect.ignore,
            )
          : Effect.void,
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });
      const turn = yield* adapter
        .sendTurn({ threadId, input: "claim before interrupt", attachments: [] })
        .pipe(Effect.forkChild);
      const requestId = yield* Deferred.await(opened);
      const response = yield* adapter
        .respondToRequest(threadId, requestId, "acceptForSession")
        .pipe(Effect.forkChild);
      yield* Deferred.await(approvalClaimed);
      const interrupt = yield* adapter.interruptTurn(threadId).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Deferred.succeed(releaseApproval, undefined);
      yield* Fiber.join(response);
      yield* Fiber.join(interrupt);
      yield* Fiber.join(turn);

      const duplicate = yield* adapter
        .respondToRequest(threadId, requestId, "decline")
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(duplicate));
      yield* adapter.sendTurn({ threadId, input: "same permission", attachments: [] });

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("reject wins over a duplicate accept-for-session response", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-reject-approval-race");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({ T3_ACP_EMIT_TOOL_CALLS: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const opened = yield* Deferred.make<ApprovalRequestId>();
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        event.type === "request.opened"
          ? Deferred.succeed(opened, ApprovalRequestId.make(String(event.requestId))).pipe(
              Effect.ignore,
            )
          : Effect.void,
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });
      const firstTurn = yield* adapter
        .sendTurn({ threadId, input: "reject this", attachments: [] })
        .pipe(Effect.forkChild);
      const firstRequestId = yield* Deferred.await(opened);
      yield* adapter.respondToRequest(threadId, firstRequestId, "decline");
      const duplicate = yield* adapter
        .respondToRequest(threadId, firstRequestId, "acceptForSession")
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(duplicate));
      yield* Fiber.join(firstTurn);

      const openedAgain = yield* Deferred.make<ApprovalRequestId>();
      const secondEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        event.type === "request.opened"
          ? Deferred.succeed(openedAgain, ApprovalRequestId.make(String(event.requestId))).pipe(
              Effect.ignore,
            )
          : Effect.void,
      ).pipe(Effect.forkChild);
      const secondTurn = yield* adapter
        .sendTurn({ threadId, input: "same permission again", attachments: [] })
        .pipe(Effect.forkChild);
      const secondRequestId = yield* Deferred.await(openedAgain);
      yield* adapter.respondToRequest(threadId, secondRequestId, "accept");
      yield* Fiber.join(secondTurn);

      yield* Fiber.interrupt(secondEventsFiber);
      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("merges concurrent sends into one turn and settles after both prompts", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-concurrent-send");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({ T3_ACP_PROMPT_DELAY_MS: "50" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const receipts = yield* Effect.all(
        [
          adapter.sendTurn({ threadId, input: "first", attachments: [] }),
          adapter.sendTurn({ threadId, input: "second", attachments: [] }),
        ],
        { concurrency: "unbounded" },
      ).pipe(TestClock.withLive);

      assert.equal(receipts[0].turnId, receipts[1].turnId);
      assert.equal(runtimeEvents.filter((event) => event.type === "turn.started").length, 1);
      assert.equal(runtimeEvents.filter((event) => event.type === "turn.completed").length, 1);
      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("interrupt invalidates a second prompt queued behind the active prompt", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-cancel-queued-prompt");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-queued-prompt-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
          T3_ACP_EMIT_PROMPT_STARTED_BEFORE_HANG: "1",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const firstPromptStarted = yield* Deferred.make<void>();
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "content.delta" && event.payload.delta === "prompt reached mock"
              ? Deferred.succeed(firstPromptStarted, undefined)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const first = yield* adapter
        .sendTurn({ threadId, input: "hang first", attachments: [] })
        .pipe(Effect.forkChild);
      yield* Deferred.await(firstPromptStarted);
      const second = yield* adapter
        .sendTurn({ threadId, input: "must not start", attachments: [] })
        .pipe(Effect.forkChild);
      for (let attempt = 0; attempt < 8; attempt += 1) yield* Effect.yieldNow;
      yield* adapter.interruptTurn(threadId);
      yield* Fiber.join(first);
      yield* Fiber.join(second);

      const requests = yield* Effect.promise(() => readJsonLines(requestLogPath));
      assert.equal(requests.filter((entry) => entry.method === "session/prompt").length, 1);
      const completed = runtimeEvents.filter((event) => event.type === "turn.completed");
      assert.equal(completed.length, 1);
      if (completed[0]?.type === "turn.completed") {
        assert.equal(completed[0].payload.state, "cancelled");
      }
      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("ignores an interrupt addressed to a stale turn id", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-stale-turn-interrupt");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
          T3_ACP_EMIT_PROMPT_STARTED_BEFORE_HANG: "1",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const activeTurnStarted = yield* Deferred.make<TurnId | undefined>();
      const promptStarted = yield* Deferred.make<void>();
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "turn.started"
              ? Deferred.succeed(activeTurnStarted, event.turnId).pipe(Effect.ignore)
              : event.type === "content.delta" && event.payload.delta === "prompt reached mock"
                ? Deferred.succeed(promptStarted, undefined).pipe(Effect.ignore)
                : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const send = yield* adapter
        .sendTurn({ threadId, input: "keep the current turn running", attachments: [] })
        .pipe(Effect.forkChild);
      const activeTurnId = yield* Deferred.await(activeTurnStarted);
      assert.ok(activeTurnId);
      yield* Deferred.await(promptStarted);

      yield* adapter.interruptTurn(threadId, TurnId.make("stale-turn-id"));
      const [session] = yield* adapter.listSessions();
      assert.equal(session?.status, "running");
      assert.equal(session?.activeTurnId, activeTurnId);
      assert.equal(runtimeEvents.filter((event) => event.type === "turn.completed").length, 0);

      yield* adapter.interruptTurn(threadId, activeTurnId);
      yield* Fiber.join(send);
      const completed = runtimeEvents.filter((event) => event.type === "turn.completed");
      assert.equal(completed.length, 1);
      if (completed[0]?.type === "turn.completed") {
        assert.equal(completed[0].turnId, activeTurnId);
        assert.equal(completed[0].payload.state, "cancelled");
      }

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("revalidates a stale interrupt after a newer turn wins the thread lock", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-stale-turn-lock-revalidation");
      const wrapperPath = yield* Effect.promise(() => makeMockKiloWrapper());
      const baseThreadLocks = yield* makeKiloThreadLockRegistry;
      const nextWaiterRegistered = yield* Deferred.make<void>();
      const staleInterruptRegistered = yield* Deferred.make<void>();
      let observeQueuedRegistrations = false;
      let queuedRegistrationCount = 0;
      const threadLocks: KiloThreadLockRegistry = {
        withLock: (registeredThreadId, effect) =>
          Effect.suspend(() => {
            if (!observeQueuedRegistrations) {
              return baseThreadLocks.withLock(registeredThreadId, effect);
            }
            const registration = queuedRegistrationCount;
            queuedRegistrationCount += 1;
            const receipt =
              registration === 0
                ? Deferred.succeed(nextWaiterRegistered, undefined)
                : registration === 1
                  ? Deferred.succeed(staleInterruptRegistered, undefined)
                  : Effect.void;
            return receipt.pipe(
              Effect.andThen(baseThreadLocks.withLock(registeredThreadId, effect)),
            );
          }),
        activeKeyCount: baseThreadLocks.activeKeyCount,
        activeUserCount: baseThreadLocks.activeUserCount,
      };
      const firstSettlementLocked = yield* Deferred.make<void>();
      const releaseFirstSettlement = yield* Deferred.make<void>();
      const blockFirstSettlement = yield* Ref.make(true);
      const adapter = yield* makeTestAdapter(wrapperPath, {
        threadLockRegistry: threadLocks,
        beforePromptSettlement: () =>
          Ref.getAndSet(blockFirstSettlement, false).pipe(
            Effect.flatMap((shouldBlock) =>
              shouldBlock
                ? Deferred.succeed(firstSettlementLocked, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseFirstSettlement)),
                  )
                : Effect.void,
            ),
          ),
      });
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const firstSend = yield* adapter
        .sendTurn({ threadId, input: "finish turn A", attachments: [] })
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(firstSettlementLocked);
      const [turnASession] = yield* adapter.listSessions();
      const turnAId = turnASession?.activeTurnId;
      assert.ok(turnAId);

      observeQueuedRegistrations = true;
      const secondSend = yield* adapter
        .sendTurn({ threadId, input: "start turn B", attachments: [] })
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(nextWaiterRegistered);
      const staleInterrupt = yield* adapter
        .interruptTurn(threadId, turnAId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(staleInterruptRegistered);
      yield* Deferred.succeed(releaseFirstSettlement, undefined);

      const firstReceipt = yield* Fiber.join(firstSend);
      yield* Fiber.join(staleInterrupt);
      const secondReceipt = yield* Fiber.join(secondSend);
      assert.notEqual(firstReceipt.turnId, secondReceipt.turnId);
      const completions = runtimeEvents.filter((event) => event.type === "turn.completed");
      assert.equal(completions.length, 2);
      assert.isTrue(
        completions.every(
          (event) => event.type === "turn.completed" && event.payload.state === "completed",
        ),
      );

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("quarantines the session when the ACP cancellation transport times out", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-cancel-transport-failure");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-cancel-failure-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_EXIT_LOG_PATH: exitLogPath,
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
          T3_ACP_EMIT_PROMPT_STARTED_BEFORE_HANG: "1",
        }),
      );
      const cancelTransportStarted = yield* Deferred.make<void>();
      const neverReleaseCancelTransport = yield* Deferred.make<void>();
      const timingOutCancelStarter: typeof startKiloAcpRuntime = (input, configureRuntime) =>
        startKiloAcpRuntime(
          {
            ...input,
            beforeCancelTransportWrite: Deferred.succeed(cancelTransportStarted, undefined).pipe(
              Effect.andThen(Deferred.await(neverReleaseCancelTransport)),
            ),
          },
          configureRuntime,
        );
      const adapter = yield* makeTestAdapter(wrapperPath, {
        startAcpRuntime: timingOutCancelStarter,
      });
      const promptStarted = yield* Deferred.make<void>();
      const sessionExited = yield* Deferred.make<void>();
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "content.delta" && event.payload.delta === "prompt reached mock"
              ? Deferred.succeed(promptStarted, undefined)
              : event.type === "session.exited"
                ? Deferred.succeed(sessionExited, undefined)
                : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const send = yield* adapter
        .sendTurn({ threadId, input: "keep running on failed cancel", attachments: [] })
        .pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(promptStarted);
      const interrupt = yield* adapter
        .interruptTurn(threadId)
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(cancelTransportStarted);
      yield* TestClock.adjust("5 seconds");
      const interruptExit = yield* Fiber.join(interrupt);
      yield* Deferred.await(sessionExited);
      yield* Fiber.join(send);

      assert.isTrue(Exit.isFailure(interruptExit));
      if (Exit.isFailure(interruptExit)) {
        const failure = Cause.squash(interruptExit.cause);
        assert.isTrue(isProviderAdapterRequestError(failure));
        if (isProviderAdapterRequestError(failure)) {
          assert.equal(failure.method, "session/cancel");
          assert.isTrue(isAcpTransportError(failure.cause));
        }
      }
      assert.deepStrictEqual(yield* adapter.listSessions(), []);
      assert.isFalse(yield* adapter.hasSession(threadId));
      const completed = runtimeEvents.filter((event) => event.type === "turn.completed");
      assert.equal(completed.length, 1);
      if (completed[0]?.type === "turn.completed") {
        assert.equal(completed[0].payload.state, "failed");
      }
      const exited = runtimeEvents.find((event) => event.type === "session.exited");
      assert.equal(exited?.type === "session.exited" && exited.payload.exitKind, "error");
      assert.include(yield* Effect.promise(() => NodeFSP.readFile(exitLogPath, "utf8")), "exit:");

      const nextPrompt = yield* adapter
        .sendTurn({ threadId, input: "must not reuse uncertain child", attachments: [] })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(nextPrompt));
      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("quarantines owned cancellation when the interrupt caller is interrupted", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-interrupt-caller-interrupted");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-interrupt-owner-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_EXIT_LOG_PATH: exitLogPath,
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
          T3_ACP_EMIT_PROMPT_STARTED_BEFORE_HANG: "1",
        }),
      );
      const cancelTransportStarted = yield* Deferred.make<void>();
      const holdCancelTransport = yield* Deferred.make<void>();
      const cancellationStarter: typeof startKiloAcpRuntime = (input, configureRuntime) =>
        startKiloAcpRuntime(
          {
            ...input,
            beforeCancelTransportWrite: Deferred.succeed(cancelTransportStarted, undefined).pipe(
              Effect.andThen(Deferred.await(holdCancelTransport)),
            ),
          },
          configureRuntime,
        );
      const adapter = yield* makeTestAdapter(wrapperPath, {
        startAcpRuntime: cancellationStarter,
      });
      const promptStarted = yield* Deferred.make<void>();
      const sessionExited = yield* Deferred.make<void>();
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "content.delta" && event.payload.delta === "prompt reached mock"
              ? Deferred.succeed(promptStarted, undefined)
              : event.type === "session.exited"
                ? Deferred.succeed(sessionExited, undefined)
                : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const send = yield* adapter
        .sendTurn({ threadId, input: "interrupt cancellation owner", attachments: [] })
        .pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(promptStarted);
      const interrupting = yield* adapter
        .interruptTurn(threadId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(cancelTransportStarted);
      yield* Fiber.interrupt(interrupting);
      const interruptExit = yield* Fiber.await(interrupting);
      yield* Deferred.await(sessionExited);
      yield* Fiber.join(send);

      assert.isTrue(Exit.isFailure(interruptExit));
      if (Exit.isFailure(interruptExit)) {
        assert.isTrue(Cause.hasInterrupts(interruptExit.cause));
      }
      assert.isFalse(yield* adapter.hasSession(threadId));
      assert.deepStrictEqual(yield* adapter.listSessions(), []);
      const completed = runtimeEvents.filter((event) => event.type === "turn.completed");
      assert.equal(completed.length, 1);
      if (completed[0]?.type === "turn.completed") {
        assert.equal(completed[0].payload.state, "failed");
      }
      assert.include(yield* Effect.promise(() => NodeFSP.readFile(exitLogPath, "utf8")), "exit:");
      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("quarantines the session when the remote prompt never settles after cancellation", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-cancel-settlement-timeout");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-cancel-timeout-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");
      const cancellationWaiting = yield* Deferred.make<void>();
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_EXIT_LOG_PATH: exitLogPath,
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
          T3_ACP_EMIT_PROMPT_STARTED_BEFORE_HANG: "1",
          T3_ACP_IGNORE_PROMPT_CANCEL_SETTLEMENT: "1",
        }),
      );
      const boundedCancelStarter: typeof startKiloAcpRuntime = (input, configureRuntime) =>
        startKiloAcpRuntime(
          {
            ...input,
            cancelSettleTimeout: "1 second",
            beforeCancelSettlementWait: Deferred.succeed(cancellationWaiting, undefined),
          },
          configureRuntime,
        );
      const adapter = yield* makeTestAdapter(wrapperPath, {
        startAcpRuntime: boundedCancelStarter,
      });
      const promptStarted = yield* Deferred.make<void>();
      const sessionExited = yield* Deferred.make<void>();
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "content.delta" && event.payload.delta === "prompt reached mock"
              ? Deferred.succeed(promptStarted, undefined)
              : event.type === "session.exited"
                ? Deferred.succeed(sessionExited, undefined)
                : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const send = yield* adapter
        .sendTurn({ threadId, input: "remote ignores cancellation", attachments: [] })
        .pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(promptStarted);
      const interrupt = yield* adapter
        .interruptTurn(threadId)
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(cancellationWaiting);
      yield* TestClock.adjust("1100 millis");
      const interruptExit = yield* Fiber.join(interrupt);
      yield* Deferred.await(sessionExited);
      yield* Fiber.join(send);

      assert.isTrue(Exit.isFailure(interruptExit));
      if (Exit.isFailure(interruptExit)) {
        const failure = Cause.squash(interruptExit.cause);
        assert.isTrue(isProviderAdapterRequestError(failure));
        if (isProviderAdapterRequestError(failure)) {
          assert.equal(failure.method, "session/cancel");
          assert.isTrue(isAcpTransportError(failure.cause));
        }
      }
      assert.deepStrictEqual(yield* adapter.listSessions(), []);
      assert.isFalse(yield* adapter.hasSession(threadId));
      const completed = runtimeEvents.filter((event) => event.type === "turn.completed");
      assert.equal(completed.length, 1);
      if (completed[0]?.type === "turn.completed") {
        assert.equal(completed[0].payload.state, "failed");
      }
      assert.include(yield* Effect.promise(() => NodeFSP.readFile(exitLogPath, "utf8")), "exit:");

      const nextPrompt = yield* adapter
        .sendTurn({ threadId, input: "must require a new session", attachments: [] })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(nextPrompt));
      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("drops late ACP output after a cancelled turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-drop-late-cancelled-output");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
          T3_ACP_EMIT_PROMPT_STARTED_BEFORE_HANG: "1",
          T3_ACP_EMIT_LATE_UPDATE_AFTER_CANCEL: "1",
        }),
      );
      const lateNativeUpdate = yield* Deferred.make<void>();
      const adapter = yield* makeTestAdapter(wrapperPath, {
        nativeEventLogger: {
          filePath: "memory://kilo-cancelled-native-events",
          write: (record: unknown) =>
            JSON.stringify(record).includes("late after cancel")
              ? Deferred.succeed(lateNativeUpdate, undefined).pipe(Effect.asVoid)
              : Effect.void,
          close: () => Effect.void,
        },
      });
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const promptStarted = yield* Deferred.make<void>();
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "content.delta" && event.payload.delta === "prompt reached mock"
              ? Deferred.succeed(promptStarted, undefined).pipe(Effect.ignore)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const send = yield* adapter
        .sendTurn({ threadId, input: "cancel before stale output", attachments: [] })
        .pipe(Effect.forkChild);
      yield* Deferred.await(promptStarted);
      yield* adapter.interruptTurn(threadId);
      // Kilo acknowledges cancellation by settling session/prompt. The
      // adapter must not release the turn for replacement until output queued
      // before that acknowledgement has crossed its event barrier.
      assert.isTrue(yield* Deferred.isDone(lateNativeUpdate));
      const cancelled = yield* Fiber.join(send);
      const replacement = yield* adapter.sendTurn({
        threadId,
        input: "replacement after cancelled prompt",
        attachments: [],
      });
      for (let attempt = 0; attempt < 8; attempt += 1) yield* Effect.yieldNow;

      const cancelledIndex = runtimeEvents.findIndex(
        (event) => event.type === "turn.completed" && event.payload.state === "cancelled",
      );
      const outputTypes = new Set([
        "content.delta",
        "item.started",
        "item.updated",
        "item.completed",
        "turn.plan.updated",
      ]);
      assert.isAtLeast(cancelledIndex, 0);
      assert.notEqual(cancelled.turnId, replacement.turnId);
      assert.deepEqual(
        runtimeEvents
          .slice(cancelledIndex + 1)
          .filter((event) => event.turnId === cancelled.turnId && outputTypes.has(event.type)),
        [],
      );

      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect(
    "continues consuming notifications and acknowledges barriers after one handler defect",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("kilo-notification-handler-defect");
        const wrapperPath = yield* Effect.promise(() => makeMockKiloWrapper());
        const failNextSessionUpdate = yield* Ref.make(true);
        const failureInjected = yield* Deferred.make<void>();
        const adapter = yield* makeTestAdapter(wrapperPath, {
          nativeEventLogger: {
            filePath: "memory://kilo-notification-handler-defect",
            write: (record: unknown) => {
              const encoded = JSON.stringify(record);
              if (
                !encoded.includes('"kind":"notification"') ||
                !encoded.includes('"method":"session/update"')
              ) {
                return Effect.void;
              }
              return Ref.getAndSet(failNextSessionUpdate, false).pipe(
                Effect.flatMap((shouldFail) =>
                  shouldFail
                    ? Deferred.succeed(failureInjected, undefined).pipe(
                        Effect.andThen(Effect.die("mock notification logger defect")),
                      )
                    : Effect.void,
                ),
              );
            },
            close: () => Effect.void,
          },
        });
        const contentAfterFailure = yield* Deferred.make<void>();
        const turnCompleted = yield* Deferred.make<void>();
        const runtimeEvents: ProviderRuntimeEvent[] = [];
        const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
          Effect.sync(() => runtimeEvents.push(event)).pipe(
            Effect.andThen(
              event.type === "content.delta" && event.payload.delta === "hello from mock"
                ? Deferred.succeed(contentAfterFailure, undefined).pipe(Effect.ignore)
                : event.type === "turn.completed"
                  ? Deferred.succeed(turnCompleted, undefined).pipe(Effect.ignore)
                  : Effect.void,
            ),
          ),
        ).pipe(Effect.forkChild);

        yield* adapter.startSession({
          threadId,
          provider: ProviderDriverKind.make("kilo"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });
        const send = yield* adapter
          .sendTurn({ threadId, input: "continue after one bad notification", attachments: [] })
          .pipe(Effect.forkChild);
        yield* Deferred.await(failureInjected);
        yield* Deferred.await(contentAfterFailure);
        const result = yield* Fiber.join(send);
        yield* Deferred.await(turnCompleted);

        assert.equal(result.threadId, threadId);
        assert.isTrue(
          runtimeEvents.some(
            (event) => event.type === "turn.completed" && event.payload.state === "completed",
          ),
        );
        yield* Fiber.interrupt(eventsFiber);
        yield* adapter.stopSession(threadId);
      }),
  );

  it.effect("settles a prompt failure exactly once and restores the session to ready", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-prompt-failure");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({ T3_ACP_FAIL_PROMPT: "1" }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const result = yield* adapter
        .sendTurn({ threadId, input: "fail", attachments: [] })
        .pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
      const completed = runtimeEvents.filter((event) => event.type === "turn.completed");
      assert.equal(completed.length, 1);
      if (completed[0]?.type === "turn.completed") {
        assert.equal(completed[0].payload.state, "failed");
        assert.include(completed[0].payload.errorMessage ?? "", "Mock prompt failure");
      }
      const sessions = yield* adapter.listSessions();
      assert.equal(sessions[0]?.status, "ready");
      assert.isUndefined(sessions[0]?.activeTurnId);
      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("rejects whitespace before mutating session state or writing configuration", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-whitespace-validation");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-whitespace-")),
      );
      const requestLogPath = NodePath.join(tempDir, "requests.ndjson");
      const markerPath = NodePath.join(tempDir, "unexpected-config");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_REQUEST_LOG_PATH: requestLogPath,
          T3_ACP_SET_CONFIG_DELAY_MS: "200",
          T3_ACP_SET_CONFIG_DELAY_AFTER: "1",
          T3_ACP_SET_CONFIG_MARKER_PATH: markerPath,
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "auto",
      });
      const before = yield* Effect.promise(() => readJsonLines(requestLogPath));
      const result = yield* adapter
        .sendTurn({
          threadId,
          input: "   ",
          attachments: [],
          modelSelection: { instanceId: ProviderInstanceId.make("kilo"), model: "gpt-5.4" },
        })
        .pipe(Effect.exit);
      const after = yield* Effect.promise(() => readJsonLines(requestLogPath));

      assert.isTrue(Exit.isFailure(result));
      assert.equal(
        after.filter((entry) => entry.method === "session/set_config_option").length,
        before.filter((entry) => entry.method === "session/set_config_option").length,
      );
      assert.isFalse(
        yield* Effect.promise(() =>
          NodeFSP.access(markerPath).then(
            () => true,
            () => false,
          ),
        ),
      );
      assert.equal(runtimeEvents.filter((event) => event.type === "turn.started").length, 0);
      const sessions = yield* adapter.listSessions();
      assert.equal(sessions[0]?.status, "ready");
      assert.isUndefined(sessions[0]?.activeTurnId);
      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("balances turn events when stop waits for prompt preparation", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-stop-during-preparation");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-stop-preparation-")),
      );
      const markerPath = NodePath.join(tempDir, "set-config-started");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_SET_CONFIG_DELAY_MS: "200",
          T3_ACP_SET_CONFIG_DELAY_AFTER: "1",
          T3_ACP_SET_CONFIG_MARKER_PATH: markerPath,
          T3_ACP_PROMPT_DELAY_MS: "500",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "auto",
      });
      const send = yield* adapter
        .sendTurn({
          threadId,
          input: "prepare slowly",
          attachments: [],
          interactionMode: "plan",
        })
        .pipe(Effect.exit, Effect.forkChild);
      yield* waitForPathCreation(markerPath);
      yield* adapter.stopSession(threadId);
      yield* Fiber.join(send);

      assert.equal(runtimeEvents.filter((event) => event.type === "turn.started").length, 1);
      const completed = runtimeEvents.filter((event) => event.type === "turn.completed");
      assert.equal(completed.length, 1);
      if (completed[0]?.type === "turn.completed") {
        assert.equal(completed[0].payload.state, "cancelled");
      }
      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("an interrupt observed during preparation prevents the prompt from starting", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-interrupt-during-preparation");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-interrupt-preparation-")),
      );
      const markerPath = NodePath.join(tempDir, "set-config-started");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_SET_CONFIG_DELAY_MS: "200",
          T3_ACP_SET_CONFIG_DELAY_AFTER: "1",
          T3_ACP_SET_CONFIG_MARKER_PATH: markerPath,
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "auto",
      });
      const send = yield* adapter
        .sendTurn({
          threadId,
          input: "interrupt preparation",
          attachments: [],
          interactionMode: "plan",
        })
        .pipe(Effect.exit, Effect.forkChild);
      yield* waitForPathCreation(markerPath);
      yield* adapter.interruptTurn(threadId);
      const result = yield* Fiber.join(send);
      assert.isTrue(Exit.isFailure(result));
      assert.equal(runtimeEvents.filter((event) => event.type === "turn.started").length, 0);
      assert.equal(runtimeEvents.filter((event) => event.type === "turn.completed").length, 0);
      const sessions = yield* adapter.listSessions();
      assert.equal(sessions[0]?.status, "ready");
      assert.isUndefined(sessions[0]?.activeTurnId);
      yield* Fiber.interrupt(eventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect(
    "releases an interrupted prompt claim before the next queued send acquires the lock",
    () =>
      Effect.gen(function* () {
        const threadId = ThreadId.make("kilo-send-interrupted-after-claim");
        const wrapperPath = yield* Effect.promise(() => makeMockKiloWrapper());
        const baseThreadLocks = yield* makeKiloThreadLockRegistry;
        const nextSendRegistered = yield* Deferred.make<void>();
        let observeNextRegistration = false;
        const threadLocks: KiloThreadLockRegistry = {
          withLock: (registeredThreadId, effect) =>
            Effect.suspend(() =>
              (observeNextRegistration
                ? Deferred.succeed(nextSendRegistered, undefined).pipe(Effect.ignore)
                : Effect.void
              ).pipe(Effect.andThen(baseThreadLocks.withLock(registeredThreadId, effect))),
            ),
          activeKeyCount: baseThreadLocks.activeKeyCount,
          activeUserCount: baseThreadLocks.activeUserCount,
        };
        const firstPromptClaimed = yield* Deferred.make<TurnId>();
        const holdFirstPrompt = yield* Deferred.make<void>();
        const secondPromptClaimed = yield* Deferred.make<TurnId>();
        const shouldHoldFirstPrompt = yield* Ref.make(true);
        const adapter = yield* makeTestAdapter(wrapperPath, {
          threadLockRegistry: threadLocks,
          afterPromptClaim: (turnId) =>
            Ref.getAndSet(shouldHoldFirstPrompt, false).pipe(
              Effect.flatMap((shouldHold) =>
                shouldHold
                  ? Deferred.succeed(firstPromptClaimed, turnId).pipe(
                      Effect.andThen(Deferred.await(holdFirstPrompt)),
                    )
                  : Deferred.succeed(secondPromptClaimed, turnId).pipe(Effect.ignore),
              ),
            ),
        });
        const runtimeEvents: ProviderRuntimeEvent[] = [];
        const secondPromptCompleted = yield* Deferred.make<void>();
        const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
          Effect.sync(() => runtimeEvents.push(event)).pipe(
            Effect.andThen(
              event.type === "turn.completed"
                ? Deferred.succeed(secondPromptCompleted, undefined).pipe(Effect.ignore)
                : Effect.void,
            ),
          ),
        ).pipe(Effect.forkChild);

        yield* adapter.startSession({
          threadId,
          provider: ProviderDriverKind.make("kilo"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });
        const firstSend = yield* adapter
          .sendTurn({ threadId, input: "interrupt after prompt claim", attachments: [] })
          .pipe(Effect.forkChild({ startImmediately: true }));
        const firstTurnId = yield* Deferred.await(firstPromptClaimed);
        observeNextRegistration = true;
        const secondSend = yield* adapter
          .sendTurn({
            threadId,
            input: "a fresh prompt must not steer the interrupted claim",
            attachments: [],
          })
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(nextSendRegistered);
        assert.isFalse(yield* Deferred.isDone(secondPromptClaimed));
        yield* Fiber.interrupt(firstSend);
        const firstExit = yield* Fiber.await(firstSend);
        const secondTurnId = yield* Deferred.await(secondPromptClaimed);
        const second = yield* Fiber.join(secondSend);
        yield* Deferred.await(secondPromptCompleted);

        assert.isTrue(Exit.isFailure(firstExit));
        if (Exit.isFailure(firstExit)) {
          assert.isTrue(Cause.hasInterrupts(firstExit.cause));
        }
        assert.notEqual(secondTurnId, firstTurnId);
        assert.equal(second.turnId, secondTurnId);
        const readySession = (yield* adapter.listSessions())[0];
        assert.equal(readySession?.status, "ready");
        assert.isUndefined(readySession?.activeTurnId);
        const started = runtimeEvents.filter((event) => event.type === "turn.started");
        const completed = runtimeEvents.filter((event) => event.type === "turn.completed");
        assert.equal(started.length, 1);
        assert.equal(completed.length, 1);
        assert.equal(started[0]?.turnId, secondTurnId);
        assert.equal(completed[0]?.turnId, secondTurnId);

        yield* Fiber.interrupt(eventsFiber);
        yield* adapter.stopSession(threadId);
      }),
  );

  it.effect("closes the ACP child process when a session stops", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-stop-session-close");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-adapter-exit-log-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");

      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_EXIT_LOG_PATH: exitLogPath,
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      assert.isTrue(yield* adapter.hasSession(threadId));

      yield* adapter.stopSession(threadId);
      assert.isFalse(yield* adapter.hasSession(threadId));

      const exitLog = yield* Effect.tryPromise(() => NodeFSP.readFile(exitLogPath, "utf8")).pipe(
        Effect.orElseSucceed(() => ""),
      );
      assert.include(exitLog, "exit:");
    }),
  );

  it.effect("finishes stop cleanup before delivering interruption during event drain", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-stop-interrupted-during-drain");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-stop-drain-interrupt-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_EXIT_LOG_PATH: exitLogPath,
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
          T3_ACP_EMIT_PROMPT_STARTED_BEFORE_HANG: "1",
        }),
      );
      const drainStarted = yield* Deferred.make<void>();
      const releaseDrain = yield* Deferred.make<void>();
      const drainFinalized = yield* Deferred.make<void>();
      const adapter = yield* makeTestAdapter(wrapperPath, {
        startAcpRuntime: makeDrainOverridingStarter(
          Deferred.succeed(drainStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseDrain)),
            Effect.ensuring(Deferred.succeed(drainFinalized, undefined)),
          ),
        ),
        stopDrainTimeout: "10 seconds",
      });
      const promptStarted = yield* Deferred.make<void>();
      const sessionExited = yield* Deferred.make<void>();
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "content.delta" && event.payload.delta === "prompt reached mock"
              ? Deferred.succeed(promptStarted, undefined).pipe(Effect.ignore)
              : event.type === "session.exited"
                ? Deferred.succeed(sessionExited, undefined).pipe(Effect.ignore)
                : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const send = yield* adapter
        .sendTurn({ threadId, input: "stop during drain", attachments: [] })
        .pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(promptStarted);
      const stopping = yield* adapter
        .stopSession(threadId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(drainStarted);
      const interrupting = yield* Fiber.interrupt(stopping).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.succeed(releaseDrain, undefined);
      yield* Fiber.join(interrupting);
      const stopExit = yield* Fiber.await(stopping);
      yield* Deferred.await(drainFinalized);
      yield* Deferred.await(sessionExited);
      yield* Fiber.join(send);

      assert.isTrue(Exit.isFailure(stopExit));
      if (Exit.isFailure(stopExit)) {
        assert.isTrue(Cause.hasInterrupts(stopExit.cause));
      }
      assert.isFalse(yield* adapter.hasSession(threadId));
      assert.deepStrictEqual(yield* adapter.listSessions(), []);
      assert.equal(runtimeEvents.filter((event) => event.type === "turn.completed").length, 1);
      assert.equal(runtimeEvents.filter((event) => event.type === "session.exited").length, 1);
      assert.isBelow(
        runtimeEvents.findIndex(
          (event) =>
            event.type === "content.delta" && event.payload.delta === "prompt reached mock",
        ),
        runtimeEvents.findIndex((event) => event.type === "turn.completed"),
      );
      assert.isTrue(Exit.isFailure(yield* adapter.stopSession(threadId).pipe(Effect.exit)));
      assert.isTrue(
        Exit.isFailure(
          yield* adapter
            .sendTurn({ threadId, input: "must not reuse stopped child", attachments: [] })
            .pipe(Effect.exit),
        ),
      );
      yield* adapter.stopAll();
      assert.equal(runtimeEvents.filter((event) => event.type === "session.exited").length, 1);
      assert.include(yield* Effect.promise(() => NodeFSP.readFile(exitLogPath, "utf8")), "exit:");
      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("times out a hung event drain and still closes the owned session", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-stop-hung-event-drain");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-stop-drain-timeout-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_EXIT_LOG_PATH: exitLogPath,
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
          T3_ACP_EMIT_PROMPT_STARTED_BEFORE_HANG: "1",
        }),
      );
      const drainStarted = yield* Deferred.make<void>();
      const drainFinalized = yield* Deferred.make<void>();
      const adapter = yield* makeTestAdapter(wrapperPath, {
        startAcpRuntime: makeDrainOverridingStarter(
          Deferred.succeed(drainStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(drainFinalized, undefined)),
          ),
        ),
        stopDrainTimeout: "1 second",
      });
      const promptStarted = yield* Deferred.make<void>();
      const sessionExited = yield* Deferred.make<void>();
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "content.delta" && event.payload.delta === "prompt reached mock"
              ? Deferred.succeed(promptStarted, undefined).pipe(Effect.ignore)
              : event.type === "session.exited"
                ? Deferred.succeed(sessionExited, undefined).pipe(Effect.ignore)
                : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const send = yield* adapter
        .sendTurn({ threadId, input: "hang the stop drain", attachments: [] })
        .pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(promptStarted);
      const stopping = yield* adapter.stopSession(threadId).pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(drainStarted);
      yield* TestClock.adjust("1 second");
      const stopExit = yield* Fiber.join(stopping);
      yield* Deferred.await(drainFinalized);
      yield* Deferred.await(sessionExited);
      yield* Fiber.join(send);

      assert.isTrue(Exit.isSuccess(stopExit));
      assert.isFalse(yield* adapter.hasSession(threadId));
      assert.deepStrictEqual(yield* adapter.listSessions(), []);
      assert.equal(runtimeEvents.filter((event) => event.type === "turn.completed").length, 1);
      assert.equal(runtimeEvents.filter((event) => event.type === "session.exited").length, 1);
      yield* adapter.stopAll();
      assert.equal(runtimeEvents.filter((event) => event.type === "session.exited").length, 1);
      assert.include(yield* Effect.promise(() => NodeFSP.readFile(exitLogPath, "utf8")), "exit:");
      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("quiesces post-barrier ACP output before completing a stopped turn", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-stop-successful-drain-post-barrier-output");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-stop-post-barrier-output-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_EXIT_LOG_PATH: exitLogPath,
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
          T3_ACP_EMIT_PROMPT_STARTED_BEFORE_HANG: "1",
        }),
      );
      const releasePostBarrierEvent = yield* Deferred.make<void>();
      const postBarrierMappingStarted = yield* Deferred.make<void>();
      const releasePostBarrierMapping = yield* Deferred.make<void>();
      const postBarrierMappingFinalized = yield* Deferred.make<void>();
      const terminalSawQuiescedConsumer = yield* Ref.make(false);
      const startAcpRuntime: typeof startKiloAcpRuntime = (input, configureRuntime) =>
        startKiloAcpRuntime(input, configureRuntime).pipe(
          Effect.map(({ runtime, started }) => {
            const postBarrierEvent: AcpSessionRuntimeEvent = {
              _tag: "ContentDelta",
              text: "late after successful drain",
              rawPayload: { source: "post-barrier-regression" },
            };
            return {
              started,
              runtime: {
                ...runtime,
                drainEvents: runtime.drainEvents.pipe(
                  Effect.andThen(Deferred.succeed(releasePostBarrierEvent, undefined)),
                  Effect.andThen(Deferred.await(postBarrierMappingStarted)),
                ),
                getEvents: () =>
                  Stream.merge(
                    runtime.getEvents(),
                    Stream.fromEffect(Deferred.await(releasePostBarrierEvent)).pipe(
                      Stream.map(() => postBarrierEvent),
                    ),
                  ),
              },
            };
          }),
        );
      const adapter = yield* makeTestAdapter(wrapperPath, {
        startAcpRuntime,
        stopDrainTimeout: "10 seconds",
        nativeEventLogger: {
          filePath: "memory://kilo-post-barrier-native-events",
          write: (record: unknown) =>
            JSON.stringify(record).includes("post-barrier-regression")
              ? Deferred.succeed(postBarrierMappingStarted, undefined).pipe(
                  Effect.andThen(Deferred.await(releasePostBarrierMapping)),
                  Effect.ensuring(Deferred.succeed(postBarrierMappingFinalized, undefined)),
                  Effect.asVoid,
                )
              : Effect.void,
          close: () => Effect.void,
        },
        afterStopTurnTerminal: () =>
          Deferred.isDone(postBarrierMappingFinalized).pipe(
            Effect.flatMap((quiesced) => Ref.set(terminalSawQuiescedConsumer, quiesced)),
            Effect.andThen(Deferred.succeed(releasePostBarrierMapping, undefined)),
          ),
      });
      const promptStarted = yield* Deferred.make<void>();
      const sessionExited = yield* Deferred.make<void>();
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "content.delta" && event.payload.delta === "prompt reached mock"
              ? Deferred.succeed(promptStarted, undefined).pipe(Effect.ignore)
              : event.type === "session.exited"
                ? Deferred.succeed(sessionExited, undefined).pipe(Effect.ignore)
                : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const send = yield* adapter
        .sendTurn({ threadId, input: "stop after a successful drain", attachments: [] })
        .pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(promptStarted);
      const stopExit = yield* adapter.stopSession(threadId).pipe(Effect.exit);
      yield* Deferred.await(sessionExited);
      yield* Fiber.join(send);

      assert.isTrue(Exit.isSuccess(stopExit));
      assert.isTrue(yield* Deferred.isDone(postBarrierMappingStarted));
      assert.isTrue(yield* Deferred.isDone(postBarrierMappingFinalized));
      assert.isTrue(yield* Ref.get(terminalSawQuiescedConsumer));
      const completedIndex = runtimeEvents.findIndex((event) => event.type === "turn.completed");
      assert.isAtLeast(completedIndex, 0);
      assert.deepEqual(
        runtimeEvents.slice(completedIndex + 1).filter((event) => event.turnId !== undefined),
        [],
      );
      assert.deepEqual(
        runtimeEvents.filter(
          (event) =>
            event.type === "content.delta" && event.payload.delta === "late after successful drain",
        ),
        [],
      );
      assert.equal(runtimeEvents.filter((event) => event.type === "turn.completed").length, 1);
      assert.equal(runtimeEvents.filter((event) => event.type === "session.exited").length, 1);
      assert.isFalse(yield* adapter.hasSession(threadId));
      assert.deepStrictEqual(yield* adapter.listSessions(), []);
      assert.include(yield* Effect.promise(() => NodeFSP.readFile(exitLogPath, "utf8")), "exit:");
      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("drops late ACP output after a stop drain timeout", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-stop-drain-timeout-late-output");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-stop-timeout-late-output-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_EXIT_LOG_PATH: exitLogPath,
          T3_ACP_HANG_FIRST_PROMPT_FOREVER: "1",
          T3_ACP_EMIT_PROMPT_STARTED_BEFORE_HANG: "1",
        }),
      );
      const drainStarted = yield* Deferred.make<void>();
      const drainFinalized = yield* Deferred.make<void>();
      const releaseLateEvents = yield* Deferred.make<void>();
      const lateEventsHandled = yield* Deferred.make<void>();
      const startAcpRuntime: typeof startKiloAcpRuntime = (input, configureRuntime) =>
        startKiloAcpRuntime(input, configureRuntime).pipe(
          Effect.map(({ runtime, started }) => {
            const lateEvents: ReadonlyArray<AcpSessionRuntimeEvent> = [
              {
                _tag: "ContentDelta",
                text: "late after timed-out drain",
                rawPayload: { source: "late-timeout-regression" },
              },
              { _tag: "EventStreamBarrier", acknowledge: lateEventsHandled },
            ];
            return {
              started,
              runtime: {
                ...runtime,
                drainEvents: Deferred.succeed(drainStarted, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.ensuring(Deferred.succeed(drainFinalized, undefined)),
                ),
                getEvents: () =>
                  Stream.merge(
                    runtime.getEvents(),
                    Stream.fromEffect(Deferred.await(releaseLateEvents)).pipe(
                      Stream.flatMap(() => Stream.fromIterable(lateEvents)),
                    ),
                  ),
              },
            };
          }),
        );
      const adapter = yield* makeTestAdapter(wrapperPath, {
        startAcpRuntime,
        stopDrainTimeout: "1 second",
        afterStopTurnTerminal: () =>
          Deferred.succeed(releaseLateEvents, undefined).pipe(
            Effect.andThen(
              Deferred.await(lateEventsHandled).pipe(
                Effect.timeoutOption("1 second"),
                Effect.asVoid,
              ),
            ),
          ),
      });
      const promptStarted = yield* Deferred.make<void>();
      const sessionExited = yield* Deferred.make<void>();
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "content.delta" && event.payload.delta === "prompt reached mock"
              ? Deferred.succeed(promptStarted, undefined).pipe(Effect.ignore)
              : event.type === "session.exited"
                ? Deferred.succeed(sessionExited, undefined).pipe(Effect.ignore)
                : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const send = yield* adapter
        .sendTurn({ threadId, input: "hang before late timeout output", attachments: [] })
        .pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(promptStarted);
      const stopping = yield* adapter.stopSession(threadId).pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(drainStarted);
      yield* TestClock.adjust("1 second");
      yield* Deferred.await(releaseLateEvents);
      yield* TestClock.adjust("1 second");
      const stopExit = yield* Fiber.join(stopping);
      yield* Deferred.await(drainFinalized);
      yield* Deferred.await(sessionExited);
      yield* Fiber.join(send);

      assert.isTrue(Exit.isSuccess(stopExit));
      const completedIndex = runtimeEvents.findIndex((event) => event.type === "turn.completed");
      assert.isAtLeast(completedIndex, 0);
      const completed = runtimeEvents[completedIndex];
      if (completed?.type !== "turn.completed") {
        return assert.fail("expected a turn.completed runtime event");
      }
      const outputTypes = new Set([
        "content.delta",
        "item.started",
        "item.updated",
        "item.completed",
        "turn.plan.updated",
      ]);
      assert.deepEqual(
        runtimeEvents
          .slice(completedIndex + 1)
          .filter((event) => event.turnId === completed.turnId && outputTypes.has(event.type)),
        [],
      );
      assert.deepEqual(
        runtimeEvents.filter(
          (event) =>
            event.type === "content.delta" && event.payload.delta === "late after timed-out drain",
        ),
        [],
      );
      assert.equal(runtimeEvents.filter((event) => event.type === "turn.completed").length, 1);
      assert.equal(runtimeEvents.filter((event) => event.type === "session.exited").length, 1);
      assert.isFalse(yield* adapter.hasSession(threadId));
      assert.deepStrictEqual(yield* adapter.listSessions(), []);
      assert.isFalse(yield* Deferred.isDone(lateEventsHandled));
      assert.include(yield* Effect.promise(() => NodeFSP.readFile(exitLogPath, "utf8")), "exit:");
      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("finishes owned stop cleanup when the stop caller is interrupted", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-stop-caller-interrupted");
      const wrapperPath = yield* Effect.promise(() => makeMockKiloWrapper());
      const stopClaimed = yield* Deferred.make<void>();
      const releaseStopCleanup = yield* Deferred.make<void>();
      const sessionExited = yield* Deferred.make<void>();
      const adapter = yield* makeTestAdapter(wrapperPath, {
        afterSessionStopClaim: () =>
          Deferred.succeed(stopClaimed, undefined).pipe(
            Effect.andThen(Deferred.await(releaseStopCleanup)),
          ),
      });
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "session.exited"
              ? Deferred.succeed(sessionExited, undefined).pipe(Effect.ignore)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const stopping = yield* adapter
        .stopSession(threadId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(stopClaimed);
      assert.isFalse(yield* adapter.hasSession(threadId));
      const interrupting = yield* Fiber.interrupt(stopping).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.succeed(releaseStopCleanup, undefined);
      yield* Fiber.join(interrupting);
      yield* Deferred.await(sessionExited);

      assert.deepStrictEqual(yield* adapter.listSessions(), []);
      assert.equal(runtimeEvents.filter((event) => event.type === "session.exited").length, 1);
      yield* adapter.stopAll();
      assert.equal(runtimeEvents.filter((event) => event.type === "session.exited").length, 1);
      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("stopAll cancels a session startup already in progress", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-stop-all-during-start");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-stop-all-start-")),
      );
      const markerPath = NodePath.join(tempDir, "startup-config-started");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_SET_CONFIG_DELAY_MS: "250",
          T3_ACP_SET_CONFIG_DELAY_AFTER: "0",
          T3_ACP_SET_CONFIG_MARKER_PATH: markerPath,
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)),
      ).pipe(Effect.forkChild);

      const starting = yield* adapter
        .startSession({
          threadId,
          provider: ProviderDriverKind.make("kilo"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
        })
        .pipe(Effect.exit, Effect.forkChild);
      yield* waitForPathCreation(markerPath);
      yield* adapter.stopAll();
      const startExit = yield* Fiber.join(starting);

      assert.isTrue(Exit.isFailure(startExit));
      if (Exit.isFailure(startExit)) {
        assert.include(String(Cause.squash(startExit.cause)), "startup was cancelled");
      }
      assert.isFalse(yield* adapter.hasSession(threadId));
      assert.deepStrictEqual(yield* adapter.listSessions(), []);
      assert.equal(runtimeEvents.filter((event) => event.type === "session.started").length, 0);
      assert.equal(runtimeEvents.filter((event) => event.type === "session.exited").length, 0);
      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("closes provisional startup ownership when interrupted before startup events", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-start-interrupted-before-events");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-start-event-interrupt-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({ T3_ACP_EXIT_LOG_PATH: exitLogPath }),
      );
      const startupRegistered = yield* Deferred.make<void>();
      const holdStartupEvents = yield* Deferred.make<void>();
      const adapter = yield* makeTestAdapter(wrapperPath, {
        beforeStartupEvents: () =>
          Deferred.succeed(startupRegistered, undefined).pipe(
            Effect.andThen(Deferred.await(holdStartupEvents)),
          ),
      });
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)),
      ).pipe(Effect.forkChild);

      const starting = yield* adapter
        .startSession({
          threadId,
          provider: ProviderDriverKind.make("kilo"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
        })
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(startupRegistered);
      assert.isTrue(yield* adapter.hasSession(threadId));
      yield* Fiber.interrupt(starting);
      const startExit = yield* Fiber.await(starting);

      assert.isTrue(Exit.isFailure(startExit));
      if (Exit.isFailure(startExit)) {
        assert.isTrue(Cause.hasInterrupts(startExit.cause));
      }
      assert.isFalse(yield* adapter.hasSession(threadId));
      assert.deepStrictEqual(yield* adapter.listSessions(), []);
      assert.equal(runtimeEvents.filter((event) => event.type === "session.started").length, 0);
      assert.equal(runtimeEvents.filter((event) => event.type === "thread.started").length, 0);
      assert.include(yield* Effect.promise(() => NodeFSP.readFile(exitLogPath, "utf8")), "exit:");

      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("publishes no startup lifecycle when a later event stamp cannot be generated", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-start-event-stamp-failure");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-start-stamp-failure-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({ T3_ACP_EXIT_LOG_PATH: exitLogPath }),
      );
      const crypto = yield* Crypto.Crypto;
      let failStartupEventStamps = false;
      let startupEventStampCalls = 0;
      const failingCrypto = Crypto.make({
        randomBytes: (size) => {
          if (failStartupEventStamps) {
            startupEventStampCalls += 1;
            if (startupEventStampCalls === 2) {
              throw new Error("injected startup event stamp failure");
            }
          }
          return globalThis.crypto.getRandomValues(new Uint8Array(size));
        },
        digest: (algorithm, data) => crypto.digest(algorithm, data),
      });
      const adapter = yield* makeTestAdapter(wrapperPath, {
        beforeStartupEvents: () =>
          Effect.sync(() => {
            failStartupEventStamps = true;
            startupEventStampCalls = 0;
          }),
      }).pipe(Effect.provideService(Crypto.Crypto, failingCrypto));
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)),
      ).pipe(Effect.forkChild);

      const startExit = yield* adapter
        .startSession({
          threadId,
          provider: ProviderDriverKind.make("kilo"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
        })
        .pipe(Effect.exit);

      assert.isTrue(Exit.isFailure(startExit));
      assert.equal(startupEventStampCalls, 2);
      assert.isFalse(yield* adapter.hasSession(threadId));
      assert.deepStrictEqual(yield* adapter.listSessions(), []);
      assert.equal(runtimeEvents.filter((event) => event.type === "session.started").length, 0);
      assert.equal(
        runtimeEvents.filter((event) => event.type === "session.state.changed").length,
        0,
      );
      assert.equal(runtimeEvents.filter((event) => event.type === "thread.started").length, 0);
      assert.include(yield* Effect.promise(() => NodeFSP.readFile(exitLogPath, "utf8")), "exit:");

      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("serializes stopSession with stopAll and emits one terminal session event", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-stop-all-stop-session-race");
      const wrapperPath = yield* Effect.promise(() => makeMockKiloWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);
      const runtimeEvents: ProviderRuntimeEvent[] = [];
      const sessionExited = yield* Deferred.make<void>();
      const eventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => runtimeEvents.push(event)).pipe(
          Effect.andThen(
            event.type === "session.exited"
              ? Deferred.succeed(sessionExited, undefined).pipe(Effect.ignore)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* Effect.all(
        [adapter.stopSession(threadId).pipe(Effect.exit), adapter.stopAll().pipe(Effect.exit)],
        {
          concurrency: "unbounded",
        },
      );
      yield* Deferred.await(sessionExited);

      assert.equal(runtimeEvents.filter((event) => event.type === "session.exited").length, 1);
      assert.isFalse(yield* adapter.hasSession(threadId));
      yield* Fiber.interrupt(eventsFiber);
    }),
  );

  it.effect("force-kills the exact scoped child when it ignores SIGTERM", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-force-kill-session-close");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-force-kill-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_EXIT_LOG_PATH: exitLogPath,
          T3_ACP_IGNORE_SIGTERM: "1",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      yield* adapter.stopSession(threadId).pipe(TestClock.withLive);
      assert.isFalse(yield* adapter.hasSession(threadId));
      assert.include(yield* Effect.promise(() => NodeFSP.readFile(exitLogPath, "utf8")), "SIGTERM");
    }),
  );

  it.effect("times out a stuck startup, closes its child, and releases the next startup", () =>
    Effect.gen(function* () {
      const firstThreadId = ThreadId.make("kilo-stuck-startup");
      const secondThreadId = ThreadId.make("kilo-after-stuck-startup");
      const wrapperPath = yield* Effect.promise(() => makeMockKiloWrapper());
      const nodeSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const spawnCount = yield* Ref.make(0);
      const killCount = yield* Ref.make(0);
      const stuckSpawned = yield* Deferred.make<void>();
      const switchingSpawner = ChildProcessSpawner.make((command) =>
        Ref.getAndUpdate(spawnCount, (count) => count + 1).pipe(
          Effect.flatMap((count) => {
            // The first spawn is the cached `kilo --version` guard. Stall the
            // first ACP child, then let the later retry use the real mock.
            if (count !== 1) return nodeSpawner.spawn(command);
            const handle = ChildProcessSpawner.makeHandle({
              pid: ChildProcessSpawner.ProcessId(991),
              exitCode: Effect.never,
              isRunning: Effect.succeed(true),
              kill: () => Ref.update(killCount, (current) => current + 1),
              unref: Effect.succeed(Effect.void),
              stdin: Sink.drain,
              stdout: Stream.never,
              stderr: Stream.never,
              all: Stream.never,
              getInputFd: () => Sink.drain,
              getOutputFd: () => Stream.empty,
            });
            return Deferred.succeed(stuckSpawned, undefined).pipe(
              Effect.andThen(Effect.addFinalizer(() => handle.kill().pipe(Effect.ignore))),
              Effect.as(handle),
            );
          }),
        ),
      );
      const adapter = yield* makeTestAdapter(wrapperPath, { startupTimeout: "50 millis" }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, switchingSpawner),
      );

      const firstStart = yield* Effect.flip(
        adapter.startSession({
          threadId: firstThreadId,
          provider: ProviderDriverKind.make("kilo"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
        }),
      ).pipe(Effect.forkChild);
      yield* Deferred.await(stuckSpawned);
      yield* TestClock.adjust("60 millis");
      const error = yield* Fiber.join(firstStart);
      assert.equal(error._tag, "ProviderAdapterProcessError");
      assert.include(error.message, "did not complete startup and initial configuration");
      assert.equal(yield* Ref.get(killCount), 1);

      const session = yield* adapter.startSession({
        threadId: secondThreadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      assert.equal(session.status, "ready");
      yield* adapter.stopSession(secondThreadId);
    }),
  );

  it.effect("bounds initial configuration and releases concurrent and subsequent stopAll", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-stuck-initial-configuration");
      const tempDir = yield* Effect.promise(() =>
        NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "kilo-initial-config-timeout-")),
      );
      const exitLogPath = NodePath.join(tempDir, "exit.log");
      const initialConfigRequested = yield* Deferred.make<void>();
      const wrapperPath = yield* Effect.promise(() =>
        makeMockKiloWrapper({
          T3_ACP_EXIT_LOG_PATH: exitLogPath,
          T3_ACP_HANG_SET_CONFIG_OPTION: "1",
        }),
      );
      const adapter = yield* makeTestAdapter(wrapperPath, {
        startupTimeout: "1 second",
        nativeEventLogger: {
          filePath: "memory://kilo-initial-config-timeout",
          write: (record: unknown) =>
            JSON.stringify(record).includes("session/set_config_option")
              ? Deferred.succeed(initialConfigRequested, undefined).pipe(Effect.asVoid)
              : Effect.void,
          close: () => Effect.void,
        },
      });

      const starting = yield* Effect.flip(
        adapter.startSession({
          threadId,
          provider: ProviderDriverKind.make("kilo"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
        }),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(initialConfigRequested);
      const concurrentStopAll = yield* adapter
        .stopAll()
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
      yield* TestClock.adjust("1100 millis");

      const error = yield* Fiber.join(starting);
      const concurrentStopExit = yield* Fiber.join(concurrentStopAll);
      assert.equal(error._tag, "ProviderAdapterProcessError");
      assert.isTrue(
        error.message.includes("did not complete startup and initial configuration") ||
          error.message.includes(
            "startup was cancelled because all provider sessions are stopping",
          ),
      );
      assert.isTrue(Exit.isSuccess(concurrentStopExit));
      assert.deepStrictEqual(yield* adapter.listSessions(), []);
      assert.isFalse(yield* adapter.hasSession(threadId));
      assert.include(yield* Effect.promise(() => NodeFSP.readFile(exitLogPath, "utf8")), "exit:");

      yield* adapter.stopAll();
      assert.deepStrictEqual(yield* adapter.listSessions(), []);
    }),
  );

  it.effect("rejects rollback instead of desynchronizing the ACP session", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("kilo-rollback-unsupported");
      const wrapperPath = yield* Effect.promise(() => makeMockKiloWrapper());
      const adapter = yield* makeTestAdapter(wrapperPath);

      yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("kilo"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      const result = yield* adapter.rollbackThread(threadId, 1).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(result));
    }),
  );
});

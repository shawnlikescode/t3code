/**
 * KiloAdapterLive — Kilo CLI (`kilo acp`) via ACP.
 *
 * @module KiloAdapterLive
 */

import {
  ApprovalRequestId,
  type KiloSettings,
  EventId,
  type ProviderApprovalDecision,
  type ProviderInteractionMode,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeRequestId,
  type ThreadId,
  TrimmedNonEmptyString,
  TurnId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Crypto from "effect/Crypto";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { mapAcpToAdapterError } from "../acp/AcpAdapterSupport.ts";
import type * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpPlanUpdatedEvent,
  makeAcpRequestOpenedEvent,
  makeAcpRequestResolvedEvent,
  makeAcpToolCallEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import { type AcpSessionModeState, parsePermissionRequest } from "../acp/AcpRuntimeModel.ts";
import { makeAcpNativeLoggerFactory } from "../acp/AcpNativeLogging.ts";
import {
  applyKiloAcpModelSelection,
  buildKiloChildAgentPolicyEnvironment,
  enforceKiloInteractiveModeEnvironment,
  startKiloAcpRuntime,
} from "../acp/KiloAcpSupport.ts";
import { type KiloAdapterShape } from "../Services/KiloAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";
import {
  isSupportedKiloVersion,
  MINIMUM_SUPPORTED_KILO_VERSION,
  parseKiloCliVersion,
  runKiloVersionCommand,
} from "./KiloProvider.ts";
const encodeUnknownJsonStringExit = Schema.encodeUnknownExit(Schema.fromJsonString(Schema.Unknown));

const PROVIDER = ProviderDriverKind.make("kilo");
const KILO_RESUME_VERSION = 1 as const;
const KILO_USER_SESSION_STARTUP_TIMEOUT = Duration.seconds(30);
const KILO_STOP_EVENT_DRAIN_TIMEOUT = Duration.seconds(1);
const KILO_PERMISSION_IDENTITY_MAX_BYTES = 256 * 1024;
const permissionIdentityEncoder = new TextEncoder();

type KiloVersionCheckOutcome =
  | { readonly ok: true }
  | { readonly ok: false; readonly detail: string; readonly cause?: unknown };

function encodeJsonStringForDiagnostics(input: unknown): string | undefined {
  const result = encodeUnknownJsonStringExit(input);
  return Exit.isSuccess(result) ? result.value : undefined;
}

interface CanonicalJsonBudget {
  remaining: number;
}

/**
 * Canonically serializes decoded JSON-RPC values (sorted keys, no whitespace).
 * Inputs are `JSON.parse` output, so the reachable hazards are depth (nested
 * JSON) and size. The running budget (UTF-16 units, a lower bound on UTF-8
 * bytes) aborts oversized inputs during serialization instead of
 * materializing the full canonical string before the byte check.
 */
function canonicalJson(
  input: unknown,
  depth = 0,
  budget: CanonicalJsonBudget = { remaining: KILO_PERMISSION_IDENTITY_MAX_BYTES },
): string | undefined {
  const charge = (piece: string): string | undefined => {
    budget.remaining -= piece.length;
    return budget.remaining < 0 ? undefined : piece;
  };
  if (input === null) return charge("null");
  if (typeof input === "string" || typeof input === "boolean") {
    return charge(JSON.stringify(input));
  }
  if (typeof input === "number") {
    return Number.isFinite(input) ? charge(JSON.stringify(input)) : undefined;
  }
  if (depth >= 32) return undefined;
  if (Array.isArray(input)) {
    const entries: string[] = [];
    for (const entry of input) {
      const value = canonicalJson(entry, depth + 1, budget);
      if (value === undefined) return undefined;
      entries.push(value);
    }
    return `[${entries.join(",")}]`;
  }
  if (!Predicate.isObject(input)) return undefined;
  const entries: string[] = [];
  for (const key of Object.keys(input).sort()) {
    const encodedKey = charge(JSON.stringify(key));
    if (encodedKey === undefined) return undefined;
    const value = canonicalJson(input[key], depth + 1, budget);
    if (value === undefined) return undefined;
    entries.push(`${encodedKey}:${value}`);
  }
  return `{${entries.join(",")}}`;
}

/**
 * Builds the stable portion of Kilo's ACP permission request. `toolCallId`
 * changes per invocation and status/content are lifecycle presentation, while
 * Kilo's exact tool permission is represented by kind, title, raw input, and
 * locations. Missing or malformed identity fields deliberately disable cache.
 */
function permissionIdentityJson(
  request: EffectAcpSchema.RequestPermissionRequest,
): string | undefined {
  const kind = request.toolCall.kind?.trim();
  const title = request.toolCall.title?.trim();
  if (!kind || !title || !Predicate.isObject(request.toolCall.rawInput)) {
    return undefined;
  }
  return canonicalJson({
    kind,
    title,
    rawInput: request.toolCall.rawInput,
    locations: request.toolCall.locations ?? [],
  });
}

const mapHandlerFailure = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.mapError(
      (cause) =>
        new EffectAcpErrors.AcpTransportError({
          detail: "Failed to process Kilo ACP handler.",
          cause,
        }),
    ),
  );

export interface KiloAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  /**
   * Selections are honored when `modelSelection.instanceId` matches this value.
   * Defaults to the legacy built-in instance id (`kilo`).
   */
  readonly instanceId?: ProviderInstanceId;
  /**
   * Optional per-session settings resolver. When provided the adapter yields
   * this effect at the start of every session and uses the result instead of
   * the `kiloSettings` captured at construction.
   *
   * Production instances bind settings to the instance scope (the hydration
   * layer rebuilds the adapter on config change) and leave this undefined.
   * Test suites that mutate `ServerSettingsService` mid-flight pass a resolver
   * that reads the latest snapshot so the closure isn't stale.
   */
  readonly resolveSettings?: Effect.Effect<KiloSettings>;
  /** Overrides the Kilo ACP readiness and initial-configuration deadline. */
  readonly startupTimeout?: Duration.Input;
  /** Overrides the bounded ACP event drain used by focused stop tests. */
  readonly stopDrainTimeout?: Duration.Input;
  /** Injects the keyed lifecycle lock registry for deterministic concurrency tests. */
  readonly threadLockRegistry?: KiloThreadLockRegistry;
  /** Optional synchronization hook after an approval is claimed. Used by focused race tests. */
  readonly afterApprovalClaim?: (requestId: ApprovalRequestId) => Effect.Effect<void>;
  /** Optional synchronization hook after prompt settlement acquires the thread lock. */
  readonly beforePromptSettlement?: (turnId: TurnId) => Effect.Effect<void>;
  /** Optional synchronization hook after a prompt claim. Used by focused interruption tests. */
  readonly afterPromptClaim?: (turnId: TurnId) => Effect.Effect<void>;
  /** Optional synchronization hook after a session stop claim. Used by focused interruption tests. */
  readonly afterSessionStopClaim?: (threadId: ThreadId) => Effect.Effect<void>;
  /** Optional synchronization hook after stop publishes a turn terminal event. Used by focused tests. */
  readonly afterStopTurnTerminal?: (turnId: TurnId) => Effect.Effect<void>;
  /** Optional synchronization hook before startup events publish. Used by focused interruption tests. */
  readonly beforeStartupEvents?: (threadId: ThreadId) => Effect.Effect<void>;
  /** Overrides ACP runtime startup. Used by focused adapter-boundary tests. */
  readonly startAcpRuntime?: typeof startKiloAcpRuntime;
}

interface PendingApproval {
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
  readonly permissionFingerprint: string | undefined;
}

interface KiloSessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly acp: AcpSessionRuntime.AcpSessionRuntime["Service"];
  notificationFiber: Fiber.Fiber<void, never> | undefined;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly sessionApprovedPermissionFingerprints: Set<string>;
  readonly supervisedModeId: string;
  readonly planModeId: string;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  readonly interruptedTurnIds: Set<TurnId>;
  lastPlanFingerprint: string | undefined;
  activeTurnId: TurnId | undefined;
  /** Number of sendTurn prompts currently in flight or being prepared. */
  promptsInFlight: number;
  stopped: boolean;
}

interface KiloThreadLockEntry {
  readonly semaphore: Semaphore.Semaphore;
  readonly users: number;
}

export interface KiloThreadLockRegistry {
  readonly withLock: <A, E, R>(
    threadId: string,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>;
  readonly activeKeyCount: Effect.Effect<number>;
  readonly activeUserCount: Effect.Effect<number>;
}

/**
 * A keyed preparation/settlement lock whose entries live only while a holder
 * or waiter is registered. Registration happens before waiting, so a key is
 * never removed while another operation can still acquire its semaphore.
 */
export const makeKiloThreadLockRegistry: Effect.Effect<KiloThreadLockRegistry> = Effect.gen(
  function* () {
    const entriesRef = yield* SynchronizedRef.make(new Map<string, KiloThreadLockEntry>());

    const register = (threadId: string) =>
      SynchronizedRef.modifyEffect(entriesRef, (current) => {
        const existing = current.get(threadId);
        if (existing !== undefined) {
          const next = new Map(current);
          next.set(threadId, { semaphore: existing.semaphore, users: existing.users + 1 });
          return Effect.succeed([existing.semaphore, next] as const);
        }
        return Semaphore.make(1).pipe(
          Effect.map((semaphore) => {
            const next = new Map(current);
            next.set(threadId, { semaphore, users: 1 });
            return [semaphore, next] as const;
          }),
        );
      });

    const release = (threadId: string, semaphore: Semaphore.Semaphore) =>
      SynchronizedRef.update(entriesRef, (current) => {
        const existing = current.get(threadId);
        if (existing === undefined || existing.semaphore !== semaphore) return current;
        const next = new Map(current);
        if (existing.users === 1) {
          next.delete(threadId);
        } else {
          next.set(threadId, { semaphore, users: existing.users - 1 });
        }
        return next;
      });

    const withLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.acquireUseRelease(
        register(threadId),
        (semaphore) => semaphore.withPermit(effect),
        (semaphore) => release(threadId, semaphore),
      );

    return {
      withLock,
      activeKeyCount: SynchronizedRef.get(entriesRef).pipe(Effect.map((entries) => entries.size)),
      activeUserCount: SynchronizedRef.get(entriesRef).pipe(
        Effect.map((entries) =>
          Array.from(entries.values()).reduce((total, entry) => total + entry.users, 0),
        ),
      ),
    };
  },
);

function settlePendingApprovalsAsCancelled(
  pendingApprovals: Map<ApprovalRequestId, PendingApproval>,
): Effect.Effect<void> {
  return Effect.uninterruptible(
    Effect.suspend(() => {
      const pendingEntries = Array.from(pendingApprovals.values());
      // Claim all requests before the first cooperative yield. A response from
      // another device after this point is stale and cannot populate the
      // session approval cache after cancellation.
      pendingApprovals.clear();
      return Effect.forEach(
        pendingEntries,
        (pending) => Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore),
        { discard: true },
      );
    }),
  );
}

const KiloResumeCursor = Schema.Struct({
  schemaVersion: Schema.Literal(KILO_RESUME_VERSION),
  sessionId: TrimmedNonEmptyString,
});
const decodeKiloResumeCursor = Schema.decodeUnknownOption(KiloResumeCursor);

function parseKiloResume(raw: unknown): { sessionId: string } | undefined {
  return Option.match(decodeKiloResumeCursor(raw), {
    onNone: () => undefined,
    onSome: ({ sessionId }) => ({ sessionId }),
  });
}

/**
 * Every Kilo session runs one of T3's injected nonce-scoped agents, so the
 * requested mode is always an exact injected id. Any fallback to a
 * Kilo-native mode would silently drop T3's child permission policy, so a
 * missing id resolves to `undefined` and callers fail closed.
 */
export function resolveKiloRequestedModeId(input: {
  readonly interactionMode: ProviderInteractionMode | undefined;
  readonly modeState: AcpSessionModeState | undefined;
  readonly supervisedModeId: string;
  readonly planModeId: string;
}): string | undefined {
  const modeState = input.modeState;
  if (!modeState) {
    return undefined;
  }
  const requestedModeId =
    input.interactionMode === "plan" ? input.planModeId : input.supervisedModeId;
  return modeState.availableModes.some((mode) => mode.id === requestedModeId)
    ? requestedModeId
    : undefined;
}

function selectPermissionOptionId(
  request: EffectAcpSchema.RequestPermissionRequest,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): string | undefined {
  const kind =
    decision === "acceptForSession" || decision === "accept" ? "allow_once" : "reject_once";
  const option = request.options.find((entry) => entry.kind === kind);
  return option?.optionId.trim() || undefined;
}

export const makeKiloAdapter = Effect.fn("makeKiloAdapter")(function* (
  kiloSettings: KiloSettings,
  options?: KiloAdapterLiveOptions,
) {
  const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("kilo");
  const adapterScope = yield* Scope.Scope;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const serverConfig = yield* Effect.service(ServerConfig);
  const crypto = yield* Crypto.Crypto;
  const nativeEventLogger =
    options?.nativeEventLogger ??
    (options?.nativeEventLogPath !== undefined
      ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, {
          stream: "native",
        })
      : undefined);
  const managedNativeEventLogger =
    options?.nativeEventLogger === undefined ? nativeEventLogger : undefined;
  const makeAcpNativeLoggers = yield* makeAcpNativeLoggerFactory();
  const startAcpRuntime = options?.startAcpRuntime ?? startKiloAcpRuntime;
  const stopDrainTimeout = Duration.fromInputUnsafe(
    options?.stopDrainTimeout ?? KILO_STOP_EVENT_DRAIN_TIMEOUT,
  );

  const sessions = new Map<ThreadId, KiloSessionContext>();
  const threadLocks = options?.threadLockRegistry ?? (yield* makeKiloThreadLockRegistry);
  const startingSessionCounts = new Map<ThreadId, number>();
  let stopAllGeneration = 0;
  let stopAllInProgressCount = 0;
  const versionChecks = new Map<string, Deferred.Deferred<KiloVersionCheckOutcome>>();
  const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const randomUUIDv4 = crypto.randomUUIDv4.pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "crypto/randomUUIDv4",
          detail: "Failed to generate Kilo runtime identifier.",
          cause,
        }),
    ),
  );
  const nextEventId = Effect.map(randomUUIDv4, (id) => EventId.make(id));
  const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

  const makePermissionFingerprint = (
    request: EffectAcpSchema.RequestPermissionRequest,
  ): Effect.Effect<string | undefined> => {
    const identity = permissionIdentityJson(request);
    if (identity === undefined) {
      return Effect.succeed(undefined);
    }
    const bytes = permissionIdentityEncoder.encode(identity);
    if (bytes.byteLength > KILO_PERMISSION_IDENTITY_MAX_BYTES) {
      return Effect.succeed(undefined);
    }
    return crypto.digest("SHA-256", bytes).pipe(
      Effect.map((digest) =>
        Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join(""),
      ),
      Effect.catchCause((cause) =>
        Effect.logWarning("Failed to fingerprint Kilo ACP permission identity.", {
          cause,
        }).pipe(Effect.as(undefined)),
      ),
    );
  };

  const runKiloVersionCheck = Effect.fn("runKiloVersionCheck")(function* (
    settings: KiloSettings,
  ): Effect.fn.Return<KiloVersionCheckOutcome> {
    const commandExit = yield* runKiloVersionCommand(
      settings,
      options?.environment ?? process.env,
    ).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
      Effect.timeoutOption("4 seconds"),
      Effect.exit,
    );
    if (Exit.isFailure(commandExit)) {
      return {
        ok: false,
        detail: `Failed to verify Kilo CLI version. T3 Code requires Kilo v${MINIMUM_SUPPORTED_KILO_VERSION} or newer.`,
        cause: commandExit.cause,
      };
    }
    if (Option.isNone(commandExit.value)) {
      return {
        ok: false,
        detail: `Timed out while verifying Kilo CLI version. T3 Code requires Kilo v${MINIMUM_SUPPORTED_KILO_VERSION} or newer.`,
      };
    }
    const result = commandExit.value.value;
    const version = parseKiloCliVersion(`${result.stdout}\n${result.stderr}`);
    if (result.code === 0 && isSupportedKiloVersion(version)) {
      return { ok: true };
    }
    const observed = version ? `v${version}` : "an unknown version";
    return {
      ok: false,
      detail: `Kilo ${observed} is unsupported. Upgrade to v${MINIMUM_SUPPORTED_KILO_VERSION} or newer with \`kilo upgrade\` before starting a Kilo thread.`,
    };
  });

  const ensureSupportedKiloVersion = Effect.fn("ensureSupportedKiloVersion")(function* (
    settings: KiloSettings,
    threadId: ThreadId,
  ) {
    const versionCommand = settings.binaryPath || "kilo";
    // One deferred per binary coalesces concurrent probes; a failed probe is
    // evicted before it settles so the next session start retries.
    const deferred = yield* Effect.uninterruptible(
      Effect.suspend(() => {
        const existing = versionChecks.get(versionCommand);
        if (existing) return Effect.succeed(existing);
        const created = Deferred.makeUnsafe<KiloVersionCheckOutcome>();
        versionChecks.set(versionCommand, created);
        return runKiloVersionCheck(settings).pipe(
          Effect.flatMap((outcome) =>
            Effect.sync(() => {
              if (!outcome.ok && versionChecks.get(versionCommand) === created) {
                versionChecks.delete(versionCommand);
              }
            }).pipe(Effect.andThen(Deferred.succeed(created, outcome))),
          ),
          Effect.forkIn(adapterScope),
          Effect.as(created),
        );
      }),
    );
    const outcome = yield* Deferred.await(deferred);
    if (!outcome.ok) {
      return yield* new ProviderAdapterProcessError({
        provider: PROVIDER,
        threadId,
        detail: outcome.detail,
        ...(outcome.cause === undefined ? {} : { cause: outcome.cause }),
      });
    }
  });

  const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
    PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

  const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
    threadLocks.withLock(threadId, effect);

  const registerSessionStart = Effect.fn("registerSessionStart")(function* (threadId: ThreadId) {
    if (stopAllInProgressCount > 0) {
      return yield* new ProviderAdapterProcessError({
        provider: PROVIDER,
        threadId,
        detail: "Kilo sessions are stopping; retry after shutdown finishes.",
      });
    }
    startingSessionCounts.set(threadId, (startingSessionCounts.get(threadId) ?? 0) + 1);
    return stopAllGeneration;
  });

  const unregisterSessionStart = (threadId: ThreadId) =>
    Effect.sync(() => {
      const count = startingSessionCounts.get(threadId);
      if (count === undefined || count <= 1) {
        startingSessionCounts.delete(threadId);
      } else {
        startingSessionCounts.set(threadId, count - 1);
      }
    });

  const ensureSessionStartIsCurrent = Effect.fn("ensureSessionStartIsCurrent")(function* (
    threadId: ThreadId,
    generation: number,
  ) {
    // registerSessionStart rejects while a stopAll is in progress and stopAll
    // bumps the generation in the same synchronous block, so a matching
    // generation proves no stopAll started after registration.
    if (generation === stopAllGeneration) return;
    return yield* new ProviderAdapterProcessError({
      provider: PROVIDER,
      threadId,
      detail: "Kilo session startup was cancelled because all provider sessions are stopping.",
    });
  });

  const logNative = Effect.fn("logNative")(function* (
    threadId: ThreadId,
    method: string,
    payload: unknown,
  ) {
    if (!nativeEventLogger) return;
    const observedAt = yield* nowIso;
    yield* nativeEventLogger.write(
      {
        observedAt,
        event: {
          id: yield* randomUUIDv4,
          kind: "notification",
          provider: PROVIDER,
          createdAt: observedAt,
          method,
          threadId,
          payload,
        },
      },
      threadId,
    );
  });

  const emitPlanUpdate = Effect.fn("emitPlanUpdate")(function* (
    ctx: KiloSessionContext,
    turnId: TurnId,
    payload: {
      readonly explanation?: string | null;
      readonly plan: ReadonlyArray<{
        readonly step: string;
        readonly status: "pending" | "inProgress" | "completed";
      }>;
    },
    rawPayload: unknown,
    method: string,
  ) {
    const fingerprint = `${turnId}:${encodeJsonStringForDiagnostics(payload) ?? "[unserializable payload]"}`;
    if (ctx.lastPlanFingerprint === fingerprint) {
      return;
    }
    ctx.lastPlanFingerprint = fingerprint;
    yield* offerRuntimeEvent(
      makeAcpPlanUpdatedEvent({
        stamp: yield* makeEventStamp(),
        provider: PROVIDER,
        threadId: ctx.threadId,
        turnId,
        payload,
        source: "acp.jsonrpc",
        method,
        rawPayload,
      }),
    );
  });

  const requireSession = (
    threadId: ThreadId,
  ): Effect.Effect<KiloSessionContext, ProviderAdapterSessionNotFoundError> => {
    const ctx = sessions.get(threadId);
    if (!ctx || ctx.stopped) {
      return Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
    }
    return Effect.succeed(ctx);
  };

  /**
   * Settles pending approvals, quiesces the notification consumer, closes the
   * session scope, and forgets the session. Callers own terminal events.
   */
  const releaseSessionResources = Effect.fn("releaseSessionResources")(function* (
    ctx: KiloSessionContext,
  ) {
    yield* settlePendingApprovalsAsCancelled(ctx.pendingApprovals);
    const notificationFiber = ctx.notificationFiber;
    if (notificationFiber) {
      ctx.notificationFiber = undefined;
      yield* Fiber.interrupt(notificationFiber);
    }
    yield* Effect.ignore(Scope.close(ctx.scope, Exit.void));
    sessions.delete(ctx.threadId);
  });

  const stopSessionInternal = Effect.fn("stopSessionInternal")(function* (ctx: KiloSessionContext) {
    yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        const stoppedTurnId = ctx.activeTurnId ?? ctx.session.activeTurnId;
        if (
          stoppedTurnId !== undefined &&
          (ctx.session.status === "running" || ctx.session.status === "connecting")
        ) {
          // Preserve queued deltas/tool updates ahead of the terminal event.
          // Stop owns cleanup once it enters this region. A broken event
          // consumer must not strand the child or session indefinitely.
          yield* ctx.acp.drainEvents.pipe(Effect.timeoutOption(stopDrainTimeout), Effect.exit);

          // Once the drain boundary is known, no later notification belongs to
          // this turn. Quiesce the consumer before publishing the terminal
          // event so even an update that started after a successful barrier, or
          // one already being mapped after a failed drain, cannot overtake
          // cancellation.
          ctx.interruptedTurnIds.add(stoppedTurnId);
          const notificationFiber = ctx.notificationFiber;
          if (notificationFiber) {
            ctx.notificationFiber = undefined;
            yield* Fiber.interrupt(notificationFiber);
          }
          yield* offerRuntimeEvent({
            type: "turn.completed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: ctx.threadId,
            turnId: stoppedTurnId,
            payload: { state: "cancelled", stopReason: "cancelled" },
          });
          yield* options?.afterStopTurnTerminal?.(stoppedTurnId) ?? Effect.void;
        }
        ctx.promptsInFlight = 0;
        ctx.activeTurnId = undefined;
        const { activeTurnId: _activeTurnId, ...stoppedSession } = ctx.session;
        ctx.session = { ...stoppedSession, status: "ready", updatedAt: yield* nowIso };
        ctx.stopped = true;
        yield* options?.afterSessionStopClaim?.(ctx.threadId) ?? Effect.void;
        yield* releaseSessionResources(ctx);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
        // Preserve caller interruption semantics only after owned cleanup and
        // terminal publication are complete.
        yield* restore(Effect.void);
      }),
    );
  });

  const quarantineSessionInternal = Effect.fn("quarantineSessionInternal")(function* (
    ctx: KiloSessionContext,
    errorMessage: string,
  ) {
    if (ctx.stopped) return;
    const stoppedTurnId = ctx.activeTurnId ?? ctx.session.activeTurnId;
    const shouldSettleTurn =
      stoppedTurnId !== undefined &&
      (ctx.session.status === "running" || ctx.session.status === "connecting");

    // Make every local path observe the quarantine before closing the runtime.
    // Cancellation uncertainty means queued remote work must never reuse this
    // child or transition the session back to ready.
    ctx.stopped = true;
    ctx.promptsInFlight = 0;
    ctx.activeTurnId = undefined;
    yield* releaseSessionResources(ctx);

    if (shouldSettleTurn) {
      yield* offerRuntimeEvent({
        type: "turn.completed",
        ...(yield* makeEventStamp()),
        provider: PROVIDER,
        threadId: ctx.threadId,
        turnId: stoppedTurnId,
        payload: { state: "failed", errorMessage },
      });
    }
    yield* offerRuntimeEvent({
      type: "session.exited",
      ...(yield* makeEventStamp()),
      provider: PROVIDER,
      threadId: ctx.threadId,
      payload: { exitKind: "error" },
    });
  });

  const applyRequestedSessionConfiguration = Effect.fn("applyRequestedSessionConfiguration")(
    function* (input: {
      readonly runtime: AcpSessionRuntime.AcpSessionRuntime["Service"];
      readonly threadId: ThreadId;
      readonly interactionMode: ProviderInteractionMode | undefined;
      readonly requestedModelId: string | undefined;
      readonly supervisedModeId: string;
      readonly planModeId: string;
    }) {
      const selectedModel = yield* applyKiloAcpModelSelection({
        runtime: input.runtime,
        requestedModelId: input.requestedModelId,
        mapError: (cause) =>
          mapAcpToAdapterError(PROVIDER, input.threadId, "session/set_config_option", cause),
      });

      const requestedModeId = resolveKiloRequestedModeId({
        interactionMode: input.interactionMode,
        modeState: yield* input.runtime.getModeState,
        supervisedModeId: input.supervisedModeId,
        planModeId: input.planModeId,
      });
      if (!requestedModeId) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/set_mode",
          detail:
            "Kilo did not advertise T3's child-scoped permission agent, so mode guarantees cannot be enforced.",
        });
      }

      yield* input.runtime
        .setMode(requestedModeId)
        .pipe(
          Effect.mapError((cause) =>
            mapAcpToAdapterError(PROVIDER, input.threadId, "session/set_mode", cause),
          ),
        );
      return selectedModel;
    },
  );

  const startSession: KiloAdapterShape["startSession"] = (input) =>
    Effect.acquireUseRelease(
      registerSessionStart(input.threadId),
      (startGeneration) =>
        withThreadLock(
          input.threadId,
          Effect.gen(function* () {
            yield* ensureSessionStartIsCurrent(input.threadId, startGeneration);
            if (input.provider !== PROVIDER) {
              return yield* new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "startSession",
                issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
              });
            }
            if (!input.cwd?.trim()) {
              return yield* new ProviderAdapterValidationError({
                provider: PROVIDER,
                operation: "startSession",
                issue: "cwd is required and must be non-empty.",
              });
            }

            const cwd = path.resolve(input.cwd.trim());
            const kiloModelSelection =
              input.modelSelection?.instanceId === boundInstanceId
                ? input.modelSelection
                : undefined;
            const existing = sessions.get(input.threadId);
            if (existing && !existing.stopped) {
              yield* stopSessionInternal(existing);
            }

            const pendingApprovals = new Map<ApprovalRequestId, PendingApproval>();
            const sessionApprovedPermissionFingerprints = new Set<string>();
            const sessionScope = yield* Scope.make("sequential");
            // Assigned after ACP startup; the permission handler can run first
            // and treats the unassigned state as "no active turn".
            let ctx: KiloSessionContext | undefined;
            let sessionScopeTransferred = false;
            yield* Effect.addFinalizer(() =>
              sessionScopeTransferred
                ? Effect.void
                : Effect.sync(() => {
                    if (sessions.get(input.threadId) === ctx) {
                      sessions.delete(input.threadId);
                    }
                  }).pipe(Effect.andThen(Scope.close(sessionScope, Exit.void))),
            );

            const resumeSessionId = parseKiloResume(input.resumeCursor)?.sessionId;
            const acpNativeLoggers = makeAcpNativeLoggers({
              nativeEventLogger,
              provider: PROVIDER,
              threadId: input.threadId,
            });

            const effectiveKiloSettings = options?.resolveSettings
              ? yield* options.resolveSettings
              : kiloSettings;
            yield* ensureSupportedKiloVersion(effectiveKiloSettings, input.threadId);
            const baseEnvironment = enforceKiloInteractiveModeEnvironment(
              options?.environment ?? process.env,
              input.runtimeMode,
            );
            const supervisedPolicy = buildKiloChildAgentPolicyEnvironment({
              environment: baseEnvironment,
              nonce: yield* randomUUIDv4,
              policy: input.runtimeMode === "full-access" ? "full-access" : "ask",
              label:
                input.runtimeMode === "full-access" ? "T3 full access code" : "T3 supervised code",
              prompt:
                input.runtimeMode === "full-access"
                  ? "You are a coding agent running in T3 Code full-access mode. Implement the user's request."
                  : "You are a coding agent supervised by T3 Code. Use tools as needed, but every tool permission is decided by the T3 client.",
            });
            if (!supervisedPolicy.ok) {
              return yield* new ProviderAdapterProcessError({
                provider: PROVIDER,
                threadId: input.threadId,
                detail: supervisedPolicy.message,
              });
            }
            const supervisedModeId = supervisedPolicy.modeId;
            const planPolicy = buildKiloChildAgentPolicyEnvironment({
              environment: supervisedPolicy.environment,
              nonce: `${yield* randomUUIDv4}-plan`,
              policy: "plan",
              label: "T3 read-only plan",
              prompt:
                "Create and explain a read-only implementation plan. Do not modify files, execute commands, ask questions, or delegate tasks.",
            });
            if (!planPolicy.ok) {
              return yield* new ProviderAdapterProcessError({
                provider: PROVIDER,
                threadId: input.threadId,
                detail: planPolicy.message,
              });
            }
            const planModeId = planPolicy.modeId;
            const childEnvironment = planPolicy.environment;

            const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
            const startupTimeout = Duration.fromInputUnsafe(
              options?.startupTimeout ?? KILO_USER_SESSION_STARTUP_TIMEOUT,
            );
            const startup = yield* startAcpRuntime(
              {
                kiloSettings: effectiveKiloSettings,
                ...(childEnvironment ? { environment: childEnvironment } : {}),
                childProcessSpawner,
                cwd,
                ...(resumeSessionId ? { resumeSessionId } : {}),
                clientInfo: { name: "t3-code", version: "0.0.0" },
                ...(mcpSession
                  ? {
                      mcpServers: [
                        {
                          type: "http" as const,
                          name: "t3-code",
                          url: mcpSession.endpoint,
                          headers: [
                            {
                              name: "Authorization",
                              value: mcpSession.authorizationHeader,
                            },
                          ],
                        },
                      ],
                    }
                  : {}),
                ...acpNativeLoggers,
              },
              (acp) =>
                acp.handleRequestPermission((params) =>
                  mapHandlerFailure(
                    Effect.gen(function* () {
                      yield* logNative(input.threadId, "session/request_permission", params);
                      const permissionRequest = parsePermissionRequest(params);
                      const permissionFingerprint = yield* makePermissionFingerprint(params);
                      const registration = yield* withThreadLock(
                        input.threadId,
                        Effect.gen(function* () {
                          const turnId = ctx?.activeTurnId ?? ctx?.session.activeTurnId;
                          if (
                            ctx === undefined ||
                            sessions.get(input.threadId) !== ctx ||
                            ctx.stopped ||
                            turnId === undefined ||
                            ctx.interruptedTurnIds.has(turnId)
                          ) {
                            return {
                              _tag: "Resolved" as const,
                              response: { outcome: { outcome: "cancelled" as const } },
                            };
                          }

                          const autoApprove =
                            input.runtimeMode === "full-access" ||
                            (input.runtimeMode === "auto-accept-edits" &&
                              permissionRequest.kind === "edit" &&
                              permissionFingerprint !== undefined) ||
                            (permissionFingerprint !== undefined &&
                              sessionApprovedPermissionFingerprints.has(permissionFingerprint));
                          if (autoApprove) {
                            const autoApprovedOptionId = selectPermissionOptionId(params, "accept");
                            if (autoApprovedOptionId !== undefined) {
                              return {
                                _tag: "Resolved" as const,
                                response: {
                                  outcome: {
                                    outcome: "selected" as const,
                                    optionId: autoApprovedOptionId,
                                  },
                                },
                              };
                            }
                          }

                          const requestId = ApprovalRequestId.make(yield* randomUUIDv4);
                          const runtimeRequestId = RuntimeRequestId.make(requestId);
                          const decision = yield* Deferred.make<ProviderApprovalDecision>();
                          pendingApprovals.set(requestId, {
                            decision,
                            permissionFingerprint,
                          });
                          yield* offerRuntimeEvent(
                            makeAcpRequestOpenedEvent({
                              stamp: yield* makeEventStamp(),
                              provider: PROVIDER,
                              threadId: input.threadId,
                              turnId,
                              requestId: runtimeRequestId,
                              permissionRequest,
                              detail:
                                permissionRequest.detail ??
                                encodeJsonStringForDiagnostics(params)?.slice(0, 2000) ??
                                "[unserializable params]",
                              args: params,
                              source: "acp.jsonrpc",
                              method: "session/request_permission",
                              rawPayload: params,
                            }),
                          );
                          return {
                            _tag: "Pending" as const,
                            decision,
                            runtimeRequestId,
                            turnId,
                          };
                        }),
                      );
                      if (registration._tag === "Resolved") {
                        return registration.response;
                      }

                      const resolved = yield* Deferred.await(registration.decision);
                      yield* offerRuntimeEvent(
                        makeAcpRequestResolvedEvent({
                          stamp: yield* makeEventStamp(),
                          provider: PROVIDER,
                          threadId: input.threadId,
                          turnId: registration.turnId,
                          requestId: registration.runtimeRequestId,
                          permissionRequest,
                          decision: resolved,
                        }),
                      );
                      const selectedOptionId =
                        resolved === "cancel"
                          ? undefined
                          : selectPermissionOptionId(params, resolved);
                      return {
                        outcome: selectedOptionId
                          ? {
                              outcome: "selected" as const,
                              optionId: selectedOptionId,
                            }
                          : ({ outcome: "cancelled" } as const),
                      };
                    }),
                  ),
                ),
            ).pipe(
              Effect.provideService(Crypto.Crypto, crypto),
              Effect.provideService(Scope.Scope, sessionScope),
              Effect.mapError((error) =>
                mapAcpToAdapterError(PROVIDER, input.threadId, "session/start", error),
              ),
              Effect.flatMap(({ runtime: acp, started }) =>
                applyRequestedSessionConfiguration({
                  runtime: acp,
                  threadId: input.threadId,
                  interactionMode: undefined,
                  requestedModelId: kiloModelSelection?.model,
                  supervisedModeId,
                  planModeId,
                }).pipe(
                  Effect.map((configuredModel) => ({
                    runtime: acp,
                    started,
                    configuredModel,
                  })),
                ),
              ),
              Effect.timeoutOption(startupTimeout),
              Effect.flatMap(
                Option.match({
                  onNone: () =>
                    Effect.fail(
                      new ProviderAdapterProcessError({
                        provider: PROVIDER,
                        threadId: input.threadId,
                        detail: `Kilo ACP did not complete startup and initial configuration within ${Duration.format(startupTimeout)}. Check that \`kilo acp\` starts successfully and that no other Kilo startup is stuck, then retry.`,
                      }),
                    ),
                  onSome: Effect.succeed,
                }),
              ),
              // On failure the scoped finalizer above closes sessionScope.
            );
            const { runtime: acp, started, configuredModel } = startup;

            const now = yield* nowIso;
            const session: ProviderSession = {
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              status: "ready",
              runtimeMode: input.runtimeMode,
              cwd,
              model: configuredModel,
              threadId: input.threadId,
              resumeCursor: {
                schemaVersion: KILO_RESUME_VERSION,
                sessionId: started.sessionId,
              },
              createdAt: now,
              updatedAt: now,
            };

            const sessionCtx: KiloSessionContext = {
              threadId: input.threadId,
              session,
              scope: sessionScope,
              acp,
              notificationFiber: undefined,
              pendingApprovals,
              sessionApprovedPermissionFingerprints,
              supervisedModeId,
              planModeId,
              turns: [],
              interruptedTurnIds: new Set(),
              lastPlanFingerprint: undefined,
              activeTurnId: undefined,
              promptsInFlight: 0,
              stopped: false,
            };
            ctx = sessionCtx;

            const nf = yield* Stream.runDrain(
              Stream.mapEffect(acp.getEvents(), (event) =>
                Effect.gen(function* () {
                  if (event._tag === "EventStreamBarrier") {
                    yield* Deferred.succeed(event.acknowledge, undefined);
                    return;
                  }
                  if (
                    event._tag === "PlanUpdated" ||
                    event._tag === "ToolCallUpdated" ||
                    event._tag === "ContentDelta"
                  ) {
                    yield* logNative(sessionCtx.threadId, "session/update", event.rawPayload);
                  }
                  if (event._tag === "ModeChanged") return;

                  // ACP session notifications do not carry a T3 turn id. Drop
                  // output when no turn owns the stream, or while cancellation
                  // is settling, instead of attaching stale output to mutable
                  // session state after a terminal event.
                  const notificationTurnId = sessionCtx.activeTurnId;
                  if (
                    notificationTurnId === undefined ||
                    sessionCtx.interruptedTurnIds.has(notificationTurnId)
                  ) {
                    return;
                  }

                  switch (event._tag) {
                    case "AssistantItemStarted":
                      yield* offerRuntimeEvent(
                        makeAcpAssistantItemEvent({
                          stamp: yield* makeEventStamp(),
                          provider: PROVIDER,
                          threadId: sessionCtx.threadId,
                          turnId: notificationTurnId,
                          itemId: event.itemId,
                          lifecycle: "item.started",
                        }),
                      );
                      return;
                    case "AssistantItemCompleted":
                      yield* offerRuntimeEvent(
                        makeAcpAssistantItemEvent({
                          stamp: yield* makeEventStamp(),
                          provider: PROVIDER,
                          threadId: sessionCtx.threadId,
                          turnId: notificationTurnId,
                          itemId: event.itemId,
                          lifecycle: "item.completed",
                        }),
                      );
                      return;
                    case "PlanUpdated":
                      yield* emitPlanUpdate(
                        sessionCtx,
                        notificationTurnId,
                        event.payload,
                        event.rawPayload,
                        "session/update",
                      );
                      return;
                    case "ToolCallUpdated":
                      yield* offerRuntimeEvent(
                        makeAcpToolCallEvent({
                          stamp: yield* makeEventStamp(),
                          provider: PROVIDER,
                          threadId: sessionCtx.threadId,
                          turnId: notificationTurnId,
                          toolCall: event.toolCall,
                          rawPayload: event.rawPayload,
                        }),
                      );
                      return;
                    case "ContentDelta":
                      yield* offerRuntimeEvent(
                        makeAcpContentDeltaEvent({
                          stamp: yield* makeEventStamp(),
                          provider: PROVIDER,
                          threadId: sessionCtx.threadId,
                          turnId: notificationTurnId,
                          ...(event.itemId ? { itemId: event.itemId } : {}),
                          text: event.text,
                          rawPayload: event.rawPayload,
                        }),
                      );
                      return;
                  }
                }).pipe(
                  Effect.catchCause((cause) =>
                    Cause.hasInterrupts(cause)
                      ? Effect.interrupt
                      : Effect.logError("Failed to process Kilo runtime notification.", {
                          cause,
                          eventTag: event._tag,
                        }),
                  ),
                ),
              ),
            ).pipe(Effect.forkIn(sessionCtx.scope));

            yield* ensureSessionStartIsCurrent(input.threadId, startGeneration);
            sessionCtx.notificationFiber = nf;
            sessions.set(input.threadId, sessionCtx);
            yield* options?.beforeStartupEvents?.(input.threadId) ?? Effect.void;
            const sessionStartedStamp = yield* makeEventStamp();
            const sessionReadyStamp = yield* makeEventStamp();
            const threadStartedStamp = yield* makeEventStamp();

            yield* Effect.uninterruptible(
              Effect.gen(function* () {
                yield* offerRuntimeEvent({
                  type: "session.started",
                  ...sessionStartedStamp,
                  provider: PROVIDER,
                  threadId: input.threadId,
                  payload: { resume: started.initializeResult },
                });
                yield* offerRuntimeEvent({
                  type: "session.state.changed",
                  ...sessionReadyStamp,
                  provider: PROVIDER,
                  threadId: input.threadId,
                  payload: { state: "ready", reason: "Kilo ACP session ready" },
                });
                yield* offerRuntimeEvent({
                  type: "thread.started",
                  ...threadStartedStamp,
                  provider: PROVIDER,
                  threadId: input.threadId,
                  payload: { providerThreadId: started.sessionId },
                });
                sessionScopeTransferred = true;
              }),
            );

            return session;
          }).pipe(Effect.scoped),
        ),
      () => unregisterSessionStart(input.threadId),
    );

  const releasePreparedPrompt = (ctx: KiloSessionContext, turnId: TurnId) =>
    Effect.sync(() => {
      ctx.promptsInFlight = Math.max(0, ctx.promptsInFlight - 1);
      if (ctx.promptsInFlight !== 0 || ctx.activeTurnId !== turnId) {
        return;
      }
      ctx.activeTurnId = undefined;
      const { activeTurnId: _activeTurnId, ...readySession } = ctx.session;
      ctx.session = { ...readySession, status: "ready" };
    });

  /** Must run while holding the thread preparation/settlement lock. */
  const settlePromptInFlight = Effect.fn("settlePromptInFlight")(function* (
    ctx: KiloSessionContext,
    turnId: TurnId,
    options: {
      readonly errorMessage?: string;
      readonly stopReason?: EffectAcpSchema.StopReason | null;
      readonly emitTurnCompletion?: boolean;
      readonly settleAllPrompts?: boolean;
    },
  ) {
    const liveCtx = sessions.get(ctx.threadId);
    if (liveCtx !== ctx || ctx.stopped) return;
    if (ctx.activeTurnId !== turnId && ctx.session.activeTurnId !== turnId) {
      return;
    }

    if (options.settleAllPrompts) {
      ctx.promptsInFlight = 0;
    } else {
      ctx.promptsInFlight = Math.max(0, ctx.promptsInFlight - 1);
      if (ctx.promptsInFlight > 0) return;
    }

    const canEmit = ctx.session.status === "running" || ctx.session.status === "connecting";
    ctx.activeTurnId = undefined;
    const { activeTurnId: _activeTurnId, ...readySession } = ctx.session;
    ctx.session = { ...readySession, status: "ready", updatedAt: yield* nowIso };

    if (options.emitTurnCompletion === false || !canEmit) return;
    if (options.errorMessage !== undefined) {
      yield* offerRuntimeEvent({
        type: "turn.completed",
        ...(yield* makeEventStamp()),
        provider: PROVIDER,
        threadId: ctx.threadId,
        turnId,
        payload: { state: "failed", errorMessage: options.errorMessage },
      });
    } else if (options.stopReason !== undefined) {
      yield* offerRuntimeEvent({
        type: "turn.completed",
        ...(yield* makeEventStamp()),
        provider: PROVIDER,
        threadId: ctx.threadId,
        turnId,
        payload: {
          state: options.stopReason === "cancelled" ? "cancelled" : "completed",
          stopReason: options.stopReason,
        },
      });
    }
    ctx.interruptedTurnIds.delete(turnId);
  });

  const sendTurn: KiloAdapterShape["sendTurn"] = Effect.fn("sendTurn")(function* (input) {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const normalizedText = input.input?.trim();
        if (!normalizedText && (input.attachments?.length ?? 0) === 0) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Turn requires non-empty text or attachments.",
          });
        }
        const promptSettled = yield* Ref.make(false);
        const promptFailureMessage = yield* Ref.make<string | undefined>(undefined);
        const prepared = yield* restore(
          withThreadLock(
            input.threadId,
            Effect.uninterruptibleMask((restorePreparation) =>
              Effect.gen(function* () {
                const ctx = yield* requireSession(input.threadId);
                // A sendTurn while a prompt is in flight is a steer. Bind and
                // count it before any cooperative yield so concurrent clients see
                // one shared active turn.
                const steeringTurnId = ctx.promptsInFlight > 0 ? ctx.activeTurnId : undefined;
                const turnId = steeringTurnId ?? TurnId.make(yield* randomUUIDv4);
                const updatedAt = yield* nowIso;
                ctx.promptsInFlight += 1;
                ctx.activeTurnId = turnId;
                ctx.session = {
                  ...ctx.session,
                  status: steeringTurnId === undefined ? "connecting" : "running",
                  activeTurnId: turnId,
                  updatedAt,
                };

                return yield* restorePreparation(
                  Effect.gen(function* () {
                    yield* options?.afterPromptClaim?.(turnId) ?? Effect.void;
                    const turnModelSelection =
                      input.modelSelection?.instanceId === boundInstanceId
                        ? input.modelSelection
                        : undefined;
                    const model = turnModelSelection?.model ?? ctx.session.model;
                    const configuredModel = yield* applyRequestedSessionConfiguration({
                      runtime: ctx.acp,
                      threadId: input.threadId,
                      interactionMode: input.interactionMode,
                      requestedModelId: model,
                      supervisedModeId: ctx.supervisedModeId,
                      planModeId: ctx.planModeId,
                    });
                    const activeModel = configuredModel ?? model;

                    const promptParts: Array<EffectAcpSchema.ContentBlock> = [];
                    if (normalizedText) {
                      promptParts.push({ type: "text", text: normalizedText });
                    }
                    const attachmentParts = yield* Effect.forEach(
                      input.attachments ?? [],
                      (attachment) =>
                        Effect.gen(function* () {
                          const attachmentPath = resolveAttachmentPath({
                            attachmentsDir: serverConfig.attachmentsDir,
                            attachment,
                          });
                          if (!attachmentPath) {
                            return yield* new ProviderAdapterRequestError({
                              provider: PROVIDER,
                              method: "session/prompt",
                              detail: `Invalid attachment id '${attachment.id}'.`,
                            });
                          }
                          const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
                            Effect.mapError(
                              (cause) =>
                                new ProviderAdapterRequestError({
                                  provider: PROVIDER,
                                  method: "session/prompt",
                                  detail: "Failed to read attachment file.",
                                  cause,
                                }),
                            ),
                          );
                          return {
                            type: "image" as const,
                            data: Buffer.from(
                              bytes.buffer,
                              bytes.byteOffset,
                              bytes.byteLength,
                            ).toString("base64"),
                            mimeType: attachment.mimeType,
                          };
                        }),
                      { concurrency: 4 },
                    );
                    promptParts.push(...attachmentParts);

                    // An interrupt that observed this preparation is caught
                    // either here or by the shouldStart revalidation under the
                    // runtime serialization permit.
                    if (ctx.interruptedTurnIds.has(turnId)) {
                      return yield* new ProviderAdapterRequestError({
                        provider: PROVIDER,
                        method: "session/prompt",
                        detail: "Kilo prompt was interrupted during preparation.",
                      });
                    }

                    if (steeringTurnId === undefined) {
                      ctx.lastPlanFingerprint = undefined;
                      yield* offerRuntimeEvent({
                        type: "turn.started",
                        ...(yield* makeEventStamp()),
                        provider: PROVIDER,
                        threadId: input.threadId,
                        turnId,
                        payload: { model: activeModel },
                      });
                    }
                    ctx.session = {
                      ...ctx.session,
                      model: activeModel,
                      status: "running",
                      activeTurnId: turnId,
                      updatedAt: yield* nowIso,
                    };

                    return { acp: ctx.acp, ctx, model: activeModel, promptParts, turnId };
                  }),
                ).pipe(
                  Effect.onExit((exit) =>
                    Exit.isSuccess(exit) ? Effect.void : releasePreparedPrompt(ctx, turnId),
                  ),
                );
              }),
            ),
          ),
        );

        return yield* restore(
          Effect.gen(function* () {
            const result = yield* prepared.acp
              .prompt(
                { prompt: prepared.promptParts },
                {
                  shouldStart: Effect.sync(() => {
                    const ctx = sessions.get(input.threadId);
                    return (
                      ctx === prepared.ctx &&
                      !ctx.stopped &&
                      ctx.activeTurnId === prepared.turnId &&
                      ctx.session.activeTurnId === prepared.turnId &&
                      ctx.promptsInFlight > 0 &&
                      !ctx.interruptedTurnIds.has(prepared.turnId)
                    );
                  }),
                },
              )
              .pipe(
                Effect.mapError((error) =>
                  mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", error),
                ),
                Effect.tapError((error) => Ref.set(promptFailureMessage, error.message)),
              );

            return yield* withThreadLock(
              input.threadId,
              Effect.gen(function* () {
                yield* options?.beforePromptSettlement?.(prepared.turnId) ?? Effect.void;
                const ctx = sessions.get(input.threadId);
                if (ctx !== prepared.ctx || ctx.stopped) {
                  yield* Ref.set(promptSettled, true);
                  return yield* new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "session/prompt",
                    detail: "Kilo session changed before the turn completed.",
                  });
                }

                // The event stream carries a barrier acknowledged by the adapter
                // consumer. Drain it while holding the thread lock so final tool
                // updates and deltas retain this turn id and land before completion
                // or any next-turn preparation.
                yield* prepared.acp.drainEvents;
                if (ctx.interruptedTurnIds.has(prepared.turnId)) {
                  // interruptTurn owns terminal settlement once it marks this turn.
                  // In particular, a locally interrupted prompt must not expose the
                  // session as ready before remote cancellation is confirmed.
                  yield* Ref.set(promptSettled, true);
                  return {
                    threadId: input.threadId,
                    turnId: prepared.turnId,
                    resumeCursor: ctx.session.resumeCursor,
                  };
                }
                if (
                  ctx.activeTurnId !== prepared.turnId ||
                  ctx.session.activeTurnId !== prepared.turnId ||
                  ctx.promptsInFlight <= 0
                ) {
                  yield* Ref.set(promptSettled, true);
                  return {
                    threadId: input.threadId,
                    turnId: prepared.turnId,
                    resumeCursor: ctx.session.resumeCursor,
                  };
                }

                const turnRecord = ctx.turns.find((turn) => turn.id === prepared.turnId);
                const promptSummary = {
                  textBlockCount: prepared.promptParts.filter((part) => part.type === "text")
                    .length,
                  imageBlockCount: prepared.promptParts.filter((part) => part.type === "image")
                    .length,
                };
                if (turnRecord) {
                  turnRecord.items.push({ prompt: promptSummary, result });
                } else {
                  ctx.turns.push({
                    id: prepared.turnId,
                    items: [{ prompt: promptSummary, result }],
                  });
                }
                ctx.session = {
                  ...ctx.session,
                  status: "running",
                  activeTurnId: prepared.turnId,
                  updatedAt: yield* nowIso,
                  model: prepared.model,
                };
                yield* settlePromptInFlight(ctx, prepared.turnId, {
                  stopReason: result.stopReason ?? null,
                });
                yield* Ref.set(promptSettled, true);

                return {
                  threadId: input.threadId,
                  turnId: prepared.turnId,
                  resumeCursor: ctx.session.resumeCursor,
                };
              }),
            );
          }),
        ).pipe(
          Effect.ensuring(
            Effect.gen(function* () {
              if (yield* Ref.get(promptSettled)) return;
              const errorMessage = yield* Ref.get(promptFailureMessage);
              yield* withThreadLock(
                input.threadId,
                Effect.gen(function* () {
                  const ctx = sessions.get(input.threadId);
                  if (ctx !== prepared.ctx || ctx.stopped) return;
                  yield* Effect.ignore(prepared.acp.drainEvents);
                  yield* settlePromptInFlight(ctx, prepared.turnId, {
                    errorMessage: errorMessage ?? "Kilo prompt request failed or was interrupted.",
                  });
                }),
              );
            }).pipe(
              Effect.catch((error) =>
                Effect.logError("Failed to settle Kilo prompt after an error.", { cause: error }),
              ),
            ),
          ),
        );
      }),
    );
  });

  const interruptTurn: KiloAdapterShape["interruptTurn"] = Effect.fn("interruptTurn")(
    function* (threadId, turnId) {
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          // Preparation holds the thread lock, so record cancellation intent for
          // that exact connecting turn before waiting. The send path observes this
          // marker before it can enter ACP. Running turns are claimed only under the
          // lock below so an old turn id can never acquire a newer turn's runtime.
          // The unlocked add is safe: it only inserts a cancellation marker that
          // the send path reads via the runtime's shouldStart revalidation, and
          // the locked claim below re-checks the same conditions before settling.
          const observed = yield* Effect.sync(() => {
            const ctx = sessions.get(threadId);
            if (!ctx || ctx.stopped) return undefined;
            const interruptedTurnId = ctx.activeTurnId ?? ctx.session.activeTurnId;
            if (turnId !== undefined && interruptedTurnId !== turnId) {
              return { ctx, interruptedTurnId, matchesRequestedTurn: false as const };
            }
            if (interruptedTurnId !== undefined && ctx.session.status === "connecting") {
              ctx.interruptedTurnIds.add(interruptedTurnId);
            }
            return { ctx, interruptedTurnId, matchesRequestedTurn: true as const };
          });
          if (!observed) {
            return yield* new ProviderAdapterSessionNotFoundError({
              provider: PROVIDER,
              threadId,
            });
          }
          if (!observed.matchesRequestedTurn || observed.interruptedTurnId === undefined) return;
          const observedTurnId = observed.interruptedTurnId;
          const cancellationClaim = yield* withThreadLock(
            threadId,
            Effect.gen(function* () {
              const ctx = yield* requireSession(threadId);
              const activeTurnId = ctx.activeTurnId ?? ctx.session.activeTurnId;
              if (ctx !== observed.ctx || activeTurnId !== observedTurnId) {
                return undefined;
              }
              ctx.interruptedTurnIds.add(observedTurnId);
              yield* settlePendingApprovalsAsCancelled(ctx.pendingApprovals);
              return { ctx, interruptedTurnId: observedTurnId };
            }),
          );
          if (!cancellationClaim) return;
          const cancelTarget = cancellationClaim.ctx;

          // Release the preparation lock while awaiting the remote cancellation.
          // A permission handler already inside logging/fingerprinting can then
          // acquire the lock, observe the interrupted turn, and answer cancelled
          // so the remote prompt is able to settle. Caller interruption is restored
          // only for this remote operation; either outcome returns to protected
          // lifecycle cleanup before the interrupt can escape.
          const cancelExit = yield* restore(
            cancelTarget.acp.cancel.pipe(
              Effect.mapError((error) =>
                mapAcpToAdapterError(PROVIDER, threadId, "session/cancel", error),
              ),
            ),
          ).pipe(Effect.exit);
          if (Exit.isFailure(cancelExit)) {
            yield* withThreadLock(
              threadId,
              Effect.gen(function* () {
                if (sessions.get(threadId) !== cancelTarget || cancelTarget.stopped) return;
                yield* quarantineSessionInternal(
                  cancelTarget,
                  "Kilo cancellation could not be confirmed. The session was terminated to prevent overlapping remote work.",
                );
              }),
            );
            return yield* Effect.failCause(cancelExit.cause);
          }

          yield* withThreadLock(
            threadId,
            Effect.gen(function* () {
              if (sessions.get(threadId) !== cancelTarget || cancelTarget.stopped) return;
              yield* Effect.ignore(cancelTarget.acp.drainEvents);
              yield* settlePromptInFlight(cancelTarget, cancellationClaim.interruptedTurnId, {
                stopReason: "cancelled",
                settleAllPrompts: true,
              });
            }),
          );
        }),
      );
    },
  );

  const respondToRequest: KiloAdapterShape["respondToRequest"] = Effect.fn("respondToRequest")(
    function* (threadId, requestId, decision) {
      return yield* withThreadLock(
        threadId,
        Effect.uninterruptible(
          Effect.gen(function* () {
            const ctx = yield* requireSession(threadId);
            const pending = yield* Effect.sync(() => {
              const claimed = ctx.pendingApprovals.get(requestId);
              if (claimed) ctx.pendingApprovals.delete(requestId);
              return claimed;
            });
            if (!pending) {
              return yield* new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "session/request_permission",
                detail: `Unknown pending approval request: ${requestId}`,
              });
            }
            yield* options?.afterApprovalClaim?.(requestId) ?? Effect.void;
            const resolved = yield* Deferred.succeed(pending.decision, decision);
            if (!resolved) {
              return yield* new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "session/request_permission",
                detail: `Approval request is already resolved: ${requestId}`,
              });
            }
            if (decision === "acceptForSession" && pending.permissionFingerprint !== undefined) {
              ctx.sessionApprovedPermissionFingerprints.add(pending.permissionFingerprint);
            }
          }),
        ),
      );
    },
  );

  const respondToUserInput: KiloAdapterShape["respondToUserInput"] = Effect.fn(
    "respondToUserInput",
  )(function* (threadId, _requestId, _answers) {
    yield* requireSession(threadId);
    return yield* new ProviderAdapterRequestError({
      provider: PROVIDER,
      method: "user-input",
      detail:
        "Kilo ACP does not forward question-tool input, so T3 disables that tool for this provider.",
    });
  });

  const readThread: KiloAdapterShape["readThread"] = Effect.fn("readThread")(function* (threadId) {
    const ctx = yield* requireSession(threadId);
    return { threadId, turns: ctx.turns };
  });

  const rollbackThread: KiloAdapterShape["rollbackThread"] = Effect.fn("rollbackThread")(
    function* (threadId, numTurns) {
      yield* requireSession(threadId);
      if (!Number.isInteger(numTurns) || numTurns < 1) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "numTurns must be an integer >= 1.",
        });
      }
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "thread/rollback",
        detail: "Kilo ACP sessions do not support provider-side rollback yet.",
      });
    },
  );

  const stopSession: KiloAdapterShape["stopSession"] = (threadId) =>
    withThreadLock(
      threadId,
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        yield* stopSessionInternal(ctx);
      }),
    );

  const listSessions: KiloAdapterShape["listSessions"] = () =>
    Effect.sync(() => Array.from(sessions.values(), (c) => ({ ...c.session })));

  const hasSession: KiloAdapterShape["hasSession"] = (threadId) =>
    Effect.sync(() => {
      const c = sessions.get(threadId);
      return c !== undefined && !c.stopped;
    });

  const stopAll: KiloAdapterShape["stopAll"] = () =>
    Effect.uninterruptible(
      Effect.gen(function* () {
        const threadIds = yield* Effect.sync(() => {
          stopAllInProgressCount += 1;
          stopAllGeneration += 1;
          return Array.from(new Set([...sessions.keys(), ...startingSessionCounts.keys()]));
        });
        yield* Effect.forEach(
          threadIds,
          (threadId) =>
            withThreadLock(
              threadId,
              Effect.gen(function* () {
                const ctx = sessions.get(threadId);
                if (ctx && !ctx.stopped) {
                  yield* stopSessionInternal(ctx);
                }
              }),
            ),
          { discard: true },
        ).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              stopAllInProgressCount = Math.max(0, stopAllInProgressCount - 1);
            }),
          ),
        );
      }),
    );

  yield* Effect.addFinalizer(() =>
    stopAll().pipe(
      Effect.catch((cause) =>
        Effect.logError("Failed to emit Kilo session shutdown event.", { cause }),
      ),
      Effect.tap(() => PubSub.shutdown(runtimeEventPubSub)),
      Effect.tap(() => managedNativeEventLogger?.close() ?? Effect.void),
    ),
  );

  const streamEvents = Stream.fromPubSub(runtimeEventPubSub);

  return {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "in-session" },
    startSession,
    sendTurn,
    interruptTurn,
    readThread,
    rollbackThread,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    stopAll,
    streamEvents,
  } satisfies KiloAdapterShape;
});

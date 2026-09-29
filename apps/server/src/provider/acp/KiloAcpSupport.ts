import {
  DEFAULT_MODEL,
  DEFAULT_TEXT_GENERATION_MODEL,
  type KiloSettings,
  type RuntimeMode,
} from "@t3tools/contracts";
import { fromLenientJson } from "@t3tools/shared/schemaJson";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";
import { findSessionConfigOption } from "./AcpRuntimeModel.ts";

const KILO_AUTH_METHOD_ID = "kilo-login";
const KILO_ACP_STARTUP_SEMAPHORES = new Map<string, Semaphore.Semaphore>();
const DEFAULT_KILO_STARTUP_COMMAND = "kilo";

function kiloStartupSemaphore(startupCommand: string): Semaphore.Semaphore {
  const existing = KILO_ACP_STARTUP_SEMAPHORES.get(startupCommand);
  if (existing) return existing;
  const created = Semaphore.makeUnsafe(1);
  KILO_ACP_STARTUP_SEMAPHORES.set(startupCommand, created);
  return created;
}
const KILO_ACP_INITIALIZE_RETRY_DELAYS = [
  Duration.millis(500),
  Duration.seconds(1),
  Duration.seconds(2),
] as const;
const KILO_CANCEL_SETTLE_TIMEOUT = Duration.seconds(5);
const KILO_CANCEL_TRANSPORT_TIMEOUT = Duration.seconds(5);
export const KILO_PROVIDER_DEFAULT_MODEL_ID = "__t3_provider_default__";
const T3_GENERIC_MODEL_FALLBACKS = new Set([
  DEFAULT_MODEL,
  DEFAULT_TEXT_GENERATION_MODEL,
  // Stored selections from older T3 releases used this generic helper model.
  "gpt-5.6-luna",
  KILO_PROVIDER_DEFAULT_MODEL_ID,
]);

export function hardenKiloProbeEnvironment(
  environment: NodeJS.ProcessEnv | undefined,
): NodeJS.ProcessEnv {
  return {
    ...(environment ?? process.env),
    // `--pure` / KILO_PURE disables external server plugins and their hooks.
    KILO_PURE: "1",
    // Automatic probes must not execute workspace-local configuration,
    // instructions, or plugin declarations.
    KILO_DISABLE_PROJECT_CONFIG: "1",
    KILO_DISABLE_EXTERNAL_SKILLS: "1",
    KILO_DISABLE_SKILL_SHELL: "1",
  };
}

/**
 * Kilo plugins and hooks execute outside ACP tool-permission requests. Keep
 * trusted user extensions available in full-access sessions, but disable them
 * for modes whose T3 controls promise to supervise repository-side effects.
 * Project config is disabled too because it may eagerly start local MCP
 * commands before any ACP tool-permission request reaches T3.
 */
export function enforceKiloInteractiveModeEnvironment(
  environment: NodeJS.ProcessEnv,
  runtimeMode: RuntimeMode,
): NodeJS.ProcessEnv {
  if (runtimeMode === "full-access") return environment;
  return {
    ...environment,
    KILO_PURE: "1",
    KILO_DISABLE_PROJECT_CONFIG: "1",
  };
}

/**
 * Kilo exposes model selection through ACP `configOptions` selects. T3 Code
 * renders those natively, so we always advertise support; agents that gate
 * config-option exposure on this capability then surface their full catalog.
 */
const KILO_CLIENT_CAPABILITIES = {
  _meta: {
    parameterizedModelPicker: true,
  },
} satisfies NonNullable<EffectAcpSchema.InitializeRequest["clientCapabilities"]>;

type KiloAcpSetupResponse =
  | EffectAcpSchema.LoadSessionResponse
  | EffectAcpSchema.NewSessionResponse
  | EffectAcpSchema.ResumeSessionResponse;

type KiloAcpRuntimeKiloSettings = Pick<KiloSettings, "binaryPath">;

const decodeLenientKiloConfig = Schema.decodeUnknownExit(fromLenientJson(Schema.Unknown));

const KILO_SAFE_READ_PERMISSION_POLICY = {
  read: "allow",
  glob: "allow",
  grep: "allow",
  list: "allow",
  webfetch: "allow",
  websearch: "allow",
  semantic_search: "allow",
  kilo_memory_recall: "allow",
  lsp: "allow",
  todoread: "allow",
} as const;

export type KiloChildAgentPolicyResult =
  | {
      readonly ok: true;
      readonly environment: NodeJS.ProcessEnv;
      readonly modeId: string;
    }
  | { readonly ok: false; readonly message: string };

/**
 * Adds a nonce-scoped primary agent to the child-only inline config. The
 * agent's final wildcard permission is merged after global, project, account,
 * and managed root permissions. A managed config cannot predict the nonce;
 * if it removes the injected agent entirely, mode verification fails closed.
 */
export function buildKiloChildAgentPolicyEnvironment(input: {
  readonly environment: NodeJS.ProcessEnv;
  readonly nonce: string;
  readonly policy: "ask" | "full-access" | "plan";
  readonly label: string;
  readonly prompt: string;
}): KiloChildAgentPolicyResult {
  let config: Record<string, unknown> = {};
  const existingContent = input.environment.KILO_CONFIG_CONTENT?.trim();
  if (existingContent) {
    const parsed = decodeLenientKiloConfig(existingContent);
    if (Exit.isFailure(parsed)) {
      return {
        ok: false,
        message: "KILO_CONFIG_CONTENT is invalid JSONC; T3 cannot safely enforce Kilo permissions.",
      };
    }
    if (!Predicate.isObject(parsed.value)) {
      return {
        ok: false,
        message:
          "KILO_CONFIG_CONTENT must contain a JSON object before T3 can enforce Kilo permissions.",
      };
    }
    config = parsed.value;
  }

  const existingAgents = Predicate.isObject(config.agent) ? config.agent : {};
  const modeId = `t3-${input.policy}-${input.nonce}`;
  const agent = {
    description: input.label,
    displayName: input.label,
    mode: "primary",
    prompt: input.prompt,
    permission:
      input.policy === "ask"
        ? {
            "*": "ask",
            ...KILO_SAFE_READ_PERMISSION_POLICY,
            // Kilo's question tool is not forwarded over ACP. Deny it rather
            // than letting a headless prompt wait forever for invisible input.
            question: "deny",
            // Kilo subagents inherit parent denies but not asks. Disabling the
            // task boundary prevents a delegated agent from bypassing T3's
            // supervised permission handler.
            task: "deny",
          }
        : input.policy === "full-access"
          ? { "*": "allow", question: "deny" }
          : {
              "*": "deny",
              ...KILO_SAFE_READ_PERMISSION_POLICY,
              skill: "allow",
              plan_exit: "allow",
              question: "deny",
              bash: "deny",
              edit: "deny",
              task: "deny",
            },
  };
  return {
    ok: true,
    modeId,
    environment: {
      ...input.environment,
      KILO_CONFIG_CONTENT: JSON.stringify({
        ...config,
        agent: { ...existingAgents, [modeId]: agent },
      }),
    },
  };
}

export interface KiloAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  "authMethodId" | "clientCapabilities" | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly kiloSettings: KiloAcpRuntimeKiloSettings | null | undefined;
  readonly environment?: NodeJS.ProcessEnv;
}

export function buildKiloAcpSpawnInput(
  kiloSettings: KiloAcpRuntimeKiloSettings | null | undefined,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
): AcpSessionRuntime.AcpSpawnInput {
  return {
    command: kiloSettings?.binaryPath || "kilo",
    args: ["acp"],
    cwd,
    ...(environment ? { env: environment } : {}),
  };
}

const makeKiloAcpRuntime = Effect.fn("makeKiloAcpRuntime")(function* (
  input: KiloAcpRuntimeInput,
): Effect.fn.Return<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  EffectAcpErrors.AcpError,
  Crypto.Crypto | Scope.Scope
> {
  const acpContext = yield* Layer.build(
    AcpSessionRuntime.layer({
      ...input,
      cancelBehavior: "wait-for-prompt",
      cancelTransportTimeout: input.cancelTransportTimeout ?? KILO_CANCEL_TRANSPORT_TIMEOUT,
      cancelSettleTimeout: input.cancelSettleTimeout ?? KILO_CANCEL_SETTLE_TIMEOUT,
      spawn: buildKiloAcpSpawnInput(input.kiloSettings, input.cwd, input.environment),
      authMethodId: KILO_AUTH_METHOD_ID,
      clientCapabilities: KILO_CLIENT_CAPABILITIES,
    }).pipe(
      Layer.provide(
        Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
      ),
    ),
  );
  return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
    Effect.provide(acpContext),
  );
});

/** Serializes cold starts per Kilo command so one binary cannot block another. */
export const withKiloAcpStartupPermit = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  startupCommand: string = DEFAULT_KILO_STARTUP_COMMAND,
) => kiloStartupSemaphore(startupCommand).withPermit(effect);

/**
 * Kilo commands whose one-time database bootstrap has been observed complete
 * (one successful startup). Later startups for the same command skip the
 * startup permit: the migration cannot rerun, and Kilo's port fallback
 * supports concurrent warm children.
 */
const confirmedKiloStartupCommands = new Set<string>();

const isAcpTransportError = Schema.is(EffectAcpErrors.AcpTransportError);

/**
 * Kilo's first process may be performing its one-time database migration when
 * another T3 server starts Kilo against the same user data. The losing child
 * exits during ACP initialization. Recreate only that pre-session attempt;
 * other startup failures remain immediate and unchanged.
 */
export const retryKiloAcpInitialization = Effect.fn("retryKiloAcpInitialization")(function* <
  A,
  E,
  R,
>(
  makeAttempt: () => Effect.Effect<A, E, R>,
  retryDelays: ReadonlyArray<Duration.Input> = KILO_ACP_INITIALIZE_RETRY_DELAYS,
): Effect.fn.Return<A, E, R> {
  let retryIndex = 0;
  while (true) {
    const result = yield* Effect.result(makeAttempt());
    if (Result.isSuccess(result)) return result.success;

    const error = result.failure;
    const isRetryable =
      isAcpTransportError(error) && error.operation === "call-rpc" && error.method === "initialize";
    const retryDelay = retryDelays[retryIndex];
    if (!isRetryable || retryDelay === undefined) {
      return yield* Effect.fail(error);
    }

    yield* Effect.sleep(retryDelay);
    retryIndex += 1;
  }
});

/**
 * Builds and starts one Kilo ACP runtime. Until the first startup of a Kilo
 * command succeeds, startups hold the process-local startup permit: Kilo's
 * one-time database bootstrap does not exclude concurrent processes, and the
 * child binds its internal HTTP listener before ACP initialization, so the
 * permit covers child creation through session readiness (or failure). Once
 * one startup has succeeded the migration is complete and later startups for
 * that command run concurrently.
 */
export const startKiloAcpRuntime = Effect.fn("startKiloAcpRuntime")(function* <
  E = never,
  R = never,
>(
  input: KiloAcpRuntimeInput,
  configureRuntime: (
    runtime: AcpSessionRuntime.AcpSessionRuntime["Service"],
  ) => Effect.Effect<void, E, R> = () => Effect.void,
): Effect.fn.Return<
  {
    readonly runtime: AcpSessionRuntime.AcpSessionRuntime["Service"];
    readonly started: AcpSessionRuntime.AcpSessionRuntimeStartResult;
  },
  EffectAcpErrors.AcpError | E,
  Crypto.Crypto | Scope.Scope | R
> {
  const parentScope = yield* Scope.Scope;
  const startupCommand = input.kiloSettings?.binaryPath || "kilo";
  const startAttempts = retryKiloAcpInitialization(() =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const attemptScope = yield* Scope.fork(parentScope);
        const attemptExit = yield* Effect.exit(
          restore(
            makeKiloAcpRuntime(input).pipe(
              Effect.tap(configureRuntime),
              Effect.flatMap((runtime) =>
                runtime.start().pipe(Effect.map((started) => ({ runtime, started }))),
              ),
              Effect.provideService(Scope.Scope, attemptScope),
            ),
          ),
        );
        if (Exit.isFailure(attemptExit)) {
          yield* Scope.close(attemptScope, attemptExit).pipe(Effect.ignore);
          return yield* Effect.failCause(attemptExit.cause);
        }
        return attemptExit.value;
      }),
    ),
  );
  if (confirmedKiloStartupCommands.has(startupCommand)) {
    return yield* startAttempts;
  }
  const started = yield* withKiloAcpStartupPermit(startAttempts, startupCommand);
  confirmedKiloStartupCommands.add(startupCommand);
  return started;
});

type KiloModelSelectOption = Extract<EffectAcpSchema.SessionConfigOption, { type: "select" }>;

function findKiloModelConfigOptionIn(
  configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption> | null | undefined,
): KiloModelSelectOption | undefined {
  if (!configOptions) return undefined;
  const byCategory = configOptions.find(
    (option) => option.type === "select" && option.category === "model",
  );
  if (byCategory && byCategory.type === "select") return byCategory;
  const byId = findSessionConfigOption(configOptions, "model");
  return byId && byId.type === "select" ? byId : undefined;
}

function findKiloModelConfigOption(
  sessionSetupResult: KiloAcpSetupResponse,
): KiloModelSelectOption | undefined {
  return findKiloModelConfigOptionIn(sessionSetupResult.configOptions);
}

export function currentKiloModelIdFromSessionSetup(
  sessionSetupResult: KiloAcpSetupResponse,
): string | undefined {
  const option = findKiloModelConfigOption(sessionSetupResult);
  if (!option) return undefined;
  const current = option.currentValue.trim();
  return current.length > 0 ? current : undefined;
}

export interface KiloDiscoveredModel {
  readonly slug: string;
  readonly name: string;
  readonly isDefault?: boolean;
}

export function kiloModelsFromSessionConfigOptions(
  sessionSetupResult: KiloAcpSetupResponse,
): ReadonlyArray<KiloDiscoveredModel> {
  const option = findKiloModelConfigOption(sessionSetupResult);
  if (!option) return [];
  const current = currentKiloModelIdFromSessionSetup(sessionSetupResult);
  const discovered = option.options
    .flatMap((entry) => ("value" in entry ? [entry] : entry.options))
    .flatMap((entry) => {
      const slug = entry.value.trim();
      if (!slug) return [];
      const name = entry.name.trim() || slug;
      return [
        {
          slug,
          name,
          ...(slug === current ? { isDefault: true } : {}),
        },
      ];
    });
  if (discovered.length > 0 || current === undefined) {
    return discovered;
  }
  // The current value is ACP-owned and safe to reuse even if an older or
  // partially initialized Kilo build omits the corresponding catalog entry.
  return [{ slug: current, name: current, isDefault: true }];
}

export const applyKiloAcpModelSelection = Effect.fn("applyKiloAcpModelSelection")(function* <
  E,
>(input: {
  readonly runtime: Pick<
    AcpSessionRuntime.AcpSessionRuntime["Service"],
    "getConfigOptions" | "setConfigOption"
  >;
  readonly requestedModelId: string | null | undefined;
  readonly mapError: (cause: EffectAcpErrors.AcpError) => E;
}): Effect.fn.Return<string | undefined, E> {
  const requested = input.requestedModelId?.trim();
  if (!requested) {
    return undefined;
  }
  const configOptions = yield* input.runtime.getConfigOptions;
  const option = findKiloModelConfigOptionIn(configOptions);
  if (!option) {
    return undefined;
  }
  if (option.currentValue.trim() === requested) {
    return requested;
  }
  const availableValues = new Set(
    option.options
      .flatMap((entry) =>
        "value" in entry ? [entry.value] : entry.options.map((item) => item.value),
      )
      .map((value) => value.trim())
      .filter(Boolean),
  );
  if (!availableValues.has(requested) && T3_GENERIC_MODEL_FALLBACKS.has(requested)) {
    // Kilo rejects model values it did not advertise. Preserve the ACP
    // session's own current model instead of sending T3's provider-agnostic
    // fallback or provider-default sentinel through
    // session/set_config_option. Explicit unknown values still reach the
    // shared ACP validator and fail locally against the authoritative list.
    return option.currentValue.trim() || undefined;
  }
  yield* input.runtime.setConfigOption(option.id, requested).pipe(Effect.mapError(input.mapError));
  return requested;
});

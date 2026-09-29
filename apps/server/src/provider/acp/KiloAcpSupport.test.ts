import { describe, expect, it } from "vite-plus/test";
import * as itx from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import {
  applyKiloAcpModelSelection,
  buildKiloChildAgentPolicyEnvironment,
  buildKiloAcpSpawnInput,
  currentKiloModelIdFromSessionSetup,
  hardenKiloProbeEnvironment,
  kiloModelsFromSessionConfigOptions,
  KILO_PROVIDER_DEFAULT_MODEL_ID,
  retryKiloAcpInitialization,
  withKiloAcpStartupPermit,
} from "./KiloAcpSupport.ts";

function modelSelectOption(
  overrides?: Partial<Extract<EffectAcpSchema.SessionConfigOption, { type: "select" }>>,
) {
  return {
    id: "model",
    name: "Model",
    category: "model" as const,
    type: "select" as const,
    currentValue: "kilo/anthropic/claude-opus-4.7",
    options: [
      { value: "kilo/anthropic/claude-opus-4.7", name: "Claude Opus 4.7" },
      { value: "kilo/google/gemini-3-pro", name: "Gemini 3 Pro" },
    ],
    ...overrides,
  };
}

function fakeRuntime(input: {
  readonly configOptions: ReadonlyArray<EffectAcpSchema.SessionConfigOption>;
  readonly setCalls?: Array<{ configId: string; value: string | boolean }>;
}) {
  const setCalls = input.setCalls ?? [];
  return {
    getConfigOptions: Effect.succeed(input.configOptions),
    setConfigOption: (configId: string, value: string | boolean) => {
      setCalls.push({ configId, value });
      return Effect.succeed({
        configOptions: input.configOptions,
      } satisfies EffectAcpSchema.SetSessionConfigOptionResponse);
    },
  } as const;
}

describe("kilo ACP support", () => {
  it("spawns the official Kilo ACP subcommand", () => {
    expect(buildKiloAcpSpawnInput(undefined, "/workspace")).toEqual({
      command: "kilo",
      args: ["acp"],
      cwd: "/workspace",
    });
  });

  it("forces probe isolation flags over conflicting inherited values", () => {
    expect(
      hardenKiloProbeEnvironment({
        PATH: "/custom/bin",
        KILO_PURE: "0",
        KILO_DISABLE_PROJECT_CONFIG: "false",
        KILO_DISABLE_EXTERNAL_SKILLS: "0",
        KILO_DISABLE_SKILL_SHELL: "no",
      }),
    ).toEqual({
      PATH: "/custom/bin",
      KILO_PURE: "1",
      KILO_DISABLE_PROJECT_CONFIG: "1",
      KILO_DISABLE_EXTERNAL_SKILLS: "1",
      KILO_DISABLE_SKILL_SHELL: "1",
    });
  });

  it("merges JSONC inline config and appends a supervised agent with safe reads but no tasks", () => {
    const result = buildKiloChildAgentPolicyEnvironment({
      environment: {
        KILO_CONFIG_CONTENT: `{
          // Existing callers may use Kilo's documented JSONC format.
          "theme": "kilo",
          "agent": {
            "existing": { "mode": "primary", },
          },
        }`,
      },
      nonce: "nonce",
      policy: "ask",
      label: "T3 supervised",
      prompt: "Implement the user's request.",
    });

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.modeId).toBe("t3-ask-nonce");
    const config = JSON.parse(result.environment.KILO_CONFIG_CONTENT ?? "") as {
      readonly theme: string;
      readonly agent: Record<string, unknown>;
    };
    expect(config.theme).toBe("kilo");
    expect(config.agent.existing).toEqual({ mode: "primary" });
    expect(config.agent[result.modeId]).toEqual({
      description: "T3 supervised",
      displayName: "T3 supervised",
      mode: "primary",
      prompt: "Implement the user's request.",
      permission: {
        "*": "ask",
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
        question: "deny",
        task: "deny",
      },
    });
  });

  it("fails closed when an existing inline config is malformed JSONC", () => {
    expect(
      buildKiloChildAgentPolicyEnvironment({
        environment: { KILO_CONFIG_CONTENT: `{ "permission": /* missing value */ }` },
        nonce: "nonce",
        policy: "ask",
        label: "T3 supervised",
        prompt: "Implement the user's request.",
      }),
    ).toEqual({
      ok: false,
      message: "KILO_CONFIG_CONTENT is invalid JSONC; T3 cannot safely enforce Kilo permissions.",
    });
  });

  it("allows tools except the unsupported question tool in full-access sessions", () => {
    const result = buildKiloChildAgentPolicyEnvironment({
      environment: {},
      nonce: "nonce",
      policy: "full-access",
      label: "T3 full access",
      prompt: "Implement the user's request.",
    });

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    const config = JSON.parse(result.environment.KILO_CONFIG_CONTENT ?? "") as {
      readonly agent: Record<string, { readonly permission: unknown }>;
    };
    expect(config.agent[result.modeId]?.permission).toEqual({
      "*": "allow",
      question: "deny",
    });
  });

  it("creates a read-only plan agent that cannot question or delegate", () => {
    const result = buildKiloChildAgentPolicyEnvironment({
      environment: {},
      nonce: "nonce",
      policy: "plan",
      label: "T3 plan",
      prompt: "Create a plan.",
    });

    expect(result.ok).toBe(true);
    if (!result.ok) return;
    const config = JSON.parse(result.environment.KILO_CONFIG_CONTENT ?? "") as {
      readonly agent: Record<string, { readonly permission: Record<string, string> }>;
    };
    expect(config.agent[result.modeId]?.permission).toMatchObject({
      "*": "deny",
      read: "allow",
      grep: "allow",
      glob: "allow",
      websearch: "allow",
      question: "deny",
      bash: "deny",
      edit: "deny",
      task: "deny",
    });
  });

  itx.it.effect("gates fake child spawn through readiness and releases on failure", () =>
    Effect.gen(function* () {
      const spawnCount = yield* Ref.make(0);
      const firstReady = yield* Deferred.make<void>();
      const firstSpawned = yield* Deferred.make<void>();

      const fakeSpawnAndStart = (ready: Effect.Effect<void, string>) =>
        withKiloAcpStartupPermit(
          Ref.update(spawnCount, (count) => count + 1).pipe(
            Effect.andThen(Deferred.succeed(firstSpawned, undefined).pipe(Effect.ignore)),
            Effect.andThen(ready),
          ),
        );

      const first = yield* fakeSpawnAndStart(
        Deferred.await(firstReady).pipe(Effect.andThen(Effect.fail("startup failed"))),
      ).pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(firstSpawned);
      const second = yield* fakeSpawnAndStart(Effect.void).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      expect(yield* Ref.get(spawnCount)).toBe(1);

      yield* Deferred.succeed(firstReady, undefined);
      expect(Exit.isFailure(yield* Fiber.join(first))).toBe(true);
      yield* Fiber.join(second);
      expect(yield* Ref.get(spawnCount)).toBe(2);
    }),
  );

  itx.it.effect("retries only ACP initialize transport failures", () =>
    Effect.gen(function* () {
      const attempts = yield* Ref.make(0);
      const result = yield* retryKiloAcpInitialization(
        () =>
          Ref.updateAndGet(attempts, (count) => count + 1).pipe(
            Effect.flatMap((attempt) =>
              attempt < 3
                ? Effect.fail(
                    new EffectAcpErrors.AcpTransportError({
                      operation: "call-rpc",
                      method: "initialize",
                      cause: new Error("Kilo exited during migration"),
                    }),
                  )
                : Effect.succeed("ready"),
            ),
          ),
        [Duration.zero, Duration.zero],
      );

      expect(result).toBe("ready");
      expect(yield* Ref.get(attempts)).toBe(3);
    }),
  );

  itx.it.effect("does not retry other ACP startup failures", () =>
    Effect.gen(function* () {
      const attempts = yield* Ref.make(0);
      const result = yield* retryKiloAcpInitialization(
        () =>
          Ref.update(attempts, (count) => count + 1).pipe(
            Effect.andThen(
              Effect.fail(
                new EffectAcpErrors.AcpTransportError({
                  operation: "call-rpc",
                  method: "session/new",
                  cause: new Error("request failed"),
                }),
              ),
            ),
          ),
        [Duration.zero],
      ).pipe(Effect.exit);

      expect(Exit.isFailure(result)).toBe(true);
      expect(yield* Ref.get(attempts)).toBe(1);
    }),
  );

  it("reads the current model id from model-category config options", () => {
    const current = currentKiloModelIdFromSessionSetup({
      sessionId: "ses_1",
      configOptions: [
        {
          id: "mode",
          name: "Mode",
          category: "mode",
          type: "select",
          currentValue: "code",
          options: [],
        },
        modelSelectOption(),
      ],
    });
    expect(current).toBe("kilo/anthropic/claude-opus-4.7");
  });

  it("falls back to an id lookup when no option is tagged with the model category", () => {
    const current = currentKiloModelIdFromSessionSetup({
      sessionId: "ses_1",
      configOptions: [modelSelectOption({ category: null })],
    });
    expect(current).toBe("kilo/anthropic/claude-opus-4.7");
  });

  it("derives discovered models from config options and marks the current one default", () => {
    const models = kiloModelsFromSessionConfigOptions({
      sessionId: "ses_1",
      configOptions: [modelSelectOption()],
    });
    expect(models).toEqual([
      {
        slug: "kilo/anthropic/claude-opus-4.7",
        name: "Claude Opus 4.7",
        isDefault: true,
      },
      { slug: "kilo/google/gemini-3-pro", name: "Gemini 3 Pro" },
    ]);
  });

  it("preserves model names from grouped config options", () => {
    const models = kiloModelsFromSessionConfigOptions({
      sessionId: "ses_1",
      configOptions: [
        modelSelectOption({
          currentValue: "kilo/google/gemini-3-pro",
          options: [
            {
              group: "kilo",
              name: "Kilo Gateway",
              options: [
                {
                  value: "kilo/google/gemini-3-pro",
                  name: "Google: Gemini 3 Pro",
                },
              ],
            },
          ],
        }),
      ],
    });

    expect(models).toEqual([
      {
        slug: "kilo/google/gemini-3-pro",
        name: "Google: Gemini 3 Pro",
        isDefault: true,
      },
    ]);
  });

  it("uses the ACP current model when the catalog is temporarily empty", () => {
    const models = kiloModelsFromSessionConfigOptions({
      sessionId: "ses_1",
      configOptions: [
        modelSelectOption({
          currentValue: "kilo/openrouter/free",
          options: [],
        }),
      ],
    });

    expect(models).toEqual([
      {
        slug: "kilo/openrouter/free",
        name: "kilo/openrouter/free",
        isDefault: true,
      },
    ]);
  });

  it("returns no models without a select config option for models", () => {
    const models = kiloModelsFromSessionConfigOptions({
      sessionId: "ses_1",
      configOptions: [
        { id: "autoApprove", name: "Auto approve", type: "boolean", currentValue: false },
      ],
    });
    expect(models).toEqual([]);
  });

  itx.it.effect("skips selection when the requested model is already current", () =>
    Effect.gen(function* () {
      const setCalls: Array<{ configId: string; value: string | boolean }> = [];
      const result = yield* applyKiloAcpModelSelection({
        runtime: fakeRuntime({
          configOptions: [modelSelectOption()],
          setCalls,
        }),
        requestedModelId: "kilo/anthropic/claude-opus-4.7",
        mapError: (cause): EffectAcpErrors.AcpError => cause,
      });
      expect(result).toBe("kilo/anthropic/claude-opus-4.7");
      expect(setCalls).toEqual([]);
    }),
  );

  itx.it.effect("sets the model config option when the request differs", () =>
    Effect.gen(function* () {
      const setCalls: Array<{ configId: string; value: string | boolean }> = [];
      const result = yield* applyKiloAcpModelSelection({
        runtime: fakeRuntime({
          configOptions: [modelSelectOption()],
          setCalls,
        }),
        requestedModelId: " kilo/google/gemini-3-pro ",
        mapError: (cause): EffectAcpErrors.AcpError => cause,
      });
      expect(result).toBe("kilo/google/gemini-3-pro");
      expect(setCalls).toEqual([{ configId: "model", value: "kilo/google/gemini-3-pro" }]);
    }),
  );

  itx.it.effect("preserves the ACP current model when a fallback is not advertised", () =>
    Effect.gen(function* () {
      const setCalls: Array<{ configId: string; value: string | boolean }> = [];
      const result = yield* applyKiloAcpModelSelection({
        runtime: fakeRuntime({
          configOptions: [modelSelectOption()],
          setCalls,
        }),
        requestedModelId: "gpt-5.6-luna",
        mapError: (cause): EffectAcpErrors.AcpError => cause,
      });
      expect(result).toBe("kilo/anthropic/claude-opus-4.7");
      expect(setCalls).toEqual([]);
    }),
  );

  itx.it.effect("normalizes the provider-default sentinel to the ACP current model", () =>
    Effect.gen(function* () {
      const setCalls: Array<{ configId: string; value: string | boolean }> = [];
      const result = yield* applyKiloAcpModelSelection({
        runtime: fakeRuntime({
          configOptions: [modelSelectOption()],
          setCalls,
        }),
        requestedModelId: KILO_PROVIDER_DEFAULT_MODEL_ID,
        mapError: (cause): EffectAcpErrors.AcpError => cause,
      });
      expect(result).toBe("kilo/anthropic/claude-opus-4.7");
      expect(setCalls).toEqual([]);
    }),
  );

  itx.it.effect("does nothing when the agent exposes no model config option", () =>
    Effect.gen(function* () {
      const result = yield* applyKiloAcpModelSelection({
        runtime: fakeRuntime({ configOptions: [] }),
        requestedModelId: "kilo/google/gemini-3-pro",
        mapError: (cause): EffectAcpErrors.AcpError => cause,
      });
      expect(result).toBeUndefined();
    }),
  );

  itx.it.effect("ignores empty model requests", () =>
    Effect.gen(function* () {
      const result = yield* applyKiloAcpModelSelection({
        runtime: fakeRuntime({ configOptions: [modelSelectOption()] }),
        requestedModelId: "   ",
        mapError: (cause): EffectAcpErrors.AcpError => cause,
      });
      expect(result).toBeUndefined();
    }),
  );
});

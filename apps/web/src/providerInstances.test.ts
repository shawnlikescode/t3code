import { ProviderDriverKind, ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import { DEFAULT_UNIFIED_SETTINGS } from "@t3tools/contracts/settings";
import { describe, expect, it } from "vite-plus/test";
import {
  applyProviderInstanceSettings,
  applyProviderInstanceSettingsToSnapshots,
  deriveProviderEntriesByEnvironment,
  deriveProviderInstanceEntries,
  getDefaultProviderInstanceModel,
  hasSelectableTextGenerationProviderSelection,
  isProviderInstancePickerReady,
  isProviderInstancePickerVisible,
  NO_PROVIDER_MODEL_SELECTION,
  resolveDefaultProviderModelSelection,
  resolveSelectableProviderInstance,
} from "./providerInstances";
import { resolveAppModelSelectionState } from "./modelSelection";

describe("hasSelectableTextGenerationProviderSelection", () => {
  it("uses catalog presence so a legitimate empty-catalog instance cannot collide", () => {
    const entries = deriveProviderInstanceEntries([
      provider({
        provider: ProviderDriverKind.make("codex"),
        instanceId: "t3code_no_provider",
        models: [],
      }),
    ]);

    expect(hasSelectableTextGenerationProviderSelection(NO_PROVIDER_MODEL_SELECTION, [])).toBe(
      false,
    );
    expect(hasSelectableTextGenerationProviderSelection(NO_PROVIDER_MODEL_SELECTION, entries)).toBe(
      true,
    );
  });
});

function provider(input: {
  provider: ProviderDriverKind;
  instanceId: string;
  enabled?: boolean;
  availability?: ServerProvider["availability"];
  displayName?: string;
  accentColor?: string;
  status?: ServerProvider["status"];
  models?: ServerProvider["models"];
  hasAuthoritativeModelCatalog?: boolean;
}): ServerProvider {
  return {
    instanceId: ProviderInstanceId.make(input.instanceId),
    driver: input.provider,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    enabled: input.enabled ?? true,
    installed: true,
    version: null,
    status: input.status ?? "ready",
    ...(input.availability ? { availability: input.availability } : {}),
    ...(input.hasAuthoritativeModelCatalog !== undefined
      ? { hasAuthoritativeModelCatalog: input.hasAuthoritativeModelCatalog }
      : {}),
    auth: { status: "authenticated" },
    checkedAt: "2026-01-01T00:00:00.000Z",
    models: input.models ?? [],
    slashCommands: [],
    skills: [],
  };
}

const model = (slug: string, isCustom = false, isDefault = false) => ({
  slug,
  name: slug,
  isCustom,
  ...(isDefault ? { isDefault: true } : {}),
  capabilities: {},
});

describe("applyProviderInstanceSettingsToSnapshots", () => {
  it("uses an enabled setting while the streamed snapshot still reports disabled", () => {
    const instanceId = ProviderInstanceId.make("t3code_no_provider");
    const driver = ProviderDriverKind.make("codex");
    const settings = {
      ...DEFAULT_UNIFIED_SETTINGS,
      providerInstances: {
        ...DEFAULT_UNIFIED_SETTINGS.providerInstances,
        [instanceId]: { driver, enabled: true },
      },
      textGenerationModelSelection: { instanceId, model: "openai/gpt-5.5" },
    };
    const providers = [
      provider({
        provider: driver,
        instanceId,
        enabled: false,
        models: [model("openai/gpt-5.5")],
      }),
    ];

    const effectiveProviders = applyProviderInstanceSettingsToSnapshots(providers, settings);
    expect(effectiveProviders[0]?.enabled).toBe(true);
    expect(resolveAppModelSelectionState(settings, effectiveProviders)).toMatchObject({
      instanceId,
      model: "openai/gpt-5.5",
    });
  });

  it("uses a disabled setting while the streamed snapshot still reports enabled", () => {
    const instanceId = ProviderInstanceId.make("codex");
    const driver = ProviderDriverKind.make("codex");
    const settings = {
      ...DEFAULT_UNIFIED_SETTINGS,
      providerInstances: {
        ...DEFAULT_UNIFIED_SETTINGS.providerInstances,
        [instanceId]: { driver, enabled: false },
      },
      textGenerationModelSelection: { instanceId, model: "openai/gpt-5.5" },
    };
    const providers = [
      provider({
        provider: driver,
        instanceId,
        enabled: true,
        models: [model("openai/gpt-5.5")],
      }),
    ];

    const effectiveProviders = applyProviderInstanceSettingsToSnapshots(providers, settings);
    expect(effectiveProviders[0]?.enabled).toBe(false);
    expect(resolveAppModelSelectionState(settings, effectiveProviders)).toEqual(
      NO_PROVIDER_MODEL_SELECTION,
    );
  });

  it("disables a stale snapshot when the same instance id was recreated under another driver", () => {
    const instanceId = ProviderInstanceId.make("shared_work");
    const oldDriver = ProviderDriverKind.make("codex");
    const replacementDriver = ProviderDriverKind.make("claudeAgent");
    const oldSelection = { instanceId, model: "openai/old-catalog-model" };
    const settings = {
      ...DEFAULT_UNIFIED_SETTINGS,
      providerInstances: {
        ...DEFAULT_UNIFIED_SETTINGS.providerInstances,
        [instanceId]: { driver: replacementDriver, enabled: true },
      },
      textGenerationModelSelection: oldSelection,
    };
    const providers = [
      provider({
        provider: oldDriver,
        instanceId,
        enabled: true,
        models: [model(oldSelection.model)],
      }),
    ];

    const effectiveProviders = applyProviderInstanceSettingsToSnapshots(providers, settings);
    expect(effectiveProviders[0]).toMatchObject({ driver: oldDriver, enabled: false });
    expect(resolveAppModelSelectionState(settings, effectiveProviders)).toEqual(
      NO_PROVIDER_MODEL_SELECTION,
    );
    expect(resolveDefaultProviderModelSelection(effectiveProviders, oldSelection)).toBeNull();
  });

  it("keeps an unavailable snapshot disabled when settings enable it", () => {
    const unavailableInstanceId = ProviderInstanceId.make("unavailable_writer");
    const liveInstanceId = ProviderInstanceId.make("live_writer");
    const codex = ProviderDriverKind.make("codex");
    const claude = ProviderDriverKind.make("claudeAgent");
    const unavailableSelection = {
      instanceId: unavailableInstanceId,
      model: "openai/unavailable-model",
    };
    const settings = {
      ...DEFAULT_UNIFIED_SETTINGS,
      providerInstances: {
        [unavailableInstanceId]: { driver: codex, enabled: true },
        [liveInstanceId]: { driver: claude, enabled: true },
      },
      textGenerationModelSelection: unavailableSelection,
    };
    const providers = [
      provider({
        provider: codex,
        instanceId: unavailableInstanceId,
        enabled: false,
        availability: "unavailable",
        models: [model(unavailableSelection.model)],
      }),
      provider({
        provider: claude,
        instanceId: liveInstanceId,
        models: [model("anthropic/live-model")],
      }),
    ];

    const effectiveProviders = applyProviderInstanceSettingsToSnapshots(providers, settings);
    const entries = deriveProviderInstanceEntries(effectiveProviders);
    const unavailableEntry = entries.find((entry) => entry.instanceId === unavailableInstanceId);

    expect(effectiveProviders[0]).toMatchObject({ availability: "unavailable", enabled: false });
    expect(unavailableEntry && isProviderInstancePickerVisible(unavailableEntry)).toBe(false);
    expect(resolveAppModelSelectionState(settings, effectiveProviders)).toMatchObject({
      instanceId: liveInstanceId,
      model: "anthropic/live-model",
    });
  });
});

describe("isProviderInstancePickerReady", () => {
  it("rejects a disabled instance even while its last probe status is ready", () => {
    const [entry] = deriveProviderInstanceEntries([
      provider({
        provider: ProviderDriverKind.make("codex"),
        instanceId: "codex",
        enabled: false,
      }),
    ]);

    expect(entry?.status).toBe("ready");
    expect(entry && isProviderInstancePickerReady(entry)).toBe(false);
  });

  it("accepts an enabled, available, ready instance", () => {
    const [entry] = deriveProviderInstanceEntries([
      provider({ provider: ProviderDriverKind.make("codex"), instanceId: "codex" }),
    ]);

    expect(entry && isProviderInstancePickerReady(entry)).toBe(true);
  });
});

describe("isProviderInstancePickerVisible", () => {
  it("keeps enabled instances in the rail and removes disabled instances", () => {
    const [enabledEntry, disabledEntry] = deriveProviderInstanceEntries([
      provider({ provider: ProviderDriverKind.make("codex"), instanceId: "codex" }),
      provider({
        provider: ProviderDriverKind.make("claudeAgent"),
        instanceId: "claudeAgent",
        enabled: false,
      }),
    ]);

    expect(enabledEntry && isProviderInstancePickerVisible(enabledEntry)).toBe(true);
    expect(disabledEntry && isProviderInstancePickerVisible(disabledEntry)).toBe(false);
  });
});

describe("applyProviderInstanceSettings", () => {
  it("uses settings when a streamed snapshot still reports a disabled default as enabled", () => {
    const entries = deriveProviderInstanceEntries([
      provider({ provider: ProviderDriverKind.make("codex"), instanceId: "codex" }),
    ]);
    const [entry] = applyProviderInstanceSettings(entries, {
      providerInstances: {
        [ProviderInstanceId.make("codex")]: {
          driver: ProviderDriverKind.make("codex"),
          enabled: false,
        },
      },
      providers: {} as never,
    });

    expect(entry?.enabled).toBe(false);
  });

  it("treats a removed custom instance snapshot as disabled", () => {
    const entries = deriveProviderInstanceEntries([
      provider({
        provider: ProviderDriverKind.make("claudeAgent"),
        instanceId: "claude_work",
      }),
    ]);
    const [entry] = applyProviderInstanceSettings(entries, {
      providerInstances: {},
      providers: {} as never,
    });

    expect(entry?.enabled).toBe(false);
  });

  it.each(["constructor", "toString"])(
    "treats a removed custom instance named %s as disabled",
    (instanceId) => {
      const entries = deriveProviderInstanceEntries([
        provider({
          provider: ProviderDriverKind.make("claudeAgent"),
          instanceId,
        }),
      ]);
      const [entry] = applyProviderInstanceSettings(entries, {
        providerInstances: {},
        providers: {} as never,
      });

      expect(entry?.enabled).toBe(false);
    },
  );

  it("uses settings for a configured custom instance named constructor", () => {
    const instanceId = ProviderInstanceId.make("constructor");
    const entries = deriveProviderInstanceEntries([
      provider({
        provider: ProviderDriverKind.make("claudeAgent"),
        instanceId,
      }),
    ]);
    const [entry] = applyProviderInstanceSettings(entries, {
      providerInstances: {
        [instanceId]: {
          driver: ProviderDriverKind.make("claudeAgent"),
          enabled: false,
        },
      },
      providers: {} as never,
    });

    expect(entry?.enabled).toBe(false);
  });

  it("treats a removed default instance for a fork driver as disabled", () => {
    const driver = ProviderDriverKind.make("constructor");
    const entries = deriveProviderInstanceEntries([
      provider({
        provider: driver,
        instanceId: "constructor",
      }),
    ]);
    const [entry] = applyProviderInstanceSettings(entries, {
      providerInstances: {},
      providers: {} as never,
    });

    expect(entry?.isDefault).toBe(true);
    expect(entry?.enabled).toBe(false);
  });

  it("uses legacy settings for a built-in default instance", () => {
    const entries = deriveProviderInstanceEntries([
      provider({
        provider: ProviderDriverKind.make("codex"),
        instanceId: "codex",
      }),
    ]);
    const [entry] = applyProviderInstanceSettings(entries, {
      providerInstances: {},
      providers: { codex: { enabled: false } } as never,
    });

    expect(entry?.enabled).toBe(false);
  });
});

describe("deriveProviderInstanceEntries", () => {
  it("uses explicit instance id and driver kind from the snapshot", () => {
    const snapshot = provider({
      provider: ProviderDriverKind.make("codex"),
      instanceId: "codex_personal",
    });
    const [entry] = deriveProviderInstanceEntries([snapshot]);

    expect(entry?.instanceId).toBe("codex_personal");
    expect(entry?.driverKind).toBe("codex");
    expect(entry?.isDefault).toBe(false);
  });
});

describe("deriveProviderEntriesByEnvironment", () => {
  it("keeps same-id default instances distinct per environment", () => {
    const byEnvironment = deriveProviderEntriesByEnvironment([
      [
        "local",
        [
          provider({
            provider: ProviderDriverKind.make("claude"),
            instanceId: "claude",
            displayName: "Claude Local",
            accentColor: "#112233",
          }),
        ],
      ],
      [
        "remote",
        [
          provider({
            provider: ProviderDriverKind.make("claude"),
            instanceId: "claude",
            displayName: "Claude Remote",
            accentColor: "#445566",
          }),
        ],
      ],
    ]);

    expect(byEnvironment.get("local")?.get("claude")?.displayName).toBe("Claude Local");
    expect(byEnvironment.get("local")?.get("claude")?.accentColor).toBe("#112233");
    expect(byEnvironment.get("remote")?.get("claude")?.displayName).toBe("Claude Remote");
    expect(byEnvironment.get("remote")?.get("claude")?.accentColor).toBe("#445566");
  });

  it("never falls back to another environment's instances", () => {
    const byEnvironment = deriveProviderEntriesByEnvironment([
      ["local", [provider({ provider: ProviderDriverKind.make("codex"), instanceId: "codex" })]],
      ["empty", []],
    ]);

    expect(byEnvironment.get("empty")?.get("codex")).toBeUndefined();
    // Every environment gets its own bucket, so an absent lookup is a real
    // "this environment has no such instance", not a missing key.
    expect(byEnvironment.get("empty")?.size).toBe(0);
  });
});

describe("resolveSelectableProviderInstance", () => {
  it("returns the requested instance when it is enabled and available", () => {
    const requested = ProviderInstanceId.make("claude_work");
    const providers = [
      provider({ provider: ProviderDriverKind.make("codex"), instanceId: "codex" }),
      provider({ provider: ProviderDriverKind.make("claudeAgent"), instanceId: requested }),
    ];

    expect(resolveSelectableProviderInstance(providers, requested)).toBe(requested);
  });

  it("falls back to the first enabled and available instance", () => {
    const disabled = ProviderInstanceId.make("codex");
    const fallback = ProviderInstanceId.make("claudeAgent");
    const providers = [
      provider({
        provider: ProviderDriverKind.make("codex"),
        instanceId: disabled,
        enabled: false,
      }),
      provider({ provider: ProviderDriverKind.make("claudeAgent"), instanceId: fallback }),
    ];

    expect(resolveSelectableProviderInstance(providers, disabled)).toBe(fallback);
  });

  it("prefers a ready instance over an enabled one whose driver cannot start", () => {
    const notInstalled = ProviderInstanceId.make("codex");
    const ready = ProviderInstanceId.make("claudeAgent");
    const providers = [
      provider({
        provider: ProviderDriverKind.make("codex"),
        instanceId: notInstalled,
        status: "error",
      }),
      provider({ provider: ProviderDriverKind.make("claudeAgent"), instanceId: ready }),
    ];

    expect(resolveSelectableProviderInstance(providers, undefined)).toBe(ready);
  });

  it("prefers an unprobed (warning) instance over one whose probe errored", () => {
    const notInstalled = ProviderInstanceId.make("codex");
    const unprobed = ProviderInstanceId.make("claudeAgent");
    const providers = [
      provider({
        provider: ProviderDriverKind.make("codex"),
        instanceId: notInstalled,
        status: "error",
      }),
      provider({
        provider: ProviderDriverKind.make("claudeAgent"),
        instanceId: unprobed,
        status: "warning",
      }),
    ];

    expect(resolveSelectableProviderInstance(providers, undefined)).toBe(unprobed);
  });

  it("keeps a requested instance even when its probe errored", () => {
    const requested = ProviderInstanceId.make("codex");
    const providers = [
      provider({
        provider: ProviderDriverKind.make("codex"),
        instanceId: requested,
        status: "error",
      }),
      provider({ provider: ProviderDriverKind.make("claudeAgent"), instanceId: "claudeAgent" }),
    ];

    expect(resolveSelectableProviderInstance(providers, requested)).toBe(requested);
  });

  it("does not invent an errored instance as a new-user default", () => {
    const notInstalled = ProviderInstanceId.make("codex");
    const providers = [
      provider({
        provider: ProviderDriverKind.make("codex"),
        instanceId: notInstalled,
        status: "error",
      }),
    ];

    expect(resolveSelectableProviderInstance(providers, undefined)).toBeUndefined();
  });

  it("does not return disabled, unavailable, or unknown instances when none are sendable", () => {
    const disabled = ProviderInstanceId.make("codex");
    const unavailable = ProviderInstanceId.make("claudeAgent");
    const unknown = ProviderInstanceId.make("removed_instance");
    const providers = [
      provider({
        provider: ProviderDriverKind.make("codex"),
        instanceId: disabled,
        enabled: false,
      }),
      provider({
        provider: ProviderDriverKind.make("claudeAgent"),
        instanceId: unavailable,
        availability: "unavailable",
      }),
    ];

    expect(resolveSelectableProviderInstance(providers, disabled)).toBeUndefined();
    expect(resolveSelectableProviderInstance(providers, unavailable)).toBeUndefined();
    expect(resolveSelectableProviderInstance(providers, unknown)).toBeUndefined();
  });
});

describe("getDefaultProviderInstanceModel", () => {
  it("uses the instance's own models, not the default instance of the kind", () => {
    const providers = [
      provider({
        provider: ProviderDriverKind.make("claudeAgent"),
        instanceId: "claude_openrouter",
        models: [model("openai/gpt-5.5", true), model("claude-opus-4-8")],
      }),
      provider({
        provider: ProviderDriverKind.make("claudeAgent"),
        instanceId: "claudeAgent",
        models: [model("claude-sonnet-5")],
      }),
    ];

    expect(
      getDefaultProviderInstanceModel(providers, ProviderInstanceId.make("claude_openrouter")),
    ).toBe("claude-opus-4-8");
  });

  it("falls back to the driver default when the instance reports no models", () => {
    const providers = [
      provider({ provider: ProviderDriverKind.make("claudeAgent"), instanceId: "claudeAgent" }),
    ];

    const resolved = getDefaultProviderInstanceModel(
      providers,
      ProviderInstanceId.make("claudeAgent"),
    );
    expect(typeof resolved).toBe("string");
    expect(resolved?.length).toBeGreaterThan(0);
  });

  it("honors the instance's declared default before model-list order", () => {
    const providers = [
      provider({
        provider: ProviderDriverKind.make("claudeAgent"),
        instanceId: "claudeAgent",
        models: [model("claude-sonnet-5"), model("claude-opus-4-8", false, true)],
      }),
    ];

    expect(getDefaultProviderInstanceModel(providers, ProviderInstanceId.make("claudeAgent"))).toBe(
      "claude-opus-4-8",
    );
  });

  it("returns undefined for an unknown instance", () => {
    expect(
      getDefaultProviderInstanceModel([], ProviderInstanceId.make("removed_instance")),
    ).toBeUndefined();
  });
});

describe("resolveDefaultProviderModelSelection", () => {
  it.each([
    ["codex", "codex", "gpt-5.6"],
    ["claudeAgent", "claudeAgent", "claude-fable-5"],
    ["cursor", "cursor", "composer-2"],
  ])("uses the only available %s instance", (driver, instanceId, modelSlug) => {
    const providers = [
      provider({
        provider: ProviderDriverKind.make(driver),
        instanceId,
        models: [model(modelSlug, false, true)],
      }),
    ];

    expect(resolveDefaultProviderModelSelection(providers, null)).toEqual({
      instanceId,
      model: modelSlug,
    });
  });

  it("preserves a valid stored selection including its options", () => {
    const providers = [
      provider({
        provider: ProviderDriverKind.make("claudeAgent"),
        instanceId: "claudeAgent",
        models: [model("claude-opus-4-8")],
      }),
    ];
    const stored = {
      instanceId: ProviderInstanceId.make("claudeAgent"),
      model: "custom-model",
      options: [{ id: "effort", value: "high" }],
    };

    expect(resolveDefaultProviderModelSelection(providers, stored)).toBe(stored);
  });

  it("heals a stale Kilo model to the live command catalog default", () => {
    const providers = [
      provider({
        provider: ProviderDriverKind.make("kilo"),
        instanceId: "kilo",
        models: [model("kilo/live", false, true), model("kilo/other")],
        hasAuthoritativeModelCatalog: true,
      }),
    ];

    expect(
      resolveDefaultProviderModelSelection(providers, {
        instanceId: ProviderInstanceId.make("kilo"),
        model: "kilo/stale",
        options: [{ id: "reasoning", value: "high" }],
      }),
    ).toEqual({ instanceId: "kilo", model: "kilo/live" });
  });

  it("preserves a Kilo selection while its live catalog is unavailable", () => {
    const providers = [
      provider({
        provider: ProviderDriverKind.make("kilo"),
        instanceId: "kilo",
        status: "warning",
        models: [],
        hasAuthoritativeModelCatalog: true,
      }),
    ];
    const stored = {
      instanceId: ProviderInstanceId.make("kilo"),
      model: "kilo/offline",
    };

    expect(resolveDefaultProviderModelSelection(providers, stored)).toBe(stored);
  });

  it("replaces a stale stored instance with the first ready instance and its model", () => {
    const providers = [
      provider({
        provider: ProviderDriverKind.make("codex"),
        instanceId: "codex",
        status: "warning",
        models: [model("gpt-5.6")],
      }),
      provider({
        provider: ProviderDriverKind.make("claudeAgent"),
        instanceId: "claudeAgent",
        models: [model("claude-opus-4-8", false, true)],
      }),
    ];

    expect(
      resolveDefaultProviderModelSelection(providers, {
        instanceId: ProviderInstanceId.make("removed-provider"),
        model: "stale-model",
      }),
    ).toEqual({ instanceId: "claudeAgent", model: "claude-opus-4-8" });
  });

  it.each([{ enabled: false }, { availability: "unavailable" as const }])(
    "replaces an unavailable stored instance deterministically",
    (requestedState) => {
      const providers = [
        provider({
          provider: ProviderDriverKind.make("codex"),
          instanceId: "codex",
          models: [model("gpt-5.6")],
          ...requestedState,
        }),
        provider({
          provider: ProviderDriverKind.make("claudeAgent"),
          instanceId: "claudeAgent",
          models: [model("claude-opus-4-8", false, true)],
        }),
      ];

      expect(
        resolveDefaultProviderModelSelection(providers, {
          instanceId: ProviderInstanceId.make("codex"),
          model: "gpt-5.6",
        }),
      ).toEqual({ instanceId: "claudeAgent", model: "claude-opus-4-8" });
    },
  );

  it("returns no selection for empty, disabled, unavailable, or error-only profiles", () => {
    expect(resolveDefaultProviderModelSelection([], null)).toBeNull();
    expect(
      resolveDefaultProviderModelSelection(
        [
          provider({
            provider: ProviderDriverKind.make("codex"),
            instanceId: "codex",
            enabled: false,
          }),
        ],
        null,
      ),
    ).toBeNull();
    expect(
      resolveDefaultProviderModelSelection(
        [
          provider({
            provider: ProviderDriverKind.make("codex"),
            instanceId: "codex",
            availability: "unavailable",
          }),
        ],
        null,
      ),
    ).toBeNull();
    expect(
      resolveDefaultProviderModelSelection(
        [
          provider({
            provider: ProviderDriverKind.make("codex"),
            instanceId: "codex",
            status: "error",
          }),
        ],
        null,
      ),
    ).toBeNull();
  });
});

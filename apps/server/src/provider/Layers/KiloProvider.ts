import {
  type KiloSettings,
  type ModelCapabilities,
  ProviderDriverKind,
  providerDriverSupportsTextGeneration,
  type ServerProvider,
  type ServerProviderModel,
  type ServerProviderSkill,
} from "@t3tools/contracts";
import { causeErrorTag } from "@t3tools/shared/observability";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { compareSemverVersions } from "@t3tools/shared/semver";

import {
  buildServerProvider,
  isCommandMissingCause,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";
import {
  hardenKiloProbeEnvironment,
  KILO_PROVIDER_DEFAULT_MODEL_ID,
} from "../acp/KiloAcpSupport.ts";

const KILO_PRESENTATION = {
  displayName: "Kilo Code",
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
  supportsConversationRollback: false,
  requiresNewThreadForModelChange: false,
  // Derived from the contracts-level driver capability so settings-time
  // resolution (which has no snapshot) and snapshot consumers agree.
  supportsTextGeneration: providerDriverSupportsTextGeneration(ProviderDriverKind.make("kilo")),
  // `kilo models` is the complete dispatchable catalog: no custom models,
  // out-of-catalog selections heal to the default, dispatch waits for a
  // ready nonempty catalog.
  hasAuthoritativeModelCatalog: true,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const VERSION_PROBE_TIMEOUT_MS = 4_000;
const KILO_MODEL_DISCOVERY_TIMEOUT_MS = 15_000;
const MAX_KILO_MODEL_COUNT = 5_000;
const MAX_KILO_MODEL_ID_LENGTH = 512;
// Bounds collection while `kilo models` streams, before parsing can reject
// excess IDs. Comfortably above the largest valid catalog
// (5,000 IDs x 512 chars is ~2.6 MB).
const KILO_MODELS_MAX_OUTPUT_BYTES = 8 * 1024 * 1024;
const KILO_VERSION_MAX_OUTPUT_BYTES = 256 * 1024;
export const MINIMUM_SUPPORTED_KILO_VERSION = "7.4.23";

const KILO_CLI_VERSION_PATTERN = /\b(\d+\.\d+\.\d+(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?)\b/;

/**
 * Unlike `parseGenericCliVersion`, keeps the prerelease suffix so
 * `7.4.23-beta.1` compares below `7.4.23` and is rejected by the minimum
 * version gate instead of masquerading as a supported release.
 */
export function parseKiloCliVersion(output: string): string | null {
  return output.match(KILO_CLI_VERSION_PATTERN)?.[1] ?? null;
}

export function isSupportedKiloVersion(version: string | null): version is string {
  return version !== null && compareSemverVersions(version, MINIMUM_SUPPORTED_KILO_VERSION) >= 0;
}

export const buildInitialKiloProviderSnapshot = Effect.fn("buildInitialKiloProviderSnapshot")(
  function* (kiloSettings: KiloSettings): Effect.fn.Return<ServerProviderDraft> {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models: ReadonlyArray<ServerProviderModel> = [];

    if (!kiloSettings.enabled) {
      return buildServerProvider({
        presentation: KILO_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Kilo Code is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: KILO_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Kilo CLI availability...",
      },
    });
  },
);

export type KiloModelsParseResult =
  | { readonly ok: true; readonly modelIds: ReadonlyArray<string> }
  | { readonly ok: false; readonly message: string };

/** Parse the newline-delimited IDs printed by the official `kilo models` command. */
export function parseKiloModelsOutput(stdout: string): KiloModelsParseResult {
  const modelIds: string[] = [];
  const seen = new Set<string>();
  for (const rawLine of stdout.split(/\r?\n/u)) {
    const modelId = rawLine.trim();
    if (!modelId) continue;
    const separatorIndex = modelId.indexOf("/");
    if (
      modelId.length > MAX_KILO_MODEL_ID_LENGTH ||
      /\s|\p{Cc}/u.test(modelId) ||
      separatorIndex <= 0 ||
      separatorIndex === modelId.length - 1
    ) {
      return { ok: false, message: "`kilo models` returned a malformed model ID." };
    }
    if (seen.has(modelId)) continue;
    if (modelIds.length >= MAX_KILO_MODEL_COUNT) {
      return { ok: false, message: "`kilo models` returned too many model IDs." };
    }
    seen.add(modelId);
    modelIds.push(modelId);
  }
  return modelIds.length > 0
    ? { ok: true, modelIds }
    : { ok: false, message: "`kilo models` returned an empty model catalog." };
}

export const runKiloModelsCommand = Effect.fn("runKiloModelsCommand")(function* (
  kiloSettings: KiloSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const command = kiloSettings.binaryPath || "kilo";
  const hardenedEnvironment = hardenKiloProbeEnvironment(environment);
  const spawnCommand = yield* resolveSpawnCommand(command, ["models"], {
    env: hardenedEnvironment,
  });
  return yield* spawnAndCollect(
    command,
    ChildProcess.make(spawnCommand.command, spawnCommand.args, {
      env: hardenedEnvironment,
      forceKillAfter: Duration.seconds(2),
      shell: spawnCommand.shell,
    }),
    { maxOutputBytes: KILO_MODELS_MAX_OUTPUT_BYTES },
  );
});

const decodeKiloSkills = Schema.decodeUnknownSync(
  Schema.Array(Schema.Struct({
    name: Schema.NonEmptyString,
    description: Schema.String,
    location: Schema.NonEmptyString,
  })),
);

export function parseKiloSkillsOutput(stdout: string): ReadonlyArray<ServerProviderSkill> {
  return decodeKiloSkills(JSON.parse(stdout)).map((skill) => ({
    name: skill.name,
    ...(skill.description.trim() ? { description: skill.description.trim() } : {}),
    path: skill.location,
    enabled: true,
  }));
}

const discoverKiloSkills = Effect.fn("discoverKiloSkills")(function* (
  kiloSettings: KiloSettings,
  environment: NodeJS.ProcessEnv,
) {
  const command = kiloSettings.binaryPath || "kilo";
  const skillEnvironment = { ...hardenKiloProbeEnvironment(environment) };
  // Metadata discovery must include the same external skill directories as
  // sessions. Plugins and skill shell execution stay disabled for this probe.
  delete skillEnvironment.KILO_DISABLE_EXTERNAL_SKILLS;
  const spawnCommand = yield* resolveSpawnCommand(command, ["debug", "skill"], {
    env: skillEnvironment,
  });
  const result = yield* spawnAndCollect(
    command,
    ChildProcess.make(spawnCommand.command, spawnCommand.args, {
      env: skillEnvironment,
      forceKillAfter: Duration.seconds(2),
      shell: spawnCommand.shell,
    }),
    { maxOutputBytes: 16 * 1024 * 1024 },
  );
  if (result.code !== 0) return [];
  return yield* Effect.try(() => parseKiloSkillsOutput(result.stdout));
});

export const runKiloVersionCommand = Effect.fn("runKiloVersionCommand")(function* (
  kiloSettings: KiloSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const command = kiloSettings.binaryPath || "kilo";
  const spawnCommand = yield* resolveSpawnCommand(command, ["--version"], {
    env: environment,
  });
  return yield* spawnAndCollect(
    command,
    ChildProcess.make(spawnCommand.command, spawnCommand.args, {
      env: environment,
      forceKillAfter: Duration.seconds(2),
      shell: spawnCommand.shell,
    }),
    { maxOutputBytes: KILO_VERSION_MAX_OUTPUT_BYTES },
  );
});

export const checkKiloProviderStatus = Effect.fn("checkKiloProviderStatus")(function* (
  kiloSettings: KiloSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels: ReadonlyArray<ServerProviderModel> = [];
  const probeFailure = (probe: {
    readonly installed: boolean;
    readonly version: string | null;
    readonly message: string;
  }) =>
    buildServerProvider({
      presentation: KILO_PRESENTATION,
      enabled: kiloSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: { ...probe, status: "error", auth: { status: "unknown" } },
    });

  if (!kiloSettings.enabled) {
    return buildServerProvider({
      presentation: KILO_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Kilo Code is disabled in T3 Code settings.",
      },
    });
  }

  const versionResult = yield* runKiloVersionCommand(kiloSettings, environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* Effect.logWarning("Kilo CLI health check failed.", {
      errorTag: error._tag,
    });
    return probeFailure({
      installed: !isCommandMissingCause(error),
      version: null,
      message: isCommandMissingCause(error)
        ? "Kilo CLI (`kilo`) is not installed or not on PATH. Install it with `npm install -g @kilocode/cli`."
        : "Failed to execute Kilo CLI health check.",
    });
  }

  if (Option.isNone(versionResult.success)) {
    return probeFailure({
      installed: true,
      version: null,
      message: "Kilo CLI is installed but timed out while running `kilo --version`.",
    });
  }

  const versionOutput = versionResult.success.value;
  const version = parseKiloCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    yield* Effect.logWarning("Kilo CLI version probe exited with a non-zero status.", {
      exitCode: versionOutput.code,
      stdoutLength: versionOutput.stdout.length,
      stderrLength: versionOutput.stderr.length,
    });
    return probeFailure({
      installed: true,
      version,
      message: "Kilo CLI is installed but failed to run.",
    });
  }

  if (!version) {
    return probeFailure({
      installed: true,
      version: null,
      message: `Unable to determine Kilo CLI version from \`kilo --version\` output. T3 Code requires Kilo v${MINIMUM_SUPPORTED_KILO_VERSION} or newer.`,
    });
  }

  if (!isSupportedKiloVersion(version)) {
    return probeFailure({
      installed: true,
      version,
      message: `Kilo v${version} is too old. Upgrade to v${MINIMUM_SUPPORTED_KILO_VERSION} or newer with \`kilo upgrade\`.`,
    });
  }

  const discoveryExit = yield* runKiloModelsCommand(kiloSettings, environment).pipe(
    Effect.timeoutOption(KILO_MODEL_DISCOVERY_TIMEOUT_MS),
    Effect.exit,
  );
  if (Exit.isFailure(discoveryExit)) {
    yield* Effect.logWarning("Kilo model discovery command failed", {
      errorTag: causeErrorTag(discoveryExit.cause),
    });
    return probeFailure({
      installed: true,
      version,
      message: "Kilo CLI is installed but `kilo models` failed. Check server logs for details.",
    });
  }
  if (Option.isNone(discoveryExit.value)) {
    yield* Effect.logWarning(
      `Kilo model discovery timed out after ${KILO_MODEL_DISCOVERY_TIMEOUT_MS}ms.`,
    );
    return probeFailure({
      installed: true,
      version,
      message: `Kilo CLI is installed but \`kilo models\` timed out after ${KILO_MODEL_DISCOVERY_TIMEOUT_MS}ms.`,
    });
  }
  const discoveryResult = discoveryExit.value.value;
  if (discoveryResult.code !== 0) {
    yield* Effect.logWarning("Kilo model discovery exited with a non-zero status.", {
      exitCode: discoveryResult.code,
      stdoutLength: discoveryResult.stdout.length,
      stderrLength: discoveryResult.stderr.length,
    });
    return probeFailure({
      installed: true,
      version,
      message: "Kilo CLI is installed but `kilo models` exited unsuccessfully.",
    });
  }
  const parsedModels = parseKiloModelsOutput(discoveryResult.stdout);
  if (!parsedModels.ok) {
    return probeFailure({
      installed: true,
      version,
      message: parsedModels.message,
    });
  }
  const models: ReadonlyArray<ServerProviderModel> = [
    {
      slug: KILO_PROVIDER_DEFAULT_MODEL_ID,
      name: "Kilo provider default",
      isCustom: false,
      isDefault: true,
      capabilities: EMPTY_CAPABILITIES,
    },
    ...parsedModels.modelIds.map((modelId): ServerProviderModel => ({
      slug: modelId,
      name: modelId,
      isCustom: false,
      capabilities: EMPTY_CAPABILITIES,
    })),
  ];

  const skills = yield* discoverKiloSkills(kiloSettings, environment).pipe(
    Effect.timeoutOption(KILO_MODEL_DISCOVERY_TIMEOUT_MS),
    Effect.map(Option.getOrElse((): ReadonlyArray<ServerProviderSkill> => [])),
    Effect.catchCause(() => Effect.succeed([] as ReadonlyArray<ServerProviderSkill>)),
  );

  return buildServerProvider({
    presentation: KILO_PRESENTATION,
    enabled: kiloSettings.enabled,
    checkedAt,
    models,
    skills,
    probe: {
      installed: true,
      version,
      status: "ready",
      auth: { status: "unknown" },
    },
  });
});

export const enrichKiloSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  readonly httpClient: HttpClient.HttpClient;
}): Effect.Effect<void> => {
  const { snapshot, publishSnapshot } = input;

  return enrichProviderSnapshotWithVersionAdvisory(snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, input.httpClient),
    Effect.flatMap((enrichedSnapshot) => publishSnapshot(enrichedSnapshot)),
    Effect.catchCause((cause) =>
      Effect.logWarning("Kilo version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
};

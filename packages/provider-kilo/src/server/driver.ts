import { KiloSettings } from "../settings.ts";
import { ProviderDriverKind, TextGenerationError, type ServerProvider } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";

import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import { makeKiloSdkAdapter, type KiloAdapterEnv } from "./adapter.ts";
import {
  buildInitialKiloProviderSnapshot,
  checkKiloProviderStatus,
  enrichKiloSnapshot,
} from "./status.ts";
import { ProviderEventLoggers } from "@t3tools/provider-core/server/ProviderEventLoggers";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import type { ServerProviderDraft } from "@t3tools/provider-core/server/snapshotProbe";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import {
  makeCachedProviderMaintenanceResolution,
  normalizeCommandPath,
  resolveProviderMaintenanceCapabilitiesEffect,
  makePackageManagedProviderMaintenanceResolver,
} from "@t3tools/provider-core/server/maintenanceResolver";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "@t3tools/provider-core/server/snapshotSettings";
const decodeKiloSettings = Schema.decodeSync(KiloSettings);

const DRIVER_KIND = ProviderDriverKind.make("kilo");

export function isKiloNativeCommandPath(commandPath: string): boolean {
  const normalized = normalizeCommandPath(commandPath);
  return normalized.endsWith("/.kilo/bin/kilo") || normalized.endsWith("/.kilo/bin/kilo.exe");
}

const UPDATE = makePackageManagedProviderMaintenanceResolver({
  provider: DRIVER_KIND,
  npmPackageName: "@kilocode/cli",
  nativeUpdate: {
    args: ["upgrade"],
    isCommandPath: isKiloNativeCommandPath,
  },
});

export type KiloDriverEnv =
  | ProviderHost.ProviderHost
  | ProviderLatestVersions.ProviderLatestVersions
  | KiloAdapterEnv
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | HttpClient.HttpClient
  | Path.Path
  | ProviderEventLoggers;

const withInstanceIdentity =
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: ServerProviderDraft): ServerProvider => ({
    ...snapshot,
    instanceId: input.instanceId,
    driver: DRIVER_KIND,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

export const KiloDriver: ProviderDriver<KiloSettings, KiloDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Kilo Code",
    supportsMultipleInstances: true,
  },
  configSchema: KiloSettings,
  defaultConfig: (): KiloSettings => decodeKiloSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const httpClient = yield* HttpClient.HttpClient;
      const latestVersions = yield* ProviderLatestVersions.ProviderLatestVersions;
      const eventLoggers = yield* ProviderEventLoggers;
      const processEnv = yield* mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies KiloSettings;
      const resolveMaintenance = yield* makeCachedProviderMaintenanceResolution(
        resolveProviderMaintenanceCapabilitiesEffect(UPDATE, {
          binaryPath: effectiveConfig.binaryPath,
          env: processEnv,
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provideService(FileSystem.FileSystem, yield* FileSystem.FileSystem),
          Effect.provideService(Path.Path, yield* Path.Path),
        ),
      );

      const orchestrationAdapter = yield* makeKiloSdkAdapter(effectiveConfig, {
        environment: processEnv,
        ...(eventLoggers.native ? { nativeEventLogger: eventLoggers.native } : {}),
        instanceId,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to initialize Kilo native adapter.",
              cause,
            }),
        ),
      );
      const checkProvider = checkKiloProviderStatus(effectiveConfig, processEnv).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );

      const snapshotSettings = yield* makeProviderSnapshotSettingsSource(effectiveConfig);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<KiloSettings>>({
        resolveMaintenance,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialKiloProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
        enrichSnapshot: ({ settings, snapshot: currentSnapshot, publishSnapshot }) =>
          resolveMaintenance()
            .pipe(
              Effect.flatMap((maintenanceCapabilities) =>
                enrichKiloSnapshot({
                  snapshot: currentSnapshot,
                  maintenanceCapabilities,
                  enableProviderUpdateChecks: settings.enableProviderUpdateChecks,
                  publishSnapshot,
                  httpClient,
                }),
              ),
            )
            .pipe(
              Effect.provideService(ProviderLatestVersions.ProviderLatestVersions, latestVersions),
            ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build Kilo snapshot.",
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        orchestrationAdapter,
        textGeneration: {
          generateCommitMessage: () =>
            Effect.fail(
              new TextGenerationError({
                operation: "generateCommitMessage",
                detail: "Kilo does not support application text generation.",
              }),
            ),
          generatePrContent: () =>
            Effect.fail(
              new TextGenerationError({
                operation: "generatePrContent",
                detail: "Kilo does not support application text generation.",
              }),
            ),
          generateBranchName: () =>
            Effect.fail(
              new TextGenerationError({
                operation: "generateBranchName",
                detail: "Kilo does not support application text generation.",
              }),
            ),
          generateThreadTitle: () =>
            Effect.fail(
              new TextGenerationError({
                operation: "generateThreadTitle",
                detail: "Kilo does not support application text generation.",
              }),
            ),
          generateReviewGuide: () =>
            Effect.fail(
              new TextGenerationError({
                operation: "generateReviewGuide",
                detail: "Kilo does not support application text generation.",
              }),
            ),
        },
      } satisfies ProviderInstance;
    }),
};

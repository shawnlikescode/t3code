import {
  ProviderDriverKind,
  DEFAULT_MODEL,
  DEFAULT_TEXT_GENERATION_MODEL,
} from "@t3tools/contracts";
import { createKiloClient } from "@kilocode/sdk/v2";
import type { OpencodeClient } from "@opencode-ai/sdk/v2";
import { OpenCodeSettings } from "@t3tools/provider-opencode/settings";
import * as OpenCodeRuntime from "@t3tools/provider-opencode/server/OpenCodeRuntime";
import * as OpenCodeServerLedger from "@t3tools/provider-opencode/server/OpenCodeServerLedger";
import * as OpenCodeAdapter from "@t3tools/provider-opencode/server/adapter";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import * as Effect from "effect/Effect";
import * as Crypto from "effect/Crypto";
import * as NetService from "@t3tools/shared/Net";
import * as Schema from "effect/Schema";
import type { KiloSettings } from "../settings.ts";
import {
  enforceKiloInteractiveModeEnvironment,
  KILO_PROVIDER_DEFAULT_MODEL_ID,
} from "./support.ts";
const decodeSettings = Schema.decodeSync(OpenCodeSettings);
export const makeKiloSdkAdapter = Effect.fn("makeKiloSdkAdapter")(function* (
  settings: KiloSettings,
  options: Pick<
    OpenCodeAdapter.OpenCodeAdapterV2Options,
    "environment" | "instanceId" | "nativeEventLogger"
  >,
) {
  const host = yield* ProviderHost.ProviderHost;
  const password = yield* (yield* Crypto.Crypto).randomUUIDv4;
  const runtime = yield* OpenCodeRuntime.makeOpenCodeRuntime({
    serverRequirement: { name: "Kilo", minimumVersion: "7.4.23" },
    createClient: (input) =>
      createKiloClient({
        baseUrl: input.baseUrl,
        directory: input.directory,
        throwOnError: true,
        headers: { Authorization: `Basic ${Buffer.from(`kilo:${password}`).toString("base64")}` },
      }) as unknown as OpencodeClient,
  }).pipe(
    Effect.provide([
      NetService.layer,
      OpenCodeServerLedger.layer({ stateDir: host.paths.stateDir }),
    ]),
  );
  return yield* OpenCodeAdapter.makeOpenCodeAdapterV2({
    ...options,
    settings: decodeSettings({ binaryPath: settings.binaryPath, serverPassword: password }),
    provider: ProviderDriverKind.make("kilo"),
    defaultAgent: "code",
    steeringRequiresAssistantAcknowledgement: true,
    interactivePermissionReplies: true,
    sessionEnvironment: (policy) => ({
      ...enforceKiloInteractiveModeEnvironment(options.environment, policy.runtimeMode),
      KILO_SERVER_PASSWORD: password,
      OPENCODE_SERVER_PASSWORD: password,
    }),
    resolveModelSlug: async (client, model) =>
      ![
        KILO_PROVIDER_DEFAULT_MODEL_ID,
        DEFAULT_MODEL,
        DEFAULT_TEXT_GENERATION_MODEL,
        "default",
        "gpt-5.6-luna",
      ].includes(model)
        ? model
        : (await client.config.get()).data?.model || "kilo/kilo-auto/balanced",
    permissionRules: (policy) => {
      const rules = OpenCodeAdapter.openCodePermissionRules(policy);
      return rules.some((r) => r.permission === "*" && r.pattern === "*" && r.action === "allow")
        ? [
            ...rules,
            { permission: "read", pattern: "*.env", action: "allow" },
            { permission: "read", pattern: "*.env.*", action: "allow" },
          ]
        : [...rules, { permission: "task", pattern: "*", action: "deny" }];
    },
    autoApprovePermission: (request, policy) =>
      policy.runtimeMode === "full-access" &&
      !request.metadata.skillShell &&
      !request.metadata.sandboxEscalation,
  }).pipe(Effect.provideService(OpenCodeRuntime.OpenCodeRuntime, runtime));
});
export type KiloAdapterEnv = Effect.Services<ReturnType<typeof makeKiloSdkAdapter>>;

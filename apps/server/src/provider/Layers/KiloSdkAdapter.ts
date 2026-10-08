import {
  DEFAULT_MODEL,
  DEFAULT_TEXT_GENERATION_MODEL,
  OpenCodeSettings,
  ProviderDriverKind,
  type KiloSettings,
} from "@t3tools/contracts";
import { createKiloClient } from "@kilocode/sdk/v2";
import type { OpencodeClient } from "@opencode-ai/sdk/v2";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as NetService from "@t3tools/shared/Net";

import { MINIMUM_SUPPORTED_KILO_VERSION } from "./KiloProvider.ts";
import { makeOpenCodeAdapter, type OpenCodeAdapterLiveOptions } from "./OpenCodeAdapter.ts";
import {
  buildOpenCodePermissionRules,
  makeOpenCodeRuntime,
  OpenCodeRuntime,
} from "../opencodeRuntime.ts";
import {
  enforceKiloInteractiveModeEnvironment,
  KILO_PROVIDER_DEFAULT_MODEL_ID,
} from "../acp/KiloAcpSupport.ts";

const decodeSettings = Schema.decodeSync(OpenCodeSettings);
const PROVIDER = ProviderDriverKind.make("kilo");

/** Reuse T3's native server adapter; only Kilo's SDK, credentials, and policy differ. */
export const makeKiloSdkAdapter = (settings: KiloSettings, options?: OpenCodeAdapterLiveOptions) =>
  Effect.gen(function* () {
    const password = yield* (yield* Crypto.Crypto).randomUUIDv4;
    const runtime = yield* makeOpenCodeRuntime({
      serverRequirement: { name: "Kilo", minimumVersion: MINIMUM_SUPPORTED_KILO_VERSION },
      createClient: (input) => {
        const client = createKiloClient({
          baseUrl: input.baseUrl,
          directory: input.directory,
          throwOnError: true,
          headers: {
            Authorization: `Basic ${Buffer.from(`kilo:${input.serverPassword ?? password}`).toString("base64")}`,
          },
        });
        // Kilo forks the OpenCode v2 API. Keep the generated SDK class boundary
        // here; live tests exercise the shared methods and streaming contract.
        return client as unknown as OpencodeClient;
      },
    }).pipe(Effect.provide(NetService.layer));
    const adapter = yield* makeOpenCodeAdapter(
      decodeSettings({
        binaryPath: settings.binaryPath,
        serverPassword: password,
      }),
      {
        ...options,
        provider: PROVIDER,
        // ACP persisted nonce-scoped agent names in user messages. Explicitly
        // select a native agent so resumed prompts never inherit a removed name.
        defaultAgent: "code",
        interactivePermissionReplies: true,
        requiresInteractiveApproval: (request) =>
          Boolean(request.metadata.skillShell || request.metadata.sandboxEscalation),
        resolveModelSlug: async (client, model) => {
          if (
            model &&
            ![
              KILO_PROVIDER_DEFAULT_MODEL_ID,
              "default",
              DEFAULT_MODEL,
              DEFAULT_TEXT_GENERATION_MODEL,
              "gpt-5.6-luna",
            ].includes(model)
          )
            return model;
          const config = await client.config.get();
          return config.data?.model || "kilo/kilo-auto/balanced";
        },
        streamChildActivity: true,
        sessionEnvironment: (mode) => ({
          ...enforceKiloInteractiveModeEnvironment(options?.environment ?? process.env, mode),
          KILO_SERVER_PASSWORD: password,
        }),
        permissionRules: (mode) => {
          const rules = buildOpenCodePermissionRules(mode);
          // Kilo propagates denies, but not asks, into children. Supervised
          // delegation must go through T3 instead of escaping its policy.
          return mode === "full-access"
            ? rules
            : [...rules, { permission: "task", pattern: "*", action: "deny" }];
        },
      },
    ).pipe(Effect.provideService(OpenCodeRuntime, runtime));
    return adapter;
  });

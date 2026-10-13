import { makeBinaryPathSetting, makeProviderSettingsSchema } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
export const KiloSettings = makeProviderSettingsSchema(
  {
    enabled: Schema.Boolean.pipe(
      Schema.withDecodingDefault(Effect.succeed(false)),
      Schema.annotateKey({ providerSettingsForm: { hidden: true } }),
    ),
    binaryPath: makeBinaryPathSetting("kilo").pipe(
      Schema.annotateKey({
        title: "Binary path",
        description: "Path to the Kilo CLI binary.",
        providerSettingsForm: { placeholder: "kilo", clearWhenEmpty: "omit" },
      }),
    ),
  },
  { order: ["binaryPath"] },
);
export type KiloSettings = typeof KiloSettings.Type;

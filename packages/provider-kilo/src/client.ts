import { ProviderDriverKind } from "@t3tools/contracts";
import { defineProviderClient } from "@t3tools/provider-core/client";
import { KiloSettings } from "./settings.ts";
export const kiloClient = defineProviderClient({
  driverKind: ProviderDriverKind.make("kilo"),
  label: "Kilo Code",
  settingsSchema: KiloSettings,
  icon: {
    viewBox: "0 0 24 24",
    fill: { light: "#F5E900", dark: "#F5E900" },
    paths: [
      { d: "M0 0h24v24H0z" },
      { d: "M5 5h3v6l5-6h4l-6 7 6 7h-4l-5-6v6H5z", fill: { light: "#111111", dark: "#111111" } },
    ],
  },
});

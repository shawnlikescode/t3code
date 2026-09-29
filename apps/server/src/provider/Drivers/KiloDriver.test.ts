import { describe, expect, it } from "vite-plus/test";

import { isKiloNativeCommandPath } from "./KiloDriver.ts";

describe("KiloDriver maintenance path classification", () => {
  it("recognizes only the standalone Kilo installer directory", () => {
    expect(isKiloNativeCommandPath("/Users/dev/.kilo/bin/kilo")).toBe(true);
    expect(isKiloNativeCommandPath("C:\\Users\\dev\\.kilo\\bin\\kilo.exe")).toBe(true);
    expect(isKiloNativeCommandPath("/Users/dev/.kilocode/bin/kilo")).toBe(false);
  });

  it("keeps package-manager bin directories on package-managed updates", () => {
    expect(isKiloNativeCommandPath("/usr/local/bin/kilo")).toBe(false);
    expect(isKiloNativeCommandPath("/opt/homebrew/bin/kilo")).toBe(false);
    expect(isKiloNativeCommandPath("C:\\Users\\dev\\AppData\\Roaming\\npm\\kilo.cmd")).toBe(false);
  });
});

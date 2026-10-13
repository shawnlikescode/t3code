import type { RuntimeMode } from "@t3tools/contracts";
export const KILO_PROVIDER_DEFAULT_MODEL_ID = "__t3_provider_default__";
export function hardenKiloProbeEnvironment(environment?: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  return {
    ...(environment ?? process.env),
    KILO_PURE: "1",
    KILO_DISABLE_PROJECT_CONFIG: "1",
    KILO_DISABLE_EXTERNAL_SKILLS: "1",
    KILO_DISABLE_SKILL_SHELL: "1",
  };
}
export function enforceKiloInteractiveModeEnvironment(
  environment: NodeJS.ProcessEnv,
  mode: RuntimeMode,
): NodeJS.ProcessEnv {
  return mode === "full-access"
    ? environment
    : { ...environment, KILO_PURE: "1", KILO_DISABLE_PROJECT_CONFIG: "1" };
}

import type { ProviderUsageReader } from "@t3tools/provider-core/server/usage";
import { readOpenCodeUsage } from "@t3tools/provider-opencode/server/usage";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import type { KiloSettings } from "../settings.ts";

/** Kilo keeps the OpenCode message schema in its own XDG data directory. */
export const kiloUsageReader: ProviderUsageReader<KiloSettings, FileSystem.FileSystem | Path.Path> =
  {
    kind: "scan",
    provider: "kilo",
    scan: Effect.fn("kiloUsageReader.scan")(function* ({ instances, windowStartMs }) {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const hostHome = yield* HostProcess.HomeDirectory;
      const sources = new Map<string, Set<string | undefined>>();
      for (const { environment } of instances) {
        const clean = (value: string | undefined) => value?.replace(/[\r\n]+/g, "");
        const home = clean(environment.HOME) || hostHome;
        const dataHome = clean(environment.XDG_DATA_HOME);
        const root = path.join(
          dataHome && path.isAbsolute(dataHome) ? dataHome : path.join(home, ".local", "share"),
          "kilo",
        );
        const database = environment.KILO_DB;
        if (database === ":memory:") continue;
        const databasePath = database ? path.resolve(root, database) : undefined;
        const dir = databasePath ? path.dirname(databasePath) : root;
        const canonical = yield* fs.realPath(dir).pipe(Effect.orElseSucceed(() => dir));
        const databaseFile = databasePath ? path.basename(databasePath) : undefined;
        const databases = sources.get(canonical) ?? new Set<string | undefined>();
        databases.add(databaseFile);
        sources.set(canonical, databases);
      }
      return yield* Effect.forEach([...sources], ([dir, databases]) =>
        Effect.gen(function* () {
          const results = yield* Effect.forEach([...databases], (databaseFile) =>
            readOpenCodeUsage(dir, windowStartMs, {
              provider: "kilo",
              ...(databaseFile ? { databaseFile } : {}),
            }),
          );
          const error = results.some((result) => result.error);
          const missing = results.every((result) => result.missing);
          const files = [
            ...new Map(
              results.flatMap((result) => result.files).map((file) => [file.path, file]),
            ).values(),
          ];
          return {
            dir,
            files: missing && !error ? null : files,
            status: error ? ("partial" as const) : ("ok" as const),
            ...(error ? { message: "Some Kilo history could not be read." } : {}),
          };
        }),
      );
    }),
  };

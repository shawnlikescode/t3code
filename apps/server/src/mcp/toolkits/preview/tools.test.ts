import { expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { Tool } from "effect/ai";

import { PreviewToolkit } from "./tools.ts";

const schemaHasDescription = (schema: unknown): boolean => {
  if (!schema || typeof schema !== "object") return false;
  const record = schema as Record<string, unknown>;
  if (typeof record.description === "string" && record.description.length > 0) return true;
  return [record.anyOf, record.oneOf, record.allOf]
    .filter(Array.isArray)
    .some((members) => members.some(schemaHasDescription));
};

const schemaHasMultipleAllOfDescriptions = (schema: unknown): boolean => {
  if (!schema || typeof schema !== "object") return false;
  const record = schema as Record<string, unknown>;
  const allOf = Array.isArray(record.allOf) ? record.allOf : [];
  const descriptionCount = allOf.filter(
    (member) =>
      member !== null &&
      typeof member === "object" &&
      typeof (member as Record<string, unknown>).description === "string",
  ).length;
  return descriptionCount > 1 || Object.values(record).some(schemaHasMultipleAllOfDescriptions);
};

it("exports provider-compatible object schemas with described parameters", () => {
  for (const tool of Object.values(PreviewToolkit.tools)) {
    const schema = Tool.getJsonSchema(tool) as {
      readonly type?: unknown;
      readonly properties?: Readonly<Record<string, unknown>>;
      readonly anyOf?: unknown;
      readonly oneOf?: unknown;
    };
    expect(
      tool.description?.length ?? 0,
      `${tool.name} should have a useful description`,
    ).toBeGreaterThan(40);
    expect(schema.type, `${tool.name} must export a top-level object schema`).toBe("object");
    expect(schema.anyOf, `${tool.name} must not export a root anyOf`).toBeUndefined();
    expect(schema.oneOf, `${tool.name} must not export a root oneOf`).toBeUndefined();
    if (tool.name === "preview_navigate") {
      expect(schemaHasMultipleAllOfDescriptions(schema)).toBe(false);
    }
    expect(
      schema.properties?.tabId,
      `${tool.name} must allow an explicit collaborative browser tab target`,
    ).toBeDefined();
    for (const [field, fieldSchema] of Object.entries(schema.properties ?? {})) {
      expect(
        schemaHasDescription(fieldSchema),
        `${tool.name}.${field} should explain what data the agent must pass`,
      ).toBe(true);
    }
  }
});

it("exports exact object result schemas for preview actions", () => {
  const actionNames = [
    "preview_click",
    "preview_type",
    "preview_hover",
    "preview_drag",
    "preview_upload",
    "preview_press",
    "preview_scroll",
    "preview_wait_for",
  ] as const;
  for (const name of actionNames) {
    // Effect's tool schemas follow the decoder default since rc.113 and leave
    // unmodeled result keys open.
    expect(Tool.getJsonSchemaFromSchema(PreviewToolkit.tools[name].successSchema)).toEqual({
      type: "object",
      properties: { toolIcon: expect.any(Object) },
      additionalProperties: true,
      description: "The preview action completed successfully.",
    });
  }
});

const validInputs = {
  preview_status: {},
  preview_open: { url: "https://example.com" },
  preview_dialog: { accept: false },
  preview_navigate: { url: "https://example.com" },
  preview_resize: { mode: "freeform", width: 1920, height: 1080 },
  preview_set_appearance: { colorScheme: "dark" },
  preview_snapshot: { includeImage: false },
  preview_click: { locator: "aria-ref=button1" },
  preview_type: { text: "hello", clear: false },
  preview_hover: { x: 0, y: 0 },
  preview_select: { locator: "select", values: [] },
  preview_drag: { source: "#source", target: "#target" },
  preview_upload: { paths: [] },
  preview_press: { key: "Enter" },
  preview_scroll: { deltaY: 0 },
  preview_evaluate: { expression: "null" },
  preview_wait_for: { text: "Ready" },
  preview_recording_start: {},
  preview_recording_stop: {},
} satisfies Record<keyof typeof PreviewToolkit.tools, object>;

it.each(Object.entries(validInputs))(
  "normalizes strict-client optional nulls for %s",
  (name, input) => {
    const tool = PreviewToolkit.tools[name as keyof typeof PreviewToolkit.tools];
    const json = Tool.getJsonSchema(tool) as {
      properties: Record<string, unknown>;
      required?: string[];
    };
    const nullable = Object.fromEntries(
      Object.keys(json.properties)
        .filter((field) => !json.required?.includes(field))
        .map((field) => [field, null]),
    );
    const decode = Schema.decodeUnknownSync(
      tool.parametersSchema as Schema.Codec<unknown, unknown>,
    );
    expect(decode({ ...nullable, ...input })).toEqual(input);
  },
);

it.each([
  { mode: "preset", preset: "iphone-12-pro", orientation: "portrait", width: null, height: null },
  { mode: "fill", preset: null, orientation: null, width: null, height: null },
])("accepts nullable unused resize fields for $mode", (input) => {
  const decode = Schema.decodeUnknownSync(PreviewToolkit.tools.preview_resize.parametersSchema);
  expect(decode(input)).toEqual(
    Object.fromEntries(Object.entries(input).filter(([, value]) => value !== null)),
  );
});

it.each([
  ["preview_resize", { mode: "freeform", width: 1920, height: 1080, preset: "iphone-se" }],
  ["preview_resize", { mode: "freeform", width: 1920, height: 1080, orientation: "portrait" }],
  ["preview_resize", { mode: "preset", preset: "iphone-se", width: 320, height: 568 }],
  ["preview_resize", { mode: "freeform", width: -1, height: 1080 }],
  ["preview_resize", { mode: "freeform", width: null, height: 1080 }],
  ["preview_resize", { mode: null }],
  ["preview_click", { locator: "button", selector: "button" }],
  [
    "preview_navigate",
    { url: "https://example.com", target: { kind: "environment-port", port: 5173 } },
  ],
  ["preview_wait_for", { text: null }],
  ["preview_scroll", { deltaY: null }],
  ["preview_evaluate", { expression: null }],
  ["preview_dialog", { accept: null }],
] as const)("retains original validation for %s: %j", (name, input) => {
  const schema = PreviewToolkit.tools[name].parametersSchema as Schema.Codec<unknown, unknown>;
  expect(() => Schema.decodeUnknownSync(schema)(input)).toThrow();
});

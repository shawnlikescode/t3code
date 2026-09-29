/**
 * KiloAdapter — shape type for the Kilo Code provider adapter.
 *
 * The driver model ({@link ../Drivers/KiloDriver}) bundles one adapter per
 * instance as a captured closure, so this module only retains the shape
 * interface as a naming anchor for the driver bundle.
 *
 * @module KiloAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/**
 * KiloAdapterShape — per-instance Kilo adapter contract.
 */
export interface KiloAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}

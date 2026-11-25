/**
 * Runtime config that Nais writes into the pod (spec.frontend.generatedConfig in
 * .nais/app.yaml). It exists only on Nais, so local runs get no telemetry.
 * https://docs.novari.cloud.nais.io/observability/frontend/reference/auto-configuration/
 */
export interface NaisConfig {
  telemetryCollectorURL: string;
  app: { name: string; namespace: string; version: string };
}

export const NAIS_CONFIG_URL = '/js/nais.js';

function isNaisConfig(value: unknown): value is NaisConfig {
  const config = value as NaisConfig | undefined;
  return typeof config?.telemetryCollectorURL === 'string'
    && typeof config.app?.name === 'string'
    && typeof config.app?.namespace === 'string'
    && typeof config.app?.version === 'string';
}

/** The Nais config, or null when the file is missing or malformed. */
export async function loadNaisConfig(
  load: () => Promise<{ default?: unknown }> = () => import(/* @vite-ignore */ NAIS_CONFIG_URL),
): Promise<NaisConfig | null> {
  try {
    const { default: config } = await load();
    return isNaisConfig(config) ? config : null;
  } catch {
    return null;
  }
}

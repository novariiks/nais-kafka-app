/**
 * Runtime config that Nais writes into the pod (spec.frontend.generatedConfig in
 * .nais/app.yaml). It exists only on Nais, so local runs get no telemetry.
 * https://docs.novari.cloud.nais.io/observability/frontend/reference/auto-configuration/
 */
export interface NaisConfig {
  telemetryCollectorURL: string;
  // Nais does not always set namespace and version (seen with version: '').
  app: { name: string; namespace?: string; version?: string };
}

export const NAIS_CONFIG_URL = '/js/nais.js';

const optionalString = (value: unknown) => value === undefined || typeof value === 'string';

function isNaisConfig(value: unknown): value is NaisConfig {
  const config = value as NaisConfig | undefined;
  return typeof config?.telemetryCollectorURL === 'string'
    && config.telemetryCollectorURL !== ''
    && typeof config.app?.name === 'string'
    && optionalString(config.app.namespace)
    && optionalString(config.app.version);
}

/** Drops empty fields, so Faro does not report an empty version. */
function withoutEmpty({ telemetryCollectorURL, app }: NaisConfig): NaisConfig {
  return {
    telemetryCollectorURL,
    app: Object.fromEntries(Object.entries(app).filter(([, v]) => v !== '')) as NaisConfig['app'],
  };
}

/** The Nais config, or null when the file is missing or malformed. */
export async function loadNaisConfig(
  load: () => Promise<{ default?: unknown }> = () => import(/* @vite-ignore */ NAIS_CONFIG_URL),
): Promise<NaisConfig | null> {
  try {
    const { default: config } = await load();
    return isNaisConfig(config) ? withoutEmpty(config) : null;
  } catch {
    return null;
  }
}

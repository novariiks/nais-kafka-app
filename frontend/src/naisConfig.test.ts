import { describe, expect, it } from 'vitest';
import { loadNaisConfig } from './naisConfig';

const config = {
  telemetryCollectorURL: 'https://telemetry.example/collect',
  app: { name: 'nais-kafka-frontend', namespace: 'flais', version: '2026.11.25-abc1234' },
};

describe('Nais config', () => {
  it('returns the generated config', async () => {
    expect(await loadNaisConfig(async () => ({ default: config }))).toEqual(config);
  });
  it('returns null when the file is missing, as in local runs', async () => {
    expect(await loadNaisConfig(() => Promise.reject(new TypeError('Failed to fetch')))).toBeNull();
  });
  it('returns null for an unexpected shape', async () => {
    expect(await loadNaisConfig(async () => ({ default: { app: config.app } }))).toBeNull();
    expect(await loadNaisConfig(async () => ({}))).toBeNull();
  });
});

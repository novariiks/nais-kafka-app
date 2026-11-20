import { describe, expect, it } from 'vitest';
import { latencyMs, mergeEvents, traceUrl, type ConsumedEvent } from './events';

const consumed = (id: string, producedAt = '2026-01-01T00:00:00Z', sequence = 1): ConsumedEvent => ({
  event: { id, runId: 'run', sequence, deviceId: 'device-1', value: 1, producedAt, producer: 'job' },
  partition: 0, offset: 0, consumer: 'replica-a', consumedAt: producedAt, traceId: null,
});

describe('event reconciliation', () => {
  it('deduplicates repeated events and overlapping history', () => {
    expect(mergeEvents([consumed('a')], [consumed('a'), consumed('b')])).toHaveLength(2);
  });
  it('puts the newest events first', () => {
    expect(mergeEvents([consumed('earlier')], [consumed('later', '2026-01-02T00:00:00Z')]).map(e => e.event.id))
      .toEqual(['later', 'earlier']);
  });
  it('orders events from the same instant by sequence', () => {
    expect(mergeEvents([], [consumed('first', undefined, 1), consumed('second', undefined, 2)]).map(e => e.event.id))
      .toEqual(['second', 'first']);
  });
  it('keeps the latest 100 events', () => {
    const events = Array.from({ length: 105 }, (_, i) => consumed(String(i), new Date(Date.UTC(2026, 0, 1, 0, i)).toISOString()));
    const merged = mergeEvents([], events);
    expect(merged).toHaveLength(100);
    expect(merged[0].event.id).toBe('104');
    expect(merged.at(-1)!.event.id).toBe('5');
  });
});

describe('event details', () => {
  it('measures latency from production to consumption', () => {
    expect(latencyMs({ ...consumed('a'), consumedAt: '2026-01-01T00:00:00.250Z' })).toBe(250);
  });
  it('links a trace to Grafana Explore', () => {
    const url = new URL(traceUrl({ url: 'https://grafana.example', tempoDatasource: 'tempo' }, 'abc123'));
    expect(url.origin + url.pathname).toBe('https://grafana.example/explore');
    expect(JSON.parse(url.searchParams.get('panes')!).trace.queries[0].query).toBe('abc123');
  });
});

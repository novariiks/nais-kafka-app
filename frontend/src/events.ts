export interface KafkaEvent {
  id: string;
  runId: string;
  sequence: number;
  deviceId: string;
  value: number;
  producedAt: string;
  producer: string;
}

export interface ConsumedEvent {
  event: KafkaEvent;
  partition: number;
  offset: number;
  consumer: string;
  consumedAt: string;
  traceId: string | null;
}

export interface Info {
  instance: string;
  topic: string;
  groupId: string;
  consumedPerPartition: Record<string, number>;
  grafana: { url: string; tempoDatasource: string };
}

export const HISTORY_LIMIT = 100;

// Live events overlap with history refreshes and reconnects. Newest first.
export function mergeEvents(current: ConsumedEvent[], incoming: ConsumedEvent[]): ConsumedEvent[] {
  const events = new Map(current.map(e => [e.event.id, e]));
  incoming.forEach(e => events.set(e.event.id, e));
  return [...events.values()]
    .sort((a, b) => Date.parse(b.event.producedAt) - Date.parse(a.event.producedAt)
      || b.event.sequence - a.event.sequence
      || a.event.id.localeCompare(b.event.id))
    .slice(0, HISTORY_LIMIT);
}

/** Time from the producer creating the event until this consumer handled it. */
export function latencyMs(e: ConsumedEvent): number {
  return Math.max(0, Date.parse(e.consumedAt) - Date.parse(e.event.producedAt));
}

/** Grafana Explore showing one trace from Tempo. */
export function traceUrl(grafana: Info['grafana'], traceId: string): string {
  const panes = {
    trace: {
      datasource: grafana.tempoDatasource,
      queries: [{ refId: 'A', datasource: { type: 'tempo', uid: grafana.tempoDatasource }, queryType: 'traceql', query: traceId }],
      range: { from: 'now-24h', to: 'now' },
    },
  };
  return `${grafana.url}/explore?schemaVersion=1&panes=${encodeURIComponent(JSON.stringify(panes))}`;
}

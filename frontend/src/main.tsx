import { StrictMode, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { latencyMs, mergeEvents, traceUrl, type ConsumedEvent, type Info } from './events';
import './style.css';

const time = (iso: string) => new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });

function App() {
  const [events, setEvents] = useState<ConsumedEvent[]>([]);
  const [info, setInfo] = useState<Info | null>(null);
  const [connected, setConnected] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    const abort = new AbortController();
    let refreshing = false;
    async function refresh() {
      if (refreshing) return;
      refreshing = true;
      try {
        const [history, latest] = await Promise.all(['/api/events', '/api/info'].map(async url => {
          const response = await fetch(url, { signal: abort.signal });
          if (!response.ok) throw new Error('Unable to reach the consumer. Retrying automatically.');
          return response.json();
        }));
        setEvents(current => mergeEvents(current, history));
        setInfo(latest);
        setError('');
      } catch (e) {
        if (!abort.signal.aborted) setError(e instanceof Error ? e.message : 'Cannot reach the consumer.');
      } finally {
        refreshing = false;
        if (!abort.signal.aborted) setLoading(false);
      }
    }
    const stream = new EventSource('/api/stream');
    stream.addEventListener('connected', () => { setConnected(true); void refresh(); });
    stream.addEventListener('event', message => {
      try {
        const event: ConsumedEvent = JSON.parse((message as MessageEvent<string>).data);
        setEvents(current => mergeEvents(current, [event]));
        setInfo(current => current && {
          ...current,
          consumedPerPartition: { ...current.consumedPerPartition, [event.partition]: (current.consumedPerPartition[event.partition] ?? 0) + 1 },
        });
      } catch { void refresh(); }
    });
    stream.onerror = () => setConnected(false);
    void refresh();
    // Reconcile missed events after reconnects or deploys.
    const timer = window.setInterval(() => { void refresh(); }, 10000);
    return () => { abort.abort(); stream.close(); window.clearInterval(timer); };
  }, []);

  const partitions = Object.entries(info?.consumedPerPartition ?? {}).sort(([a], [b]) => Number(a) - Number(b));
  const lastRun = events[0]?.event.runId;
  const lastRunEvents = events.filter(e => e.event.runId === lastRun);

  return <div className="app-shell">
    <aside className="sidebar">
      <a className="brand" href="/" aria-label="NAIS Kafka home"><span className="brand-mark">n.</span> NAIS <span className="brand-light">kafka</span></a>
      <div className="workspace-label">PLATFORM PLAYGROUND</div>
      <div className="channel active"><span>#</span> {info?.topic ?? 'events'} <span className="channel-badge">LIVE</span></div>
      <p className="sidebar-description">A Naisjob writes.<br />Kafka keeps.<br />Kotlin reads.</p>
      <div className="stack">
        <div className="workspace-label">UNDER THE HOOD</div>
        <div><span className="stack-symbol">Nj</span><span>Naisjob <small>Publishes every two minutes</small></span></div>
        <div><span className="stack-symbol">K</span><span>Kafka <small>Keyed by device, 3 partitions</small></span></div>
        <div><span className="stack-symbol">Kt</span><span>Kotlin <small>Spring Kafka consumer</small></span></div>
        <div><span className="stack-symbol">OT</span><span>OpenTelemetry <small>One trace, job to browser</small></span></div>
      </div>
      <div className="sidebar-footer">Built to explore NAIS.<br /><span>Read-only. Just watch.</span></div>
    </aside>
    <main>
      <header className="topbar">
        <div><span className="hash">#</span><strong>{info?.topic ?? 'events'}</strong><span className="divider" /><span className="topic">Consumer group {info?.groupId ?? '…'}</span></div>
        <span className={`connection ${connected ? 'online' : ''}`} role="status"><i />{connected ? 'Live updates' : 'Reconnecting'}</span>
      </header>
      <section className="conversation" aria-label="Consumed events">
        <div className="welcome">
          <span className="eyebrow">EVENTS IN MOTION</span>
          <h1>Hello, Kafka.</h1>
          <p>A Naisjob publishes a batch of sensor readings every two minutes. Watch them arrive, then follow any trace into Grafana.</p>
          <div className="journey"><span>Naisjob</span><b>→</b><span>Kafka</span><b>→</b><span>Kotlin</span><b>→</b><span>You</span></div>
        </div>
        <div className="stats">
          {partitions.map(([partition, count]) => <div className="stat" key={partition}>
            <span className="stat-label">Partition {partition}</span><strong>{count.toLocaleString()}</strong><small>consumed by this replica</small>
          </div>)}
          {lastRun && <div className="stat">
            <span className="stat-label">Latest run</span><strong className="mono">{lastRun.slice(0, 8)}</strong>
            <small>{lastRunEvents.length} events · {time(lastRunEvents[0].event.producedAt)}</small>
          </div>}
        </div>
        <div className="conversation-divider"><span>Newest first</span></div>
        <div className="messages" role="log" aria-live="polite" aria-label="Events">
          {loading && <p className="empty">Loading events…</p>}
          {!loading && events.length === 0 && <div className="empty"><span className="empty-icon">#</span><h2>Waiting for the next run.</h2><p>The Naisjob publishes every two minutes.</p></div>}
          {events.map(e => <article className="message" key={e.event.id}>
            <div className="avatar" aria-hidden="true">{e.event.deviceId.replace('device-', 'D')}</div>
            <div className="message-content">
              <div className="message-meta"><strong>{e.event.deviceId}</strong><time dateTime={e.event.producedAt} title={new Date(e.event.producedAt).toLocaleString()}>{time(e.event.producedAt)}</time></div>
              <p><span className="reading">{e.event.value.toFixed(1)}</span> <span className="unit">units</span></p>
              <div className="tags">
                <span title="Kafka partition and offset">p{e.partition} · offset {e.offset}</span>
                <span title={`Run ${e.event.runId} from ${e.event.producer}`}>run {e.event.runId.slice(0, 8)} · #{e.event.sequence}</span>
                <span title="Time from producing to consuming">{latencyMs(e).toLocaleString()} ms</span>
                <span title="Consumer replica that read this event">via {e.consumer}</span>
                {e.traceId && info && <a href={traceUrl(info.grafana, e.traceId)} target="_blank" rel="noreferrer" title="Open this trace in Grafana">trace {e.traceId.slice(0, 8)} ↗</a>}
              </div>
            </div>
          </article>)}
        </div>
      </section>
      <footer className="composer-area">
        {error && <p className="error" role="alert">{error}</p>}
        <div className="bottom-note"><span>Latest 100 events · Live updates over server-sent events</span><span className="backend-instance" title="Consumer replica serving this browser">{info?.instance ?? 'Connecting…'}</span></div>
      </footer>
    </main>
  </div>;
}

createRoot(document.getElementById('root')!).render(<StrictMode><App /></StrictMode>);

import { getWebInstrumentations, initializeFaro } from '@grafana/faro-web-sdk';
import { TracingInstrumentation } from '@grafana/faro-web-tracing';
import { setErrorReporter } from './errors';
import type { NaisConfig } from './naisConfig';

// Loaded with import() only when the Nais config exists, so Faro and its tracing
// (about 500 kB) stay out of the main bundle and out of local runs.
export function startTelemetry(nais: NaisConfig): void {
  const faro = initializeFaro({
    url: nais.telemetryCollectorURL,
    // name (and namespace, when Nais sets it) match .nais/app.yaml, so APM shows this
    // next to the backends.
    app: nais.app,
    instrumentations: [
      ...getWebInstrumentations(),
      // Adds a traceparent header to same-origin fetches, so /api calls continue in
      // nginx's access log and the consumer's spans. EventSource cannot set headers,
      // so the live stream is not traced.
      new TracingInstrumentation(),
    ],
    // No beforeSend filter: the page shows no personal data and URLs carry none.
  });
  setErrorReporter(error => faro.api.pushError(error));
}

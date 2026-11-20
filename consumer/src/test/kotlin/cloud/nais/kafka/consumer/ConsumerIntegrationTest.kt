package cloud.nais.kafka.consumer

import cloud.nais.kafka.events.EVENTS_TOPIC
import cloud.nais.kafka.events.KafkaEvent
import io.opentelemetry.api.GlobalOpenTelemetry
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import tools.jackson.databind.json.JsonMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = [EVENTS_TOPIC], partitions = 3, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class ConsumerIntegrationTest(
    @Autowired private val kafka: KafkaTemplate<String, String>,
    @Autowired private val json: JsonMapper,
    @Autowired private val recent: RecentEvents,
    @LocalServerPort private val port: Int,
) {
    @Test
    fun `consumes events and continues the producer's trace`() {
        val event = event()
        // Stands in for the Naisjob's root span. Without the agent this is a no-op span.
        val span = GlobalOpenTelemetry.getTracer("test").spanBuilder("producer run").startSpan()
        span.makeCurrent().use { send(event) }
        span.end()

        val consumed = await().atMost(Duration.ofSeconds(30))
            .until({ recent.history().find { it.event.id == event.id } }, { it != null })!!

        assertEquals(event, consumed.event)
        if (span.spanContext.isValid) {
            assertEquals(span.spanContext.traceId, consumed.traceId)
        } else {
            assertNull(consumed.traceId)
        }
    }

    @Test
    fun `streams consumed events to browsers`() {
        val lines = ConcurrentLinkedQueue<String>()
        val request = HttpRequest.newBuilder(URI("http://localhost:$port/api/stream")).build()
        val stream = HttpClient.newHttpClient().sendAsync(request, HttpResponse.BodyHandlers.ofLines())
            .thenAccept { response -> response.body().forEach(lines::add) }
        try {
            await().atMost(Duration.ofSeconds(10)).untilAsserted {
                assertTrue("ready" in lines.mapNotNull(::sseData), "received: $lines")
            }

            val event = event()
            send(event)

            val data = await().atMost(Duration.ofSeconds(30))
                .until({ lines.mapNotNull(::sseData).find { event.id.toString() in it } }, { it != null })!!
            assertEquals(event, json.readValue(data, ConsumedEvent::class.java).event)
        } finally {
            stream.cancel(true)
        }
    }

    /** The value of an SSE `data` field. One space after the colon is optional. */
    private fun sseData(line: String): String? =
        if (line.startsWith("data:")) line.removePrefix("data:").removePrefix(" ") else null

    private fun send(event: KafkaEvent) {
        kafka.send(EVENTS_TOPIC, event.deviceId, json.writeValueAsString(event)).get()
    }

    private fun event() = KafkaEvent(
        id = UUID.randomUUID(),
        runId = UUID.randomUUID(),
        sequence = 1,
        deviceId = "device-1",
        value = 42.0,
        producedAt = Instant.parse("2026-11-20T12:00:00Z"),
        producer = "test",
    )
}

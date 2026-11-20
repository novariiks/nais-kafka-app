package cloud.nais.kafka.consumer

import cloud.nais.kafka.events.EVENTS_TOPIC
import cloud.nais.kafka.events.KafkaEvent
import io.opentelemetry.api.GlobalOpenTelemetry
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.time.Instant
import java.util.UUID

@SpringBootTest
@EmbeddedKafka(topics = [EVENTS_TOPIC], partitions = 3, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class ConsumerIntegrationTest(
    @Autowired private val kafka: KafkaTemplate<String, String>,
    @Autowired private val json: JsonMapper,
    @Autowired private val recent: RecentEvents,
) {
    @Test
    fun `consumes events and continues the producer's trace`() {
        val event = KafkaEvent(
            id = UUID.randomUUID(),
            runId = UUID.randomUUID(),
            sequence = 1,
            deviceId = "device-1",
            value = 42.0,
            producedAt = Instant.parse("2026-11-20T12:00:00Z"),
            producer = "test",
        )
        // Stands in for the Naisjob's root span. Without the agent this is a no-op span.
        val span = GlobalOpenTelemetry.getTracer("test").spanBuilder("producer run").startSpan()
        span.makeCurrent().use {
            kafka.send(EVENTS_TOPIC, event.deviceId, json.writeValueAsString(event)).get()
        }
        span.end()

        val consumed = await().atMost(Duration.ofSeconds(30))
            .until({ recent.history().find { it.event.id == event.id } }, { it != null })!!

        assertEquals(event, consumed.event)
        assertEquals(1L, recent.countsPerPartition()[consumed.partition])
        if (span.spanContext.isValid) {
            assertEquals(span.spanContext.traceId, consumed.traceId)
        } else {
            assertNull(consumed.traceId)
        }
    }
}

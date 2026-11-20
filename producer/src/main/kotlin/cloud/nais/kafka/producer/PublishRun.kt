package cloud.nais.kafka.producer

import cloud.nais.kafka.events.EVENTS_TOPIC
import cloud.nais.kafka.events.KafkaEvent
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.StatusCode
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import tools.jackson.databind.json.JsonMapper
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.random.Random

@Component
class PublishRun(
    private val kafka: KafkaTemplate<String, String>,
    private val json: JsonMapper,
    private val properties: ProducerProperties,
) : ApplicationRunner {
    private val log = LoggerFactory.getLogger(javaClass)
    private val tracer = GlobalOpenTelemetry.getTracer("nais-kafka-producer")

    // A cron job has no incoming request, so this span is the root of each run's trace.
    // The agent's Kafka instrumentation makes every send a child span and writes
    // the trace context into the record headers.
    override fun run(args: ApplicationArguments) {
        val runId = UUID.randomUUID()
        val span = tracer.spanBuilder("producer run")
            .setAttribute("producer.run_id", runId.toString())
            .setAttribute("producer.events", properties.eventsPerRun.toLong())
            .startSpan()
        try {
            span.makeCurrent().use { publish(runId) }
        } catch (e: Exception) {
            span.recordException(e)
            span.setStatus(StatusCode.ERROR)
            throw e
        } finally {
            span.end()
        }
    }

    private fun publish(runId: UUID) {
        val sends = (1..properties.eventsPerRun).map { sequence ->
            val event = KafkaEvent(
                id = UUID.randomUUID(),
                runId = runId,
                sequence = sequence,
                deviceId = "device-${Random.nextInt(1, properties.devices + 1)}",
                value = Random.nextDouble(0.0, 100.0),
                producedAt = Instant.now(),
                producer = properties.instance,
            )
            kafka.send(EVENTS_TOPIC, event.deviceId, json.writeValueAsString(event))
        }
        // Fail the run (and the Naisjob) unless the broker acknowledged every record.
        val results = sends.map { it.get(30, TimeUnit.SECONDS) }
        val partitions = results.groupingBy { it.recordMetadata.partition() }.eachCount().toSortedMap()
        log.info("Published {} events for run {} to {} (records per partition: {})", results.size, runId, EVENTS_TOPIC, partitions)
    }
}

package cloud.nais.kafka.consumer

import cloud.nais.kafka.events.EVENTS_TOPIC
import cloud.nais.kafka.events.KafkaEvent
import io.opentelemetry.api.trace.Span
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import tools.jackson.databind.json.JsonMapper
import java.time.Instant

@Component
class EventListener(
    private val json: JsonMapper,
    private val recent: RecentEvents,
    @param:Value("\${consumer.instance}") private val instance: String,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    // The agent continues the producer's trace from the record headers,
    // so the current span here belongs to the trace started by the Naisjob.
    @KafkaListener(topics = [EVENTS_TOPIC])
    fun consume(record: ConsumerRecord<String, String>) {
        val event = json.readValue(record.value(), KafkaEvent::class.java)
        val traceId = Span.current().spanContext.takeIf { it.isValid }?.traceId
        recent.add(
            ConsumedEvent(
                event = event,
                partition = record.partition(),
                offset = record.offset(),
                consumer = instance,
                consumedAt = Instant.now(),
                traceId = traceId,
            ),
        )
        log.debug("Consumed {} from partition {} offset {} (trace {})", event.id, record.partition(), record.offset(), traceId)
    }
}

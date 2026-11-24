package cloud.nais.kafka.consumer

import cloud.nais.kafka.events.EVENTS_TOPIC
import cloud.nais.kafka.events.KafkaEvent
import io.opentelemetry.api.trace.Span
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.AbstractConsumerSeekAware
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback
import org.springframework.stereotype.Component
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.time.Instant

@Component
class EventListener(
    private val json: JsonMapper,
    private val recent: RecentEvents,
    private val live: LiveEvents,
    @param:Value("\${consumer.instance}") private val instance: String,
) : AbstractConsumerSeekAware() {
    private val log = LoggerFactory.getLogger(javaClass)

    // State is in memory, so a restarted replica would show nothing until the next job run.
    // Replaying the newest records of each assigned partition refills the window right away.
    // RecentEvents drops records it already holds.
    override fun onPartitionsAssigned(assignments: Map<TopicPartition, Long>, callback: ConsumerSeekCallback) {
        super.onPartitionsAssigned(assignments, callback)
        if (assignments.isEmpty()) return
        val perPartition = (RecentEvents.CAPACITY + assignments.size - 1) / assignments.size
        assignments.keys.forEach { callback.seekRelative(it.topic(), it.partition(), -perPartition.toLong(), false) }
        log.atInfo()
            .addKeyValue("partitions", assignments.keys.map { it.partition() }.sorted())
            .addKeyValue("replay_per_partition", perPartition)
            .log("Assigned {}; replaying up to {} records per partition", assignments.keys, perPartition)
    }

    // The agent continues the producer's trace from the record headers,
    // so the current span here belongs to the trace started by the Naisjob.
    @KafkaListener(topics = [EVENTS_TOPIC])
    fun consume(record: ConsumerRecord<String, String>) {
        val event = json.readValue(record.value(), KafkaEvent::class.java)
        val traceId = Span.current().spanContext.takeIf { it.isValid }?.traceId
        val consumed = ConsumedEvent(
            event = event,
            partition = record.partition(),
            offset = record.offset(),
            consumer = instance,
            consumedAt = Instant.now(),
            traceId = traceId,
        )
        val isNew = recent.add(consumed)
        if (isNew) live.publish(consumed)
        // One line per event: about 10 every two minutes. The agent adds trace_id and span_id
        // of this process span to the MDC, so each line links to its trace.
        log.atInfo()
            .addKeyValue("event_id", event.id)
            .addKeyValue("run_id", event.runId)
            .addKeyValue("device_id", event.deviceId)
            .addKeyValue("partition", record.partition())
            .addKeyValue("offset", record.offset())
            .addKeyValue("latency_ms", Duration.between(event.producedAt, consumed.consumedAt).toMillis())
            // false when replaying records after a partition assignment
            .addKeyValue("new", isNew)
            .log("Consumed {} from partition {} offset {}", event.deviceId, record.partition(), record.offset())
    }
}

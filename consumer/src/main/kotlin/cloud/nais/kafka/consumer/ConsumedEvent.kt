package cloud.nais.kafka.consumer

import cloud.nais.kafka.events.KafkaEvent
import java.time.Instant

/** A [KafkaEvent] plus where and how this replica received it. */
data class ConsumedEvent(
    val event: KafkaEvent,
    val partition: Int,
    val offset: Long,
    val consumer: String,
    val consumedAt: Instant,
    /** Trace of the consuming span, or null when tracing is off. */
    val traceId: String?,
)

package cloud.nais.kafka.events

import java.time.Instant
import java.util.UUID

/** Nais prefixes topic names with the owning team. Local Kafka uses the same name. */
const val EVENTS_TOPIC = "flais.nais-kafka-events"

/** One simulated sensor reading. The producer keys records by [deviceId]. */
data class KafkaEvent(
    val id: UUID,
    val runId: UUID,
    val sequence: Int,
    val deviceId: String,
    val value: Double,
    val producedAt: Instant,
    val producer: String,
)

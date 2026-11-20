package cloud.nais.kafka.consumer

import cloud.nais.kafka.events.EVENTS_TOPIC
import org.springframework.beans.factory.annotation.Value
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api")
class EventController(
    private val recent: RecentEvents,
    @param:Value("\${consumer.instance}") private val instance: String,
    @param:Value("\${spring.kafka.consumer.group-id}") private val groupId: String,
) {
    @GetMapping("/events")
    fun history(): List<ConsumedEvent> = recent.history()

    @GetMapping("/info")
    fun info(): Map<String, Any> = mapOf(
        "application" to "nais-kafka-consumer",
        "instance" to instance,
        "topic" to EVENTS_TOPIC,
        "groupId" to groupId,
        "consumedPerPartition" to recent.countsPerPartition(),
    )
}

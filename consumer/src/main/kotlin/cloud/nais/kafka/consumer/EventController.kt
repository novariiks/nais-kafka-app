package cloud.nais.kafka.consumer

import cloud.nais.kafka.events.EVENTS_TOPIC
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter

@RestController
@RequestMapping("/api")
class EventController(
    private val recent: RecentEvents,
    private val live: LiveEvents,
    @param:Value("\${consumer.instance}") private val instance: String,
    @param:Value("\${spring.kafka.consumer.group-id}") private val groupId: String,
) {
    @GetMapping("/events")
    fun history(): List<ConsumedEvent> = recent.history()

    @GetMapping("/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun stream(): SseEmitter = live.subscribe()

    @GetMapping("/info")
    fun info(): Map<String, Any> = mapOf(
        "application" to "nais-kafka-consumer",
        "instance" to instance,
        "topic" to EVENTS_TOPIC,
        "groupId" to groupId,
        "consumedPerPartition" to recent.countsPerPartition(),
    )
}

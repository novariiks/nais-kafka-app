package cloud.nais.kafka.consumer

import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import tools.jackson.databind.json.JsonMapper
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

/** Pushes consumed events to connected browsers over server-sent events. */
@Component
class LiveEvents(private val json: JsonMapper) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val clients = ConcurrentHashMap.newKeySet<SseEmitter>()

    fun subscribe(): SseEmitter {
        val emitter = SseEmitter(0L)
        clients.add(emitter)
        log.atInfo().addKeyValue("open_streams", clients.size).log("Event stream opened ({} open)", clients.size)
        emitter.onCompletion { remove(emitter, "completed") }
        emitter.onTimeout { remove(emitter, "timed out") }
        emitter.onError { remove(emitter, "failed") }
        try {
            emitter.send(SseEmitter.event().name("connected").data("ready"))
        } catch (e: Exception) {
            remove(emitter, "failed")
            throw e
        }
        return emitter
    }

    fun publish(event: ConsumedEvent) = broadcast("event", json.writeValueAsString(event))

    @Scheduled(fixedRate = 15000)
    fun heartbeat() = broadcast("heartbeat", "ping")

    private fun broadcast(name: String, payload: String) {
        for (client in clients) {
            try {
                client.send(SseEmitter.event().name(name).data(payload))
            } catch (_: IOException) {
                remove(client, "closed by client")
                client.complete()
            } catch (_: IllegalStateException) {
                remove(client, "closed by client")
            }
        }
    }

    // Several callbacks can fire for the same stream; only the first removal is logged.
    private fun remove(emitter: SseEmitter, reason: String) {
        if (!clients.remove(emitter)) return
        log.atInfo()
            .addKeyValue("open_streams", clients.size)
            .addKeyValue("reason", reason)
            .log("Event stream {} ({} open)", reason, clients.size)
    }
}

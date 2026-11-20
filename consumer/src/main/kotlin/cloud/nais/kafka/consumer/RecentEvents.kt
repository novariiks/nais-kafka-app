package cloud.nais.kafka.consumer

import org.springframework.stereotype.Component
import java.util.UUID

/** The latest events this replica consumed, oldest first. State is in memory and per replica. */
@Component
class RecentEvents {
    private val events = ArrayDeque<ConsumedEvent>()
    private val ids = HashSet<UUID>()
    private val perPartition = sortedMapOf<Int, Long>()

    /**
     * Adds [event] unless it is already in the window, which happens when the
     * listener replays recent records after a partition assignment.
     * Returns true if the event was new.
     */
    @Synchronized
    fun add(event: ConsumedEvent): Boolean {
        if (!ids.add(event.event.id)) return false
        events.addLast(event)
        if (events.size > CAPACITY) ids.remove(events.removeFirst().event.id)
        perPartition.merge(event.partition, 1, Long::plus)
        return true
    }

    @Synchronized
    fun history(): List<ConsumedEvent> = events.toList()

    /** New events consumed per partition since this replica started. */
    @Synchronized
    fun countsPerPartition(): Map<Int, Long> = perPartition.toMap()

    companion object {
        const val CAPACITY = 100
    }
}

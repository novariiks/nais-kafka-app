package cloud.nais.kafka.consumer

import org.springframework.stereotype.Component

/** The latest events this replica consumed, oldest first. State is in memory and per replica. */
@Component
class RecentEvents {
    private val events = ArrayDeque<ConsumedEvent>()
    private val perPartition = sortedMapOf<Int, Long>()

    @Synchronized
    fun add(event: ConsumedEvent) {
        events.addLast(event)
        if (events.size > CAPACITY) events.removeFirst()
        perPartition.merge(event.partition, 1, Long::plus)
    }

    @Synchronized
    fun history(): List<ConsumedEvent> = events.toList()

    /** Events consumed per partition since this replica started. */
    @Synchronized
    fun countsPerPartition(): Map<Int, Long> = perPartition.toMap()

    companion object {
        const val CAPACITY = 100
    }
}

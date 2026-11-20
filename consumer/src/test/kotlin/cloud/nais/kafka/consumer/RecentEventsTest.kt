package cloud.nais.kafka.consumer

import cloud.nais.kafka.events.KafkaEvent
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

class RecentEventsTest {
    @Test
    fun `ignores events already in the window`() {
        val recent = RecentEvents()
        val event = consumed(partition = 0, offset = 1)

        assertTrue(recent.add(event))
        assertFalse(recent.add(event.copy(offset = 1, consumedAt = Instant.now())))

        assertEquals(listOf(event), recent.history())
        assertEquals(mapOf(0 to 1L), recent.countsPerPartition())
    }

    @Test
    fun `keeps the newest events up to capacity`() {
        val recent = RecentEvents()
        val events = (0 until RecentEvents.CAPACITY + 5).map { consumed(partition = it % 3, offset = it.toLong()) }
        events.forEach { recent.add(it) }

        assertEquals(events.takeLast(RecentEvents.CAPACITY), recent.history())
        // An evicted event counts as new again if it is replayed.
        assertTrue(recent.add(events.first()))
        assertEquals(RecentEvents.CAPACITY + 6L, recent.countsPerPartition().values.sum())
    }

    private fun consumed(partition: Int, offset: Long) = ConsumedEvent(
        event = KafkaEvent(
            id = UUID.randomUUID(),
            runId = UUID.randomUUID(),
            sequence = 1,
            deviceId = "device-1",
            value = 1.0,
            producedAt = Instant.EPOCH,
            producer = "test",
        ),
        partition = partition,
        offset = offset,
        consumer = "test",
        consumedAt = Instant.EPOCH,
        traceId = null,
    )
}

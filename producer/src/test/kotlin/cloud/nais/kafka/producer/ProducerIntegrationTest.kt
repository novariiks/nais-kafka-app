package cloud.nais.kafka.producer

import cloud.nais.kafka.events.EVENTS_TOPIC
import cloud.nais.kafka.events.KafkaEvent
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils
import tools.jackson.databind.json.JsonMapper
import java.time.Duration

// The ApplicationRunner publishes one run while the context starts.
@SpringBootTest(properties = ["producer.events-per-run=7"])
@EmbeddedKafka(topics = [EVENTS_TOPIC], partitions = 3, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class ProducerIntegrationTest(
    @Autowired private val broker: EmbeddedKafkaBroker,
    @Autowired private val json: JsonMapper,
) {
    @Test
    fun `publishes one run keyed by device`() {
        val props = KafkaTestUtils.consumerProps(broker, "producer-test", false) +
            (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest")
        DefaultKafkaConsumerFactory(props, StringDeserializer(), StringDeserializer()).createConsumer().use { consumer ->
            broker.consumeFromAnEmbeddedTopic(consumer, EVENTS_TOPIC)
            val records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10), 7).toList()
            val events = records.map { json.readValue(it.value(), KafkaEvent::class.java) }

            assertEquals(7, events.size)
            assertEquals(1, events.map { it.runId }.toSet().size)
            assertEquals((1..7).toList(), events.map { it.sequence }.sorted())
            records.zip(events).forEach { (record, event) -> assertEquals(event.deviceId, record.key()) }
        }
    }
}

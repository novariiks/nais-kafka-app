package cloud.nais.kafka.producer

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("producer")
data class ProducerProperties(
    /** Events to publish per job run. */
    val eventsPerRun: Int = 10,
    /** Number of simulated devices. Each device is a record key, so it stays on one partition. */
    val devices: Int = 6,
    /** Name shown in the UI as the event's producer. */
    val instance: String = "local",
)

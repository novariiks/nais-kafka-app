package cloud.nais.kafka.producer

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import kotlin.system.exitProcess

@SpringBootApplication
@ConfigurationPropertiesScan
class ProducerApplication

/**
 * Runs one batch and exits. Closing the context flushes the Kafka producer;
 * the JVM shutdown hook then lets the OpenTelemetry agent export pending spans.
 */
fun main(args: Array<String>) {
    exitProcess(SpringApplication.exit(runApplication<ProducerApplication>(*args)))
}

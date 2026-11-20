plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "nais-kafka-app"
include("events", "producer", "consumer")

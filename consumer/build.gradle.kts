plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

kotlin { jvmToolchain(21) }
repositories { mavenCentral() }
dependencies {
    implementation(project(":events"))
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("tools.jackson.module:jackson-module-kotlin")
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    // API only. The Nais OpenTelemetry agent supplies the implementation at runtime.
    implementation("io.opentelemetry:opentelemetry-api")
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("org.awaitility:awaitility")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
tasks.test {
    useJUnitPlatform()
    // ./gradlew :consumer:test -PotelAgent=/path/to/opentelemetry-javaagent.jar
    // runs the tests with the OpenTelemetry agent and prints spans to the log.
    providers.gradleProperty("otelAgent").orNull?.let { agent ->
        // Relative paths are resolved from the repository root, not the test working directory.
        jvmArgs("-javaagent:${rootProject.file(agent).absolutePath}")
        environment("OTEL_SERVICE_NAME", "nais-kafka-consumer-test")
        environment("OTEL_TRACES_EXPORTER", "logging")
        environment("OTEL_METRICS_EXPORTER", "none")
        environment("OTEL_LOGS_EXPORTER", "none")
        testLogging { showStandardStreams = true }
    }
}
tasks.bootJar { archiveFileName = "app.jar" }

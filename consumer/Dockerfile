FROM gradle:9.9.2-jdk21 AS build
WORKDIR /workspace
COPY settings.gradle.kts build.gradle.kts ./
COPY gradle/libs.versions.toml gradle/libs.versions.toml
# settings.gradle.kts includes every module, so each needs its build file.
COPY producer/build.gradle.kts producer/build.gradle.kts
COPY events/build.gradle.kts events/build.gradle.kts
COPY events/src events/src
COPY consumer/build.gradle.kts consumer/build.gradle.kts
COPY consumer/src consumer/src
RUN gradle --no-daemon :consumer:bootJar

FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app
RUN addgroup -S app && adduser -S app -G app
COPY --from=build --chown=app:app /workspace/consumer/build/libs/app.jar /app/app.jar
USER app
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]

# compose.yaml only: Nais injects the OpenTelemetry agent itself.
FROM runtime AS local
ADD --chown=app:app --checksum=sha256:e43a270df53c6f423e0ab157be9c27ccc01d5dc3232e162fe54777573db2f149 \
    https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.33.1/opentelemetry-javaagent.jar \
    /app/opentelemetry-javaagent.jar
ENV JAVA_TOOL_OPTIONS=-javaagent:/app/opentelemetry-javaagent.jar

# Default target, used by the Nais deploy workflow.
FROM runtime

# syntax=docker/dockerfile:1
# Shared multi-stage build for every service. Usage: docker build --build-arg MODULE=relay-api .
FROM eclipse-temurin:26.0.2_10-jdk-noble AS build
ARG MODULE
WORKDIR /src
COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY relay-common/pom.xml relay-common/
COPY relay-api/pom.xml relay-api/
COPY relay-dispatcher/pom.xml relay-dispatcher/
COPY mock-receiver/pom.xml mock-receiver/
COPY relay-e2e/pom.xml relay-e2e/
COPY relay-common/src relay-common/src
COPY relay-api/src relay-api/src
COPY relay-dispatcher/src relay-dispatcher/src
COPY mock-receiver/src mock-receiver/src
RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw -q -B -pl ${MODULE} -am package -DskipTests -Djacoco.skip=true \
 && cp ${MODULE}/target/${MODULE}-*-exec.jar /app.jar \
 && java -Djarmode=tools -jar /app.jar extract --layers --launcher --destination /layers

FROM eclipse-temurin:27_35-jre-alpine
RUN addgroup -S relay && adduser -S -G relay -u 10001 relay
WORKDIR /app
COPY --from=build --chown=relay:relay /layers/dependencies/ ./
COPY --from=build --chown=relay:relay /layers/spring-boot-loader/ ./
COPY --from=build --chown=relay:relay /layers/snapshot-dependencies/ ./
COPY --from=build --chown=relay:relay /layers/application/ ./
USER 10001
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]

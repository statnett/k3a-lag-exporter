FROM maven:3-eclipse-temurin-17 AS builder
WORKDIR /workspace
COPY pom.xml pom.xml
RUN mvn dependency:resolve
COPY src/ src/
RUN mvn --batch-mode verify

FROM eclipse-temurin:17.0.8_7-jre-alpine
WORKDIR /app
COPY --from=builder /workspace/target/k3a-lag-exporter-jar-with-dependencies.jar ./k3a-lag-exporter.jar

ENTRYPOINT ["java", "-Dconfig.file=k3a-lag-exporter.conf", "-jar", "k3a-lag-exporter.jar"]

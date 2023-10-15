FROM maven:3-eclipse-temurin-17 AS builder
WORKDIR /workspace
COPY pom.xml pom.xml
# Tests are run outside docker-build
RUN mvn dependency:resolve -DincludeScope=runtime
COPY src/main src/main
RUN mvn --batch-mode -Dmaven.test.skip=true package

FROM eclipse-temurin:21_35-jre-alpine
WORKDIR /app
COPY --from=builder /workspace/target/k3a-lag-exporter-jar-with-dependencies.jar ./k3a-lag-exporter.jar

ENTRYPOINT ["java", "-Dconfig.file=k3a-lag-exporter.conf", "-jar", "k3a-lag-exporter.jar"]

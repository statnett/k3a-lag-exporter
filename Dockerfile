FROM maven:3-eclipse-temurin-21@sha256:a63f5b5b823038516ae7ef61637ac1658b9c6ebdedf29bf2fe0eaa78a322fe03 AS builder
WORKDIR /workspace
COPY pom.xml pom.xml
# Tests are run outside docker-build
RUN mvn dependency:resolve -DincludeScope=runtime
COPY src/main src/main
RUN mvn --batch-mode -Dmaven.test.skip=true package

FROM eclipse-temurin:25-jre-alpine@sha256:bf9c91071c4f90afebb31d735f111735975d6fe2b668a82339f8204202203621
WORKDIR /app
COPY --from=builder /workspace/target/k3a-lag-exporter-jar-with-dependencies.jar ./k3a-lag-exporter.jar
RUN apk update \
  && apk upgrade \
  && rm -rf /var/cache/apk/*

ENTRYPOINT ["java", "-Dconfig.file=k3a-lag-exporter.conf", "-jar", "k3a-lag-exporter.jar"]

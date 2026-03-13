FROM maven:3-eclipse-temurin-25@sha256:303967de98a80bb27e9fa71a97b3f0db8a1b30f4e80e5067569d346eb37dbe58 AS builder
WORKDIR /workspace
COPY pom.xml pom.xml
# Tests are run outside docker-build
RUN mvn dependency:resolve -DincludeScope=runtime
COPY src/main src/main
RUN mvn --batch-mode -Dmaven.test.skip=true package

FROM eclipse-temurin:25.0.2_10-jre-alpine@sha256:f10d6259d0798c1e12179b6bf3b63cea0d6843f7b09c9f9c9c422c50e44379ec
WORKDIR /app
COPY --from=builder /workspace/target/k3a-lag-exporter-jar-with-dependencies.jar ./k3a-lag-exporter.jar
RUN apk update \
  && apk upgrade \
  && rm -rf /var/cache/apk/*

ENTRYPOINT ["java", "-Dconfig.file=k3a-lag-exporter.conf", "-jar", "k3a-lag-exporter.jar"]

FROM maven:3-eclipse-temurin-25@sha256:e3d34f15aa3cb5323856d2dc9e1c0db5645dff5c038a36a12e38a82dd4e9f595 AS builder
WORKDIR /workspace
COPY pom.xml pom.xml
# Tests are run outside docker-build
RUN mvn dependency:resolve -DincludeScope=runtime
COPY src/main src/main
RUN mvn --batch-mode -Dmaven.test.skip=true package

FROM eclipse-temurin:25-jre-alpine@sha256:9c65fe190cb22ba92f50b8d29a749d0f1cfb2437e09fe5fbf9ff927c45fc6e99
WORKDIR /app
COPY --from=builder /workspace/target/k3a-lag-exporter-jar-with-dependencies.jar ./k3a-lag-exporter.jar
RUN apk update \
  && apk upgrade \
  && rm -rf /var/cache/apk/*

ENTRYPOINT ["java", "-Dconfig.file=k3a-lag-exporter.conf", "-jar", "k3a-lag-exporter.jar"]

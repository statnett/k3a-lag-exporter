FROM maven:3-eclipse-temurin-25@sha256:1b1fc6d0168ea616afd1c861d6f32ec37c9ec2ffe88a0351b3771dd4ad86b0d8 AS builder
WORKDIR /workspace
COPY pom.xml pom.xml
# Tests are run outside docker-build
RUN mvn dependency:resolve -DincludeScope=runtime
COPY src/main src/main
RUN mvn --batch-mode -Dmaven.test.skip=true package

FROM eclipse-temurin:25.0.3_9-jre-alpine@sha256:28db6fdf60e38945e43d840c0333aeaec66c15943070104f7586fd3c9d1665b0
WORKDIR /app
COPY --from=builder /workspace/target/k3a-lag-exporter-jar-with-dependencies.jar ./k3a-lag-exporter.jar
RUN apk update \
  && apk upgrade \
  && rm -rf /var/cache/apk/*

ENTRYPOINT ["java", "-Dconfig.file=k3a-lag-exporter.conf", "-jar", "k3a-lag-exporter.jar"]

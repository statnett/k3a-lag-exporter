FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY target/k3a-lag-exporter-jar-with-dependencies.jar ./

ENTRYPOINT ["java", "-Dconfig.file=k3a-lag-exporter.conf", "-jar", "k3a-lag-exporter-jar-with-dependencies.jar"]

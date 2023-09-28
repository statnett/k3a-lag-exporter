FROM eclipse-temurin:11-alpine
WORKDIR /app
COPY target/k3a-lag-exporter.jar ./

CMD ["sh", "-c", "exec java -Dconfig.file=k3a-lag-exporter.conf -jar k3a-lag-exporter.jar"]

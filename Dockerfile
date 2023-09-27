FROM eclipse-temurin:11-alpine
ARG APP_VERSION=0.0.1-SNAPSHOT
ENV APP_VERSION=${APP_VERSION}
WORKDIR /app
COPY target/*-full.jar ./

CMD ["sh", "-c", "exec java -Dconfig.file=k3a-lag-exporter.conf -jar k3a-lag-exporter-${APP_VERSION}-full.jar"]

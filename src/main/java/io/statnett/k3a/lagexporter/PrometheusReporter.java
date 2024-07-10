package io.statnett.k3a.lagexporter;

import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.statnett.k3a.lagexporter.model.ClusterData;
import io.statnett.k3a.lagexporter.model.ConsumerGroupData;

import java.io.IOException;
import java.util.List;

public final class PrometheusReporter {

    private static final String PROMETHEUS_NAMESPACE = Conf.getPrometheusMetricNamespace();
    private final Gauge consumerGroupLagGauge = Gauge.builder()
        .name(buildPrometheusFQName(PROMETHEUS_NAMESPACE, "consumergroup", "group_lag"))
        .labelNames("cluster_name", "group", "topic", "partition" /*, "member_host", "consumer_id", "client_id" */)
        .help("Group offset lag of a partition")
        .register();
    private final Gauge consumerGroupOffsetGauge = Gauge.builder()
        .name(buildPrometheusFQName(PROMETHEUS_NAMESPACE, "consumergroup", "group_offset"))
        .labelNames("cluster_name", "group", "topic", "partition" /*, "member_host", "consumer_id", "client_id" */)
        .help("Group offset of a partition")
        .register();
    private final Gauge pollTimeMsGauge = Gauge.builder()
        .name(buildPrometheusFQName(PROMETHEUS_NAMESPACE, "lag_exporter", "poll_time_ms"))
        .labelNames("cluster_name")
        .help("Time (in ms) spent polling all data")
        .register();

    private void startPrometheusWebServer() {
        try {
            HTTPServer.builder()
                .port(Conf.getPrometheusPort())
                .buildAndStart();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void start() {
        startPrometheusWebServer();
    }

    public void publish(final ClusterData clusterData) {
        final String clusterName = clusterData.clusterName();
        publishConsumerGroupLag(clusterName, clusterData);
        publishPollTimeMs(clusterName, clusterData);
    }

    private void publishConsumerGroupLag(final String clusterName, final ClusterData clusterData) {
        for (final List<ConsumerGroupData> consumerGroupDataList : clusterData.topicAndConsumerData().values()) {
            for (final ConsumerGroupData consumerGroupData : consumerGroupDataList) {
                final String topic = consumerGroupData.topicPartition().topic();
                final String partition = String.valueOf(consumerGroupData.topicPartition().partition());
                final String consumerGroupId = consumerGroupData.consumerGroupId();
                final long lag = consumerGroupData.lag();
                if (lag >= 0) {
                    consumerGroupLagGauge
                        .labelValues(clusterName, consumerGroupId, topic, partition)
                        .set(lag);
                }
                final long offset = consumerGroupData.offset();
                if (offset >= 0) {
                    consumerGroupOffsetGauge
                        .labelValues(clusterName, consumerGroupId, topic, partition)
                        .set(offset);
                }
            }
        }
    }

    private void publishPollTimeMs(final String clusterName, final ClusterData clusterData) {
        if (clusterData.pollTimeMs() >= 0L) {
            pollTimeMsGauge
                .labelValues(clusterName)
                .set(clusterData.pollTimeMs());
        }
    }

    private static String buildPrometheusFQName(final String namespace, final String subsystem, final String name) {
        return namespace + "_" + subsystem + "_" + name;
    }

}

package io.statnett.k3a.lagexporter;

import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.statnett.k3a.lagexporter.model.ClusterData;
import io.statnett.k3a.lagexporter.model.ConsumerGroupData;
import io.statnett.k3a.lagexporter.model.TopicPartitionData;

import java.io.IOException;

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
        final String clusterName = clusterData.getClusterName();
        publishConsumerGroupLag(clusterName, clusterData);
        publishPollTimeMs(clusterName, clusterData);
    }

    private void publishConsumerGroupLag(final String clusterName, final ClusterData clusterData) {
        for (final TopicPartitionData topicPartitionData : clusterData.getAllTopicPartitionData()) {
            final String topic = topicPartitionData.getTopicPartition().topic();
            final String partition = String.valueOf(topicPartitionData.getTopicPartition().partition());
            for (final ConsumerGroupData consumerGroupData : topicPartitionData.getConsumerGroupDataMap().values()) {
                final String consumerGroupId = consumerGroupData.getConsumerGroupId();
                final long lag = consumerGroupData.getLag();
                if (lag >= 0) {
                    consumerGroupLagGauge
                        .labelValues(clusterName, consumerGroupId, topic, partition)
                        .set(lag);
                }
                final long offset = consumerGroupData.getOffset();
                if (offset >= 0) {
                    consumerGroupOffsetGauge
                        .labelValues(clusterName, consumerGroupId, topic, partition)
                        .set(offset);
                }
            }
        }
    }

    private void publishPollTimeMs(final String clusterName, final ClusterData clusterData) {
        if (clusterData.getPollTimeMs() >= 0L) {
            pollTimeMsGauge
                .labelValues(clusterName)
                .set(clusterData.getPollTimeMs());
        }
    }

    private static String buildPrometheusFQName(final String namespace, final String subsystem, final String name) {
        return namespace + "_" + subsystem + "_" + name;
    }

}

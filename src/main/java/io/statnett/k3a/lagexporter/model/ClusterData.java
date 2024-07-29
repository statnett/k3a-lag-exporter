package io.statnett.k3a.lagexporter.model;

import java.util.List;
import java.util.Map;

public record ClusterData(
    String clusterName,
    Map<TopicPartitionData, List<ConsumerGroupData>> topicAndConsumerData,
    long pollTimeMs
) {
}

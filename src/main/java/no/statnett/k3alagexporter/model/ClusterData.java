package no.statnett.k3alagexporter.model;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public final class ClusterData {

    private final String clusterName;
    private final Map<TopicPartition, TopicPartitionData> topicPartitionDataMap = new HashMap<>();
    private long pollTimeMs = -1L;

    public ClusterData(final String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public TopicPartitionData findTopicPartitionData(final TopicPartition topicPartition) {
        synchronized (topicPartitionDataMap) {
            return topicPartitionDataMap.computeIfAbsent(topicPartition, TopicPartitionData::new);
        }
    }

    public Collection<TopicPartitionData> getAllTopicPartitionData() {
        synchronized (topicPartitionDataMap) {
            return topicPartitionDataMap.values();
        }
    }

    public long getPollTimeMs() {
        return pollTimeMs;
    }

    public void setPollTimeMs(final long pollTimeMs) {
        this.pollTimeMs = pollTimeMs;
    }

}

package io.statnett.k3a.lagexporter.model;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

public final class TopicPartitionData {

    private final TopicPartition topicPartition;
    private long endOffset = -1;
    private int numReplicas = -1;
    private final Map<String, ConsumerGroupData> consumerGroupDataMap = new HashMap<>();

    public TopicPartitionData(final TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(final long endOffset) {
        this.endOffset = endOffset;
    }

    public int getNumReplicas() {
        return numReplicas;
    }

    public void setNumReplicas(final int numReplicas) {
        this.numReplicas = numReplicas;
    }

    public Map<String, ConsumerGroupData> getConsumerGroupDataMap() {
        return consumerGroupDataMap;
    }

    public ConsumerGroupData findConsumerGroupData(final String consumerGroupId) {
        synchronized (consumerGroupDataMap) {
            return consumerGroupDataMap.computeIfAbsent(consumerGroupId, ConsumerGroupData::new);
        }
    }

    public void calculateLags() {
        synchronized (consumerGroupDataMap) {
            for (final ConsumerGroupData consumerGroupData : consumerGroupDataMap.values()) {
                if (endOffset < 0) {
                    consumerGroupData.setLag(-1);
                } else {
                    consumerGroupData.setLag(Math.max(0, endOffset - consumerGroupData.getOffset()));
                }
            }
        }
    }

}

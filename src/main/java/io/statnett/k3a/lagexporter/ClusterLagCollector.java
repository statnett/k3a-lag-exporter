package io.statnett.k3a.lagexporter;

import io.statnett.k3a.lagexporter.model.ClusterData;
import io.statnett.k3a.lagexporter.model.ConsumerGroupData;
import io.statnett.k3a.lagexporter.model.TopicPartitionData;
import io.statnett.k3a.lagexporter.utils.RegexStringListFilter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public final class ClusterLagCollector {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterLagCollector.class);
    private final String clusterName;
    private final RegexStringListFilter topicFilter;
    private final RegexStringListFilter consumerGroupFilter;
    private final ClusterClient client;

    public ClusterLagCollector(final String clusterName,
                               final Collection<String> topicAllowList, final Collection<String> topicDenyList,
                               final Collection<String> consumerGroupAllowList, final Collection<String> consumerGroupDenyList,
                               final ClusterClient client) {
        this.clusterName = clusterName;
        this.topicFilter = new RegexStringListFilter(topicAllowList, topicDenyList);
        this.consumerGroupFilter = new RegexStringListFilter(consumerGroupAllowList, consumerGroupDenyList);
        this.client = client;
    }

    public ClusterData collectClusterData() {
        final ClusterData clusterData = new ClusterData(clusterName);
        final Set<TopicPartition> topicPartitions = new HashSet<>();
        final boolean clientConnected = client.isConnected();
        final long startMs = System.currentTimeMillis();
        final Set<String> allConsumerGroupIds = client.consumerGroupIds(consumerGroupFilter);
        findConsumerGroupOffsets(allConsumerGroupIds, clusterData, topicPartitions);
        findReplicaCounts(clusterData, topicPartitions);
        findEndOffsetsAndUpdateLag(topicPartitions, clusterData);
        final long pollTimeMs = System.currentTimeMillis() - startMs;
        if (clientConnected) {
            clusterData.setPollTimeMs(pollTimeMs);
        }
        LOG.info("Polled lag data for {} in {} ms", clusterName, pollTimeMs);
        return clusterData;
    }

    private void findConsumerGroupOffsets(final Set<String> consumerGroupIds, final ClusterData clusterData, final Set<TopicPartition> topicPartitions) {
        client.consumerGroupOffsets(consumerGroupIds)
            .forEach((consumerGroup, offsets) -> offsets.forEach((partition, offsetAndMetadata) -> {
                final String topicName = partition.topic();
                if (!topicFilter.isAllowed(topicName)) {
                    return;
                }
                if (offsetAndMetadata == null) {
                    LOG.info("No offset data for partition {}", partition);
                    return;
                }
                final TopicPartitionData topicPartitionData = clusterData.findTopicPartitionData(partition);
                final ConsumerGroupData consumerGroupData = topicPartitionData.findConsumerGroupData(consumerGroup);
                consumerGroupData.setOffset(offsetAndMetadata.offset());
                topicPartitions.add(partition);
            }));
    }

    private void findReplicaCounts(final ClusterData clusterData, final Set<TopicPartition> topicPartitions) {
        Set<String> topics = topicPartitions.stream()
            .map(TopicPartition::topic)
            .collect(Collectors.toSet());
        client.describeTopics(topics).values()
            .forEach(topicDescription -> topicDescription.partitions().forEach(topicPartitionInfo -> {
                final TopicPartition topicPartition = new TopicPartition(topicDescription.name(), topicPartitionInfo.partition());
                clusterData.findTopicPartitionData(topicPartition).setNumReplicas(topicPartitionInfo.replicas().size());
            }));
    }

    private void findEndOffsetsAndUpdateLag(final Set<TopicPartition> topicPartitions, final ClusterData clusterData) {
        if (topicPartitions.isEmpty()) {
            return;
        }
        long t = System.currentTimeMillis();
        final Set<TopicPartition> multiReplicaPartitions = new HashSet<>();
        final Set<TopicPartition> singleReplicaPartitions = new HashSet<>();
        for (final TopicPartition topicPartition : topicPartitions) {
            if (clusterData.findTopicPartitionData(topicPartition).getNumReplicas() > 1) {
                multiReplicaPartitions.add(topicPartition);
            } else {
                singleReplicaPartitions.add(topicPartition);
            }
        }
        findEndOffsetsAndUpdateLagImpl(multiReplicaPartitions, clusterData);
        findEndOffsetsAndUpdateLagImpl(singleReplicaPartitions, clusterData);
        t = System.currentTimeMillis() - t;
        LOG.debug("Found end offsets in {} ms", t);
    }

    private void findEndOffsetsAndUpdateLagImpl(final Set<TopicPartition> topicPartitions, final ClusterData clusterData) {
        try {
            client.endOffsets(topicPartitions)
                .forEach((partition, offset) -> {
                    final TopicPartitionData topicPartitionData = clusterData.findTopicPartitionData(partition);
                    topicPartitionData.calculateLags(offset == null ? -1 : offset);
                });
        } catch (final TimeoutException e) {
            setLagUnknown(topicPartitions, clusterData);
        }
    }

    private static void setLagUnknown(final Set<TopicPartition> topicPartitions, final ClusterData clusterData) {
        for (final TopicPartition topicPartition : topicPartitions) {
            final TopicPartitionData topicPartitionData = clusterData.findTopicPartitionData(topicPartition);
            topicPartitionData.resetLags();
        }
    }

}

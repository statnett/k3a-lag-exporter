package io.statnett.k3a.lagexporter;

import io.statnett.k3a.lagexporter.model.ClusterData;
import io.statnett.k3a.lagexporter.model.ConsumerGroupData;
import io.statnett.k3a.lagexporter.model.ConsumerGroupOffset;
import io.statnett.k3a.lagexporter.model.TopicPartitionData;
import io.statnett.k3a.lagexporter.utils.RegexStringListFilter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

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
        final boolean clientConnected = client.isConnected();
        final long startMs = System.currentTimeMillis();
        final Set<String> allConsumerGroupIds = client.consumerGroupIds(consumerGroupFilter);
        final Map<TopicPartition, List<ConsumerGroupOffset>> groupOffsets = findConsumerGroupOffsets(allConsumerGroupIds);
        final Map<TopicPartition, TopicPartitionData> topicPartitionData = findTopicPartitionData(groupOffsets.keySet());
        final Map<TopicPartitionData, List<ConsumerGroupData>> topicAndConsumerData = calculateLag(groupOffsets, topicPartitionData);
        final long pollTimeMs;
        if (clientConnected) {
            pollTimeMs = System.currentTimeMillis() - startMs;
        } else {
            pollTimeMs = -1;
        }
        final ClusterData clusterData = new ClusterData(clusterName, topicAndConsumerData, pollTimeMs);
        if (allConsumerGroupIds.isEmpty()) {
            LOG.info("No consumer groups found; nothing to do.");
        } else {
            LOG.info("Polled lag data for {} in {} ms", clusterName, pollTimeMs);
        }
        return clusterData;
    }

    private Map<TopicPartition, List<ConsumerGroupOffset>> findConsumerGroupOffsets(final Set<String> consumerGroupIds) {
        final Set<ConsumerGroupOffset> consumerGroupOffsetSet = new HashSet<>();
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
                consumerGroupOffsetSet.add(new ConsumerGroupOffset(partition, consumerGroup, offsetAndMetadata.offset()));
            }));
        return consumerGroupOffsetSet.stream().collect(groupingBy(ConsumerGroupOffset::topicPartition, Collectors.toUnmodifiableList()));
    }

    private Map<TopicPartition, TopicPartitionData> findTopicPartitionData(final Set<TopicPartition> topicPartitions) {
        final Map<TopicPartition, Integer> replicaCounts = findReplicaCounts(topicPartitions);
        final Map<TopicPartition, Long> endOffsets = findEndOffsets(topicPartitions);
        final HashMap<TopicPartition, TopicPartitionData> topicPartitionData = new HashMap<>();
        for (final TopicPartition topicPartition : topicPartitions) {
            final Long endOffset = endOffsets.get(topicPartition);
            final Integer numReplicas = replicaCounts.get(topicPartition);
            topicPartitionData.put(topicPartition, new TopicPartitionData(topicPartition, endOffset == null ? -1 : endOffset, numReplicas == null ? -1 : numReplicas));
        }
        return Collections.unmodifiableMap(topicPartitionData);
    }

    private Map<TopicPartition, Integer> findReplicaCounts(final Set<TopicPartition> topicPartitions) {
        final Map<TopicPartition, Integer> replicaCounts = new HashMap<>();
        final Set<String> topics = topicPartitions.stream()
            .map(TopicPartition::topic)
            .collect(Collectors.toSet());
        client.describeTopics(topics).values()
            .forEach(topicDescription -> topicDescription.partitions().forEach(topicPartitionInfo -> {
                final TopicPartition topicPartition = new TopicPartition(topicDescription.name(), topicPartitionInfo.partition());
                replicaCounts.put(topicPartition, topicPartitionInfo.replicas().size());
            }));
        return Collections.unmodifiableMap(replicaCounts);
    }

    private Map<TopicPartition, Long> findEndOffsets(final Set<TopicPartition> topicPartitions) {
        if (topicPartitions.isEmpty()) {
            return Collections.emptyMap();
        }
        long t = System.currentTimeMillis();
        final Map<TopicPartition, Long> endOffsets = new HashMap<>();
        try {
            client.endOffsets(topicPartitions)
                .forEach((partition, offset) -> endOffsets.put(partition, (offset == null ? -1 : offset)));
        } catch (final TimeoutException e) {
            LOG.debug("Timed out getting endOffsets");
        }
        t = System.currentTimeMillis() - t;
        LOG.debug("Found end offsets in {} ms", t);
        return Collections.unmodifiableMap(endOffsets);
    }

    private static Map<TopicPartitionData, List<ConsumerGroupData>> calculateLag(
    final Map<TopicPartition, List<ConsumerGroupOffset>> consumerGroupOffsets,
    final Map<TopicPartition, TopicPartitionData> topicPartitionData
    ) {
        final Map<TopicPartitionData, List<ConsumerGroupData>> topicAndConsumerData = new HashMap<>();
        for (final Map.Entry<TopicPartition, List<ConsumerGroupOffset>> entry : consumerGroupOffsets.entrySet()) {
            final TopicPartitionData partitionData = topicPartitionData.get(entry.getKey());
            final List<ConsumerGroupData> consumerGroupData = new ArrayList<>(entry.getValue().size());
            for (final ConsumerGroupOffset consumerGroupOffset : entry.getValue()) {
                consumerGroupData.add(new ConsumerGroupData(consumerGroupOffset, partitionData.endOffset()));
            }
            topicAndConsumerData.put(partitionData, consumerGroupData);
        }
        return Collections.unmodifiableMap(topicAndConsumerData);
    }
}

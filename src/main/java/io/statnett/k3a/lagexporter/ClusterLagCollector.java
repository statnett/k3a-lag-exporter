package io.statnett.k3a.lagexporter;

import io.statnett.k3a.lagexporter.model.ClusterData;
import io.statnett.k3a.lagexporter.model.ConsumerGroupData;
import io.statnett.k3a.lagexporter.model.TopicPartitionData;
import io.statnett.k3a.lagexporter.utils.RegexStringListFilter;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class ClusterLagCollector {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterLagCollector.class);
    private final String clusterName;
    private final RegexStringListFilter topicFilter;
    private final RegexStringListFilter consumerGroupFilter;
    private final Map<String, Object> consumerConfig;
    private final Map<String, Object> adminConfig;
    private Admin admin;
    private Consumer<?, ?> consumer;

    public ClusterLagCollector(final String clusterName,
                               final Collection<String> topicAllowList, final Collection<String> topicDenyList,
                               final Collection<String> consumerGroupAllowList, final Collection<String> consumerGroupDenyList,
                               final Map<String, Object> consumerConfig, final Map<String, Object> adminConfig) {
        this.clusterName = clusterName;
        this.topicFilter = new RegexStringListFilter(topicAllowList, topicDenyList);
        this.consumerGroupFilter = new RegexStringListFilter(consumerGroupAllowList, consumerGroupDenyList);
        this.consumerConfig = consumerConfig;
        this.adminConfig = adminConfig;
    }

    public ClusterData collectClusterData() {
        final ClusterData clusterData = new ClusterData(clusterName);
        final Set<TopicPartition> topicPartitions = new HashSet<>();
        final boolean isFirstRun = admin == null || consumer == null;
        final long startMs = System.currentTimeMillis();
        final Set<String> allConsumerGroupIds = findAllConsumerGroupIds(getAdmin());
        findConsumerGroupOffsets(getAdmin(), allConsumerGroupIds, clusterData, topicPartitions);
        findReplicaCounts(getAdmin(), clusterData, topicPartitions);
        findEndOffsetsAndUpdateLag(getConsumer(), topicPartitions, clusterData);
        final long pollTimeMs = System.currentTimeMillis() - startMs;
        if (!isFirstRun) {
            clusterData.setPollTimeMs(pollTimeMs);
        }
        LOG.info("Polled lag data for " + clusterName + " in " + pollTimeMs + " ms");
        return clusterData;
    }

    private Set<String> findAllConsumerGroupIds(final Admin admin) {
        try {
            long t = System.currentTimeMillis();
            final Set<String> consumerGroupIds = new HashSet<>();
            final ListConsumerGroupsResult listConsumerGroupsResult = admin.listConsumerGroups();
            for (final ConsumerGroupListing consumerGroupListing : listConsumerGroupsResult.all().get()) {
                final String consumerGroupId = consumerGroupListing.groupId();
                if (consumerGroupFilter.isAllowed(consumerGroupId)) {
                    consumerGroupIds.add(consumerGroupId);
                }
            }
            t = System.currentTimeMillis() - t;
            LOG.debug("Found all consumer group ids in " + t + " ms");
            return consumerGroupIds;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void findConsumerGroupOffsets(final Admin admin, final Set<String> consumerGroupIds, final ClusterData clusterData, final Set<TopicPartition> topicPartitions) {
        try {
            long t = System.currentTimeMillis();
            final ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = admin.listConsumerGroupOffsets(toMapForAllOffsets(consumerGroupIds));
            for (final Map.Entry<String, Map<TopicPartition, OffsetAndMetadata>> all : listConsumerGroupOffsetsResult.all().get().entrySet()) {
                final String consumerGroupId = all.getKey();
                for (final Map.Entry<TopicPartition, OffsetAndMetadata> partitionOffsetAndMetadataEntry : all.getValue().entrySet()) {
                    final TopicPartition partition = partitionOffsetAndMetadataEntry.getKey();
                    final String topicName = partition.topic();
                    if (!topicFilter.isAllowed(topicName)) {
                        continue;
                    }
                    final OffsetAndMetadata data = partitionOffsetAndMetadataEntry.getValue();
                    if (data == null) {
                        LOG.info("No offset data for partition " + partition);
                        continue;
                    }
                    final TopicPartitionData topicPartitionData = clusterData.findTopicPartitionData(partition);
                    final ConsumerGroupData consumerGroupData = topicPartitionData.findConsumerGroupData(consumerGroupId);
                    consumerGroupData.setOffset(data.offset());
                    topicPartitions.add(partition);
                }
            }
            t = System.currentTimeMillis() - t;
            LOG.debug("Found consumer group offsets in " + t + " ms");
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, ListConsumerGroupOffsetsSpec> toMapForAllOffsets(final Set<String> consumerGroupIds) {
        final ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec().topicPartitions(null);
        final Map<String,ListConsumerGroupOffsetsSpec> map = new HashMap<>();
        for (final String consumerGroupId : consumerGroupIds) {
            map.put(consumerGroupId, spec);
        }
        return map;
    }

    private void findReplicaCounts(final Admin admin, final ClusterData clusterData, final Set<TopicPartition> topicPartitions) {
        final Set<String> topics = new HashSet<>();
        for (final TopicPartition topicPartition : topicPartitions) {
            topics.add(topicPartition.topic());
        }
        try {
            final Collection<TopicDescription> topicDescriptions = admin.describeTopics(topics).allTopicNames().get().values();
            for (final TopicDescription topicDescription : topicDescriptions) {
                for (final TopicPartitionInfo topicPartitionInfo : topicDescription.partitions()) {
                    final TopicPartition topicPartition = new TopicPartition(topicDescription.name(), topicPartitionInfo.partition());
                    clusterData.findTopicPartitionData(topicPartition).setNumReplicas(topicPartitionInfo.replicas().size());
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void findEndOffsetsAndUpdateLag(final Consumer<?, ?> consumer, final Set<TopicPartition> topicPartitions, final ClusterData clusterData) {
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
        findEndOffsetsAndUpdateLagImpl(consumer, multiReplicaPartitions, clusterData);
        findEndOffsetsAndUpdateLagImpl(consumer, singleReplicaPartitions, clusterData);
        t = System.currentTimeMillis() - t;
        LOG.debug("Found end offsets in " + t + " ms");
    }

    private void findEndOffsetsAndUpdateLagImpl(final Consumer<?, ?> consumer, final Set<TopicPartition> topicPartitions, final ClusterData clusterData) {
        try {
            final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
            for (final Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
                final TopicPartition partition = entry.getKey();
                final Long offset = entry.getValue();
                final TopicPartitionData topicPartitionData = clusterData.findTopicPartitionData(partition);
                topicPartitionData.setEndOffset(offset == null ? -1 : offset);
                topicPartitionData.calculateLags();
            }
        } catch (final TimeoutException e) {
            LOG.warn("Got timeout while querying end offsets. Some partitions may be offline.");
            for (final TopicPartition topicPartition : topicPartitions) {
                final TopicPartitionData topicPartitionData = clusterData.findTopicPartitionData(topicPartition);
                topicPartitionData.setEndOffset(-1);
                topicPartitionData.calculateLags();
            }
        }
    }

    private Admin getAdmin() {
        if (admin == null) {
            admin = AdminClient.create(adminConfig);
        }
        return admin;
    }

    private Consumer<?, ?> getConsumer() {
        if (consumer == null) {
            consumer = new KafkaConsumer<>(consumerConfig);
        }
        return consumer;
    }

}

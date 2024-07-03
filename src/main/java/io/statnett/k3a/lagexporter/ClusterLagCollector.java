package io.statnett.k3a.lagexporter;

import io.statnett.k3a.lagexporter.model.ClusterData;
import io.statnett.k3a.lagexporter.model.ConsumerGroupData;
import io.statnett.k3a.lagexporter.model.TopicPartitionData;
import io.statnett.k3a.lagexporter.utils.RegexStringListFilter;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
        final Set<String> consumerGroupIds = new HashSet<>();
        try {
            long t = System.currentTimeMillis();
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
            if (isTimeout(e)) {
                LOG.warn("Got timeout while listing consumer groups.");
                invalidateClients();
                return consumerGroupIds;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private void findConsumerGroupOffsets(final Admin admin, final Set<String> consumerGroupIds, final ClusterData clusterData, final Set<TopicPartition> topicPartitions) {
        if (consumerGroupIds.isEmpty()) {
            return;
        }
        try {
            long t = System.currentTimeMillis();
            admin.listConsumerGroupOffsets(toMapForAllOffsets(consumerGroupIds)).all().get()
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
            t = System.currentTimeMillis() - t;
            LOG.debug("Found consumer group offsets in {} ms", t);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, ListConsumerGroupOffsetsSpec> toMapForAllOffsets(final Set<String> consumerGroupIds) {
        final ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec().topicPartitions(null);
        final Map<String, ListConsumerGroupOffsetsSpec> map = new HashMap<>();
        for (final String consumerGroupId : consumerGroupIds) {
            map.put(consumerGroupId, spec);
        }
        return map;
    }

    private void findReplicaCounts(final Admin admin, final ClusterData clusterData, final Set<TopicPartition> topicPartitions) {
        if (topicPartitions.isEmpty()) {
            return;
        }
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
        findEndOffsetsAndUpdateLagImpl(consumer, multiReplicaPartitions, clusterData);
        findEndOffsetsAndUpdateLagImpl(consumer, singleReplicaPartitions, clusterData);
        t = System.currentTimeMillis() - t;
        LOG.debug("Found end offsets in " + t + " ms");
    }

    private void findEndOffsetsAndUpdateLagImpl(final Consumer<?, ?> consumer, final Set<TopicPartition> topicPartitions, final ClusterData clusterData) {
        try {
            consumer.endOffsets(topicPartitions)
                .forEach((partition, offset) -> {
                    final TopicPartitionData topicPartitionData = clusterData.findTopicPartitionData(partition);
                    topicPartitionData.setEndOffset(offset == null ? -1 : offset);
                    topicPartitionData.calculateLags();
                });
        } catch (final Exception e) {
            if (isTimeout(e)) {
                LOG.warn("Got timeout while querying end offsets. Some partitions may be offline.");
                setLagUnknown(topicPartitions, clusterData);
                invalidateClients();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private static void setLagUnknown(final Set<TopicPartition> topicPartitions, final ClusterData clusterData) {
        for (final TopicPartition topicPartition : topicPartitions) {
            final TopicPartitionData topicPartitionData = clusterData.findTopicPartitionData(topicPartition);
            topicPartitionData.setEndOffset(-1);
            topicPartitionData.calculateLags();
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

    private void invalidateClients() {
        if (admin != null) {
            try {
                admin.close();
            } catch (final Exception e) {
                LOG.info("Error closing admin client (ignoring): " + e.getMessage());
            } finally {
                admin = null;
            }
        }
        if (consumer != null) {
            try {
                consumer.close();
            } catch (final Exception e) {
                LOG.info("Error closing consumer client (ignoring): " + e.getMessage());
            } finally {
                consumer = null;
            }
        }
    }

    private static boolean isTimeout(final Throwable t) {
        Throwable current = t;
        while (current != null) {
            if (TimeoutException.class.isAssignableFrom(current.getClass())) {
                return true;
            }
            if (current == current.getCause()) {
                break;
            }
            current = current.getCause();
        }
        return false;
    }

}

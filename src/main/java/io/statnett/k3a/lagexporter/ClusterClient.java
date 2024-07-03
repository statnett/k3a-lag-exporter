package io.statnett.k3a.lagexporter;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.statnett.k3a.lagexporter.utils.Exceptions.findTimeoutException;
import static java.util.Collections.emptySet;
import static java.util.function.Function.identity;

public class ClusterClient {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Map<String, Object> adminConfig;
    private final Map<String, Object> consumerConfig;

    private Admin admin;
    private Consumer<?, ?> consumer;

    public ClusterClient(Map<String, Object> adminConfig, Map<String, Object> consumerConfig) {
        this.adminConfig = adminConfig;
        this.consumerConfig = consumerConfig;
    }

    public Map<TopicPartition, Long> endOffsets(final Set<TopicPartition> topicPartitions) {
        try {
            return getConsumer().endOffsets(topicPartitions);
        } catch (final RuntimeException e) {
            TimeoutException timeoutException = findTimeoutException(e);
            if (timeoutException != null) {
                log.warn("Got timeout while querying end offsets. Some partitions may be offline.");
                close();
                throw timeoutException;
            } else {
                throw e;
            }
        }
    }

    public Set<String> consumerGroupIds(final Predicate<String> filter) {
        long t = System.currentTimeMillis();
        try {
            return getAdmin().listConsumerGroups().all().get().stream()
                .map(ConsumerGroupListing::groupId)
                .filter(filter)
                .collect(Collectors.toSet());
        } catch (final Exception e) {
            TimeoutException timeoutException = findTimeoutException(e);
            if (timeoutException != null) {
                log.warn("Got timeout while querying end offsets. Some partitions may be offline.");
                close();
                return emptySet();
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            t = System.currentTimeMillis() - t;
            log.debug("Found all consumer group ids in {} ms", t);
        }
    }

    public Map<String, Map<TopicPartition, OffsetAndMetadata>> consumerGroupOffsets(final Set<String> consumerGroupIds) {
        if (consumerGroupIds.isEmpty()) {
            return Collections.emptyMap();
        }
        long t = System.currentTimeMillis();
        try {
            Map<String, ListConsumerGroupOffsetsSpec> groupSpecs = consumerGroupIds.stream()
                .collect(Collectors.toMap(identity(), s -> new ListConsumerGroupOffsetsSpec().topicPartitions(null)));
            return admin.listConsumerGroupOffsets(groupSpecs).all().get();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            t = System.currentTimeMillis() - t;
            log.debug("Found consumer group offsets in {} ms", t);
        }
    }

    public Map<String, TopicDescription> describeTopics(final Set<String> topics) {
        if (topics.isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            return getAdmin().describeTopics(topics).allTopicNames().get();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (final Exception e) {
                log.info("Error closing admin client (ignoring): {}", e.getMessage());
            } finally {
                admin = null;
            }
        }
        if (consumer != null) {
            try {
                consumer.close();
            } catch (final Exception e) {
                log.info("Error closing consumer client (ignoring): {}", e.getMessage());
            } finally {
                consumer = null;
            }
        }
    }

    public boolean isConnected() {
        return admin != null && consumer != null;
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

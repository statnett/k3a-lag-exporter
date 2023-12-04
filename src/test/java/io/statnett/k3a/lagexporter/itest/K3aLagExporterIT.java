package io.statnett.k3a.lagexporter.itest;

import io.statnett.k3a.lagexporter.ClusterLagCollector;
import io.statnett.k3a.lagexporter.model.ClusterData;
import io.statnett.k3a.lagexporter.model.ConsumerGroupData;
import io.statnett.k3a.lagexporter.model.TopicPartitionData;
import no.shhsoft.k3aembedded.K3aEmbedded;
import no.shhsoft.k3aembedded.K3aTestUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public final class K3aLagExporterIT {

    private static K3aEmbedded broker;
    private static final String CLUSTER_NAME = "the-cluster";
    private static final String TOPIC = "the-topic";
    private static final String CONSUMER_GROUP_ID = "consumer-group";
    private static ClusterLagCollector lagCollector;
    private int lastProducedValue = -1;

    @BeforeAll
    public static void beforeClass() {
        broker = new K3aEmbedded.Builder().build();
        broker.start();
    }

    @AfterAll
    public static void afterClass() {
        broker.stop();
    }

    @Test
    public void shouldDetectLag() {
        lagCollector = new ClusterLagCollector(CLUSTER_NAME,
                                               null, null, null, null,
                                               getMinimalConsumerConfig(), getMinimalAdminConfig());
        try (final Producer<Integer, String> producer = new KafkaProducer<>(K3aTestUtils.producerProps(broker))) {
            try (final Consumer<Integer, String> consumer = new KafkaConsumer<>(K3aTestUtils.consumerProps(CONSUMER_GROUP_ID, false, broker))) {
                consumer.subscribe(Collections.singleton(TOPIC));
                produce(producer);
                int consumedValue = consume(consumer);
                assertEquals(lastProducedValue, consumedValue);
                assertLag(0);
                produce(producer);
                assertLag(1);
                produce(producer);
                assertLag(2);
                produce(producer);
                assertLag(3);
                do {
                    consumedValue = consume(consumer);
                } while (consumedValue < lastProducedValue);
                assertLag(0);
            }
        }
    }

    private void assertLag(final int expected) {
        final ClusterData clusterData = lagCollector.collect();
        final TopicPartitionData topicPartitionData = clusterData.findTopicPartitionData(new TopicPartition(TOPIC, 0));
        assertNotNull(topicPartitionData);
        final ConsumerGroupData consumerGroupData = topicPartitionData.findConsumerGroupData(CONSUMER_GROUP_ID);
        assertNotNull(consumerGroupData);
        assertEquals(expected, consumerGroupData.getLag(), 0.00001);
    }

    private void produce(final Producer<Integer, String> producer) {
        final ProducerRecord<Integer, String> record = new ProducerRecord<>(TOPIC, 0, String.valueOf(++lastProducedValue));
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    throw (exception instanceof RuntimeException) ? (RuntimeException) exception : new RuntimeException(exception);
                }
            }).get(); // Make call synchronous, to be able to get exceptions in time.
        } catch (final InterruptedException | ExecutionException e) {
            final Throwable cause = e.getCause();
            throw (cause instanceof RuntimeException) ? (RuntimeException) cause : new RuntimeException(e);
        }
        producer.flush();
    }

    private int consume(final Consumer<Integer, String> consumer) {
        int lastValue = -1;
        final ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000));
        for (final ConsumerRecord<Integer, String> record : records) {
            lastValue = Integer.valueOf(record.value());
            consumer.commitAsync();
        }
        return lastValue;
    }

    public Map<String, Object> getMinimalAdminConfig() {
        return getCommonConfig();
    }

    public Map<String, Object> getMinimalConsumerConfig() {
        final Map<String, Object> map = getCommonConfig();
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return map;
    }

    private Map<String, Object> getCommonConfig() {
        final Map<String, Object> map = new HashMap<>();
        map.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        map.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers());
        return map;
    }

}

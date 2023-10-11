package no.statnett.k3alagexporter.itest;

import no.shhsoft.k3aembedded.K3aEmbedded;
import no.statnett.k3alagexporter.ClusterLagCollector;
import no.statnett.k3alagexporter.model.ClusterData;
import no.statnett.k3alagexporter.model.ConsumerGroupData;
import no.statnett.k3alagexporter.model.TopicPartitionData;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
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
        try (final Producer<Integer, Integer> producer = getProducer()) {
            try (final Consumer<Integer, Integer> consumer = getConsumer(CONSUMER_GROUP_ID)) {
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

    private void produce(final Producer<Integer, Integer> producer) {
        final ProducerRecord<Integer, Integer> record = new ProducerRecord<>(TOPIC, 0, ++lastProducedValue);
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

    private int consume(final Consumer<Integer, Integer> consumer) {
        int lastValue = -1;
        final ConsumerRecords<Integer, Integer> records = consumer.poll(Duration.ofMillis(1000));
        for (final ConsumerRecord<Integer, Integer> record : records) {
            lastValue = record.value();
            consumer.commitAsync();
        }
        return lastValue;
    }

    public Producer<Integer, Integer> getProducer() {
        final Map<String, Object> map = getCommonConfig();
        map.put(ProducerConfig.ACKS_CONFIG, "all");
        map.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "3000");
        map.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");
        map.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        map.put(ProducerConfig.RETRIES_CONFIG, "0");
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        return new KafkaProducer<>(map);
    }

    public Consumer<Integer, Integer> getConsumer(final String consumerGroupId) {
        final Map<String, Object> map = getCommonConfig();
        map.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        map.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        return new KafkaConsumer<>(map);
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
        map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers());
        return map;
    }

}

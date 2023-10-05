package no.statnett.k3alagexporter.itest.services;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.HashMap;
import java.util.Map;

public final class KafkaCluster {
    private final EmbeddedKafkaBroker broker;

    public KafkaCluster(EmbeddedKafkaBroker broker) {
        this.broker = broker;
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
        map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        return map;
    }

}

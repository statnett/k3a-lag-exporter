package no.statnett.k3alagexporter.itest;

import no.statnett.k3alagexporter.ClusterLagCollector;
import no.statnett.k3alagexporter.Conf;
import no.statnett.k3alagexporter.itest.services.KafkaCluster;
import no.statnett.k3alagexporter.model.ClusterData;
import no.statnett.k3alagexporter.model.ConsumerGroupData;
import no.statnett.k3alagexporter.model.TopicPartitionData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

public final class K3aLagExporterIT {

    private static KafkaCluster kafkaCluster;
    private static final String TOPIC = "the-topic";
    private static final String CONSUMER_GROUP_ID = "consumer-group";
    private static ClusterLagCollector lagCollector;
    private int nextProducedValue = 0;

    @BeforeClass
    public static void before() {
        kafkaCluster = new KafkaCluster();
        kafkaCluster.start();
        Conf.setFromString(createConfig(kafkaCluster));
        lagCollector = new ClusterLagCollector(Conf.getClusterName());
    }

    @AfterClass
    public static void after() {
        kafkaCluster.stop();
    }

    private static String createConfig(final KafkaCluster kafkaCluster) {
        return "kafka-lag-exporter {\n"
               + "  clusters = [ {\n"
               + "    name = \"the-cluster\"\n"
               + "    bootstrap-brokers = \"" + kafkaCluster.getBootstrapServers() + "\"\n"
               + "    consumer-properties = {}\n"
               + "    admin-client-properties = {}\n"
               + "  } ]\n"
               + "}\n";
    }

    @Test
    public void test() {
        try (final Producer<Integer, Integer> producer = kafkaCluster.getProducer()) {
            try (final Consumer<Integer, Integer> consumer = kafkaCluster.getConsumer(CONSUMER_GROUP_ID)) {
                consumer.subscribe(Collections.singleton(TOPIC));
                produce(producer);
                final int consumedValue = consume(consumer);
                Assert.assertEquals(nextProducedValue - 1, consumedValue);
                assertLag(0);
                produce(producer);
                produce(producer);
                produce(producer);
                assertLag(3);
                consume(consumer);
                consume(consumer);
                consume(consumer);
                assertLag(0);
            }
        }
    }

    private void assertLag(final int expected) {
        final ClusterData clusterData = lagCollector.collect();
        final TopicPartitionData topicPartitionData = clusterData.findTopicPartitionData(new TopicPartition(TOPIC, 0));
        Assert.assertNotNull(topicPartitionData);
        final ConsumerGroupData consumerGroupData = topicPartitionData.findConsumerGroupData(CONSUMER_GROUP_ID);
        Assert.assertNotNull(consumerGroupData);
        Assert.assertEquals(expected, consumerGroupData.getLag(), 0.00001);
    }

    private void produce(final Producer<Integer, Integer> producer) {
        final ProducerRecord<Integer, Integer> record = new ProducerRecord<>(TOPIC, 0, nextProducedValue);
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    throw (exception instanceof RuntimeException) ? (RuntimeException) exception : new RuntimeException(exception);
                }
            }).get(); // Make call synchronous, to be able to get exceptions in time.
            ++nextProducedValue;
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

}

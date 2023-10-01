package no.statnett.k3alagexporter.itest;

import no.statnett.k3alagexporter.itest.services.KafkaCluster;
import no.statnett.k3alagexporter.itest.services.LagExporter;
import no.statnett.k3alagexporter.itest.utils.MetricsParser;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

public final class K3aLagExporterIT {

    private static KafkaCluster kafkaCluster;
    private static LagExporter lagExporter;
    private static final String TOPIC = "the-topic";
    private static final String CONSUMER_GROUP_ID = "consumer-group";
    private int nextProducedValue = 0;

    @BeforeClass
    public static void before() {
        kafkaCluster = new KafkaCluster();
        kafkaCluster.start();
        lagExporter = new LagExporter(kafkaCluster);
        lagExporter.start();
    }

    @AfterClass
    public static void after() {
        lagExporter.stop();
        kafkaCluster.stop();
    }

    @Test
    public void test() {
        try (final Producer<Integer, Integer> producer = getProducer()) {
            try (final Consumer<Integer, Integer> consumer = getConsumer(CONSUMER_GROUP_ID)) {
                consumer.subscribe(Collections.singleton(TOPIC));
                produce(producer);
                final int consumedValue = consume(consumer);
                Assert.assertEquals(nextProducedValue - 1, consumedValue);
                sleepSeconds(1);
                assertLag(0);
                produce(producer);
                produce(producer);
                produce(producer);
                sleepSeconds(1);
                assertLag(3);
                consume(consumer);
                consume(consumer);
                consume(consumer);
                sleepSeconds(1);
                assertLag(0);
            }
        }
    }

    private void sleepSeconds(final int seconds) {
        try {
            Thread.sleep(seconds * 1000L);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertLag(final int expected) {
        final List<MetricsParser.Metric> metrics = lagExporter.getMetrics();
        MetricsParser.Metric metric = null;
        for (final MetricsParser.Metric m : metrics) {
            if ("k3a_consumergroup_group_lag".equals(m.name()) && TOPIC.equals(m.labels().get("topic"))) {
                metric = m;
                break;
            }
        }
        if (metric == null) {
            throw new RuntimeException("Did not find the metric");
        }
        Assert.assertEquals(expected, metric.value(), 0.00001);
    }

    private Producer<Integer, Integer> getProducer() {
        return kafkaCluster.getProducer();
    }

    private Consumer<Integer, Integer> getConsumer(final String consumerGroupId) {
        return kafkaCluster.getConsumer(consumerGroupId);
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

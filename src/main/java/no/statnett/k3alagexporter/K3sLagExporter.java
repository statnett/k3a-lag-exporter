package no.statnett.k3alagexporter;

import no.statnett.k3alagexporter.model.ClusterData;
import no.statnett.k3alagexporter.model.ConsumerGroupData;
import no.statnett.k3alagexporter.model.TopicPartitionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class K3sLagExporter {

    private static final Logger LOG = LoggerFactory.getLogger(K3sLagExporter.class);

    private void doit() {
        final long msBetweenCollections = Conf.getPollIntervalMs();
        final PrometheusReporter prometheusReporter = new PrometheusReporter();
        prometheusReporter.start();
        final ClusterLagCollector collector = new ClusterLagCollector(Conf.getClusterName());
        for (;;) {
            long t = System.currentTimeMillis();
            final ClusterData clusterData = collector.collect();
            prometheusReporter.publish(clusterData);
            t = System.currentTimeMillis() - t;
            try {
                Thread.sleep(Math.max(0, msBetweenCollections - t));
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void dumpToStdout(final ClusterData clusterData) {
        int numTopicPartitions = 0;
        int numConsumerGroups = 0;
        for (final TopicPartitionData topicPartitionData : clusterData.getAllTopicPartitionData()) {
            ++ numTopicPartitions;
            System.out.println("Partition: " + topicPartitionData.getTopicPartition() + ": endOffset=" + topicPartitionData.getEndOffset());
            for (final ConsumerGroupData consumerGroupData : topicPartitionData.getConsumerGroupDataMap().values()) {
                ++numConsumerGroups;
                System.out.println("  Consumer group: " + consumerGroupData.getConsumerGroupId() + ": offset=" + consumerGroupData.getOffset() + ", lag=" + consumerGroupData.getLag());
            }
        }
        System.out.println("\nIn total " + numTopicPartitions + " topic partitions and " + numConsumerGroups + " consumer groups.");
    }

    public static void main(final String[] args) {
        new K3sLagExporter().doit();
    }

}

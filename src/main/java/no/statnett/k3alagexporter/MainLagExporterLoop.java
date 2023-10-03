package no.statnett.k3alagexporter;

import no.statnett.k3alagexporter.model.ClusterData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MainLagExporterLoop {

    private static final Logger LOG = LoggerFactory.getLogger(MainLagExporterLoop.class);

    public void runLoop() {
        try {
            final long msBetweenCollections = Conf.getPollIntervalMs();
            LOG.info("Starting polling every " + msBetweenCollections + " ms");
            final PrometheusReporter prometheusReporter = new PrometheusReporter();
            prometheusReporter.start();
            final ClusterLagCollector collector = new ClusterLagCollector(Conf.getClusterName(),
                                                                          Conf.getTopicAllowList(), Conf.getTopicDenyList(),
                                                                          Conf.getConsumerGroupAllowList(), Conf.getConsumerGroupDenyList(),
                                                                          Conf.getConsumerConfig(), Conf.getAdminConfig());
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
        } catch (final Exception e) {
            LOG.error("Fatal error. Terminating.", e);
            System.exit(1);
        }
    }

}

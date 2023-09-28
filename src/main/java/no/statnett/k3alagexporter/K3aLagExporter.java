package no.statnett.k3alagexporter;

import no.statnett.k3alagexporter.model.ClusterData;

public final class K3aLagExporter {

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

    public static void main(final String[] args) {
        new K3aLagExporter().doit();
    }

}

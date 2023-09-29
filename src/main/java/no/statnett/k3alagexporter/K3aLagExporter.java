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

    private static void showHelpAndExit() {
        System.out.println("For configuration and usage of k3a-lag-exporter, please see");
        System.out.println("https://github.com/statnett/k3a-lag-exporter");
        System.exit(0);
    }

    public static void main(final String[] args) {
        if (args.length != 0) {
            showHelpAndExit();
        }
        final String configFile = System.getProperty("config.file");
        if (configFile == null) {
            System.err.println("You need to specify a config.file property.");
            System.exit(1);
        }
        Conf.setFromFile(configFile);
        new K3aLagExporter().doit();
    }

}

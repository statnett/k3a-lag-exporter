package no.statnett.k3alagexporter;

import no.statnett.k3alagexporter.model.ClusterData;
import no.statnett.k3alagexporter.utils.LogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class K3aLagExporter {

    private static void showHelpAndExit() {
        System.out.println("For configuration and usage of k3a-lag-exporter, please see");
        System.out.println("https://github.com/statnett/k3a-lag-exporter");
        System.exit(0);
    }

    public static void main(final String[] args) {
        LogUtils.initLogging();
        if (args.length != 0) {
            showHelpAndExit();
        }
        final String configFile = System.getProperty("config.file");
        if (configFile == null) {
            System.err.println("You need to specify a config.file property.");
            System.exit(1);
        }
        Conf.setFromFile(configFile);
        new MainLagExporterLoop().runLoop();
    }

}

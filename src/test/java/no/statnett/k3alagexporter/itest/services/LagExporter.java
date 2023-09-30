package no.statnett.k3alagexporter.itest.services;

import no.statnett.k3alagexporter.Conf;
import no.statnett.k3alagexporter.K3aLagExporter;
import no.statnett.k3alagexporter.itest.utils.MetricsParser;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

public final class LagExporter {

    private final KafkaCluster kafkaCluster;
    private K3aLagExporter lagExporter;
    private final MetricsParser metricsParser = new MetricsParser();

    public LagExporter(final KafkaCluster kafkaCluster) {
        this.kafkaCluster = kafkaCluster;
    }

    public void start() {
        Conf.setFromString(createConfig());
        lagExporter = new K3aLagExporter();
        new Thread(() -> lagExporter.start()).start();
    }

    public void stop() {
        lagExporter.stop();
    }

    private String createConfig() {
        return "kafka-lag-exporter {\n"
               + "  poll-interval = 300 ms\n"
               + "  clusters = [ {\n"
               + "    name = \"the-cluster\"\n"
               + "    bootstrap-brokers = \"" + kafkaCluster.getBootstrapServers() + "\"\n"
               + "    consumer-properties = {}\n"
               + "    admin-client-properties = {}\n"
               + "  } ]\n"
               + "}\n";
    }

    public List<MetricsParser.Metric> getMetrics() {
        final String metricsContents = getMetricsContents();
        return metricsParser.getMetrics(metricsContents);
    }

    private String getMetricsContents() {
        try {
            final URL url = new URL("http://localhost:8000/metrics");
            final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            final int responseCode = conn.getResponseCode();
            if (responseCode >= 300) {
                throw new RuntimeException("Expected 2xx, got " + responseCode);
            }
            return new String(conn.getInputStream().readAllBytes());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}

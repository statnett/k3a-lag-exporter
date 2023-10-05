package no.statnett.k3alagexporter.itest.k3aembedded;

import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import kafka.server.MetaProperties;
import kafka.server.Server;
import kafka.tools.StorageTool;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.MetadataVersion;
import scala.Option;
import scala.collection.immutable.Seq;
import scala.jdk.CollectionConverters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class K3aEmbedded {

    private static final String NODE_ID = "1";
    private int brokerPort = -1;
    private Server server;
    private Path logDirectory;

    private HashMap<String, String> getConfigMap(final Path logDir, final int brokerPort, final int controllerPort) {
        final HashMap<String, String> map = new HashMap<>();
        map.put("node.id", NODE_ID);
        map.put("process.roles", "broker, controller");
        map.put("controller.quorum.voters", NODE_ID + "@localhost:" + controllerPort);
        map.put("controller.listener.names", "CONTROLLER");
        map.put("inter.broker.listener.name", "BROKER");
        map.put("listeners", "BROKER://:" + brokerPort + ", CONTROLLER://:" + controllerPort);
        map.put("listener.security.protocol.map", "BROKER:PLAINTEXT, CONTROLLER:PLAINTEXT");
        map.put("log.dir", logDir.toString());
        map.put("offsets.topic.num.partitions", "1");
        map.put("offsets.topic.replication.factor", "1");
        map.put("group.initial.rebalance.delay.ms", "0");
        return map;
    }

    public void start() {
        if (server != null) {
            throw new RuntimeException("Server already started");
        }
        logDirectory = createKafkaLogDirectory();
        brokerPort = NetworkUtils.getRandomAvailablePort();
        final int controllerPort = NetworkUtils.getRandomAvailablePort();
        final Map<String, String> map = getConfigMap(logDirectory, brokerPort, controllerPort);
        final KafkaConfig config = new KafkaConfig(map);
        formatKafkaLogDirectory(config);
        server = new KafkaRaftServer(config, Time.SYSTEM);
        server.startup();
    }

    public void stop() {
        if (server == null) {
            return;
        }
        server.shutdown();
        server.awaitShutdown();
        server = null;
        FileUtils.deleteRecursively(logDirectory.toFile());
        logDirectory = null;
    }

    public int getBrokerPort() {
        return brokerPort;
    }

    public String getBootstrapServers() {
        return "localhost:" + getBrokerPort();
    }

    private Path createKafkaLogDirectory() {
        try {
            return Files.createTempDirectory("kafka");
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void formatKafkaLogDirectory(final KafkaConfig kafkaConfig) {
        if (logDirectory == null) {
            throw new RuntimeException("No log directory. This should not happen.");
        }
        final String clusterId = Uuid.randomUuid().toString();
        final MetadataVersion metadataVersion = MetadataVersion.latest();
        final MetaProperties metaProperties = StorageTool.buildMetadataProperties(clusterId, kafkaConfig);
        final BootstrapMetadata bootstrapMetadata = StorageTool.buildBootstrapMetadata(metadataVersion, Option.empty(), "format command");
        final Seq<String> seq = CollectionConverters.ListHasAsScala(Collections.singletonList(logDirectory.toString())).asScala().toList().toSeq();
        StorageTool.formatCommand(System.out, seq, metaProperties, bootstrapMetadata, metadataVersion, false);
    }

}

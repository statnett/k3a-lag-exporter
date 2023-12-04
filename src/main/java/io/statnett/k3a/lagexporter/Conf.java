package io.statnett.k3a.lagexporter;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Conf {

    private static final Config DEFAULT_CONFIG = ConfigFactory.parseResources("reference.conf");
    private static Config conf = ConfigFactory.load();
    private static final String MAIN_OBJECT_NAME = "k3a-lag-exporter";

    public static int getPrometheusPort() {
        return conf.getInt(MAIN_OBJECT_NAME + ".reporters.prometheus.port");
    }

    public static String getPrometheusMetricNamespace() {
        return conf.getString(MAIN_OBJECT_NAME + ".reporters.prometheus.metric-namespace");
    }

    public static String getClusterName() {
        return getCluster().getString("name");
    }

    public static Map<String, Object> getConsumerConfig() {
        final Map<String, Object> map = configToMap(getCluster().getConfig("consumer-properties"));
        addTimeoutConfigs(map);
        map.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        map.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        map.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return map;
    }

    public static Map<String, Object> getAdminConfig() {
        final Map<String, Object> map = configToMap(getCluster().getConfig("admin-properties"));
        addTimeoutConfigs(map);
        map.putIfAbsent(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        return map;
    }

    private static void addTimeoutConfigs(final Map<String, Object> map) {
        map.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, getClientTimeoutMs());
        map.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, getClientTimeoutMs());
    }

    public static String getBootstrapServers() {
        return getCluster().getString("bootstrap-servers");
    }

    public static long getPollIntervalMs() {
        return conf.getDuration(MAIN_OBJECT_NAME + ".poll-interval").toMillis();
    }

    public static int getClientTimeoutMs() {
        return (int) conf.getDuration(MAIN_OBJECT_NAME + ".kafka-client-timeout").toMillis();
    }

    public static Collection<String> getConsumerGroupDenyList() {
        return getStringCollectionOrNull(getCluster(), "group-deny-list");
    }

    public static Collection<String> getConsumerGroupAllowList() {
        return getStringCollectionOrNull(getCluster(), "group-allow-list");
    }

    public static Collection<String> getTopicDenyList() {
        return getStringCollectionOrNull(getCluster(), "topic-deny-list");
    }

    public static Collection<String> getTopicAllowList() {
        return getStringCollectionOrNull(getCluster(), "topic-allow-list");
    }

    private static Collection<String> getStringCollectionOrNull(final Config config, final String name) {
        if (!config.hasPath(name)) {
            return null;
        }
        final List<String> list = new ArrayList<>(config.getStringList(name));
        return list.isEmpty() ? null : list;
    }

    private static Map<String, Object> configToMap(final Config config) {
        final Map<String, Object> map = new HashMap<>();
        for (final Map.Entry<String, ConfigValue> entry : config.entrySet()) {
            map.put(entry.getKey(), entry.getValue().unwrapped());
        }
        return map;
    }

    private static Config getCluster() {
        final List<? extends Config> list = conf.getConfigList(MAIN_OBJECT_NAME + ".clusters");
        if (list == null || list.size() != 1) {
            throw new RuntimeException("Exactly one cluster must be given.");
        }
        return list.get(0);
    }

    public static void setFromFile(final String configFile) {
        conf = ConfigFactory.parseFile(new File(configFile)).withFallback(DEFAULT_CONFIG).resolve();
    }

}

package no.statnett.k3alagexporter;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ConfTest {

    @Test
    public void shouldFindPrometheusPort() {
        assertEquals(8000, Conf.getPrometheusPort());
    }

    @Test
    public void shouldFindClusterName() {
        assertEquals("the-kafka-cluster", Conf.getClusterName());
    }

    @Test
    public void shouldFindConsumerProperties() {
        final Map<String, Object> map = Conf.getConsumerConfig();
        assertNotNull(map);
        assertEquals("SSL", map.get("security.protocol"));
    }

    @Test
    public void shouldFindAdminProperties() {
        final Map<String, Object> map = Conf.getAdminConfig();
        assertNotNull(map);
        assertEquals("password", map.get("ssl.keystore.password"));
    }

    @Test
    public void shouldFindPollInterval() {
        assertEquals(30000L, Conf.getPollIntervalMs());
    }

    @Test
    public void shouldFindBootstrapServers() {
        assertEquals("kafka.example.com:9092", Conf.getBootstrapServers());
    }

    @Test
    public void shouldFindTopicAllowList() {
        final Collection<String> list = Conf.getTopicAllowList();
        assertNotNull(list);
        assertTrue(list.contains("topic1"));
        assertEquals(2, list.size());
    }

}

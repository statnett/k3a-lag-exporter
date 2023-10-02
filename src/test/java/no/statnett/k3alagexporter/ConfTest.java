package no.statnett.k3alagexporter;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

public final class ConfTest {

    @Test
    public void shouldFindPrometheusPort() {
        Assert.assertEquals(8000, Conf.getPrometheusPort());
    }

    @Test
    public void shouldFindClusterName() {
        Assert.assertEquals("the-kafka-cluster", Conf.getClusterName());
    }

    @Test
    public void shouldFindConsumerProperties() {
        final Map<String, Object> map = Conf.getConsumerConfig();
        Assert.assertNotNull(map);
        Assert.assertEquals("SSL", map.get("security.protocol"));
    }

    @Test
    public void shouldFindAdminProperties() {
        final Map<String, Object> map = Conf.getAdminConfig();
        Assert.assertNotNull(map);
        Assert.assertEquals("password", map.get("ssl.keystore.password"));
    }

    @Test
    public void shouldFindPollInterval() {
        Assert.assertEquals(30000L, Conf.getPollIntervalMs());
    }

    @Test
    public void shouldFindBootstrapServers() {
        Assert.assertEquals("kafka.example.com:9092", Conf.getBootstrapServers());
    }

    @Test
    public void shouldFindTopicAllowList() {
        final Collection<String> list = Conf.getTopicAllowList();
        Assert.assertNotNull(list);
        Assert.assertTrue(list.contains("topic1"));
        Assert.assertEquals(2, list.size());
    }

}

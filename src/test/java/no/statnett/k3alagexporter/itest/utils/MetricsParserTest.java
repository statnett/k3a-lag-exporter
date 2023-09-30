package no.statnett.k3alagexporter.itest.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public final class MetricsParserTest {

    @Test
    public void shouldParseMetricsLine() {
        final List<MetricsParser.Metric> metrics = new MetricsParser().getMetrics(
        """
        # HELP k3a_consumergroup_group_lag Group offset lag of a partition
        # TYPE k3a_consumergroup_group_lag gauge
        k3a_consumergroup_group_lag{cluster_name="the-cluster",group="consumer-group",partition="0",topic="the-topic"} 0.0
        # HELP k3a_lag_exporter_poll_time_ms Time (in ms) spent polling all data
        # TYPE k3a_lag_exporter_poll_time_ms gauge
        k3a_lag_exporter_poll_time_ms{cluster_name="the-cluster"} 11.0
        """);
        Assert.assertEquals(2, metrics.size());
        Assert.assertEquals("k3a_consumergroup_group_lag", metrics.get(0).getName());
        Assert.assertEquals(0.0, metrics.get(0).getValue(), 0.000001);
        Assert.assertEquals("the-cluster", metrics.get(0).getLabels().get("cluster_name"));
        Assert.assertEquals("consumer-group", metrics.get(0).getLabels().get("group"));
        Assert.assertEquals("0", metrics.get(0).getLabels().get("partition"));
        Assert.assertEquals("the-topic", metrics.get(0).getLabels().get("topic"));
        Assert.assertEquals("k3a_lag_exporter_poll_time_ms", metrics.get(1).getName());
        Assert.assertEquals(11.0, metrics.get(1).getValue(), 0.000001);
        Assert.assertEquals("the-cluster", metrics.get(1).getLabels().get("cluster_name"));
    }

}

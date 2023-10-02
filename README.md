# k3a-lag-exporter

A very simple lag exporter for Kafka topic partitions, where "lag" is
defined as the difference between the last produced index and the last
consumed index.

This implementation is intended to be run in a K8s-like environment,
where pods are restarted on failure. Hence, any exception during
runtime will make the application terminate.

The lag will be exported as a metric for Prometheus, like this:

```text
k3a_consumergroup_group_lag{cluster_name="the-cluster", group="the-group", partition="0", topic="the-topic"} 0
```

There's also a metric reporting the time spent polling the cluster:

```text
k3a_lag_exporter_poll_time_ms{cluster_name="the-cluster"} 8
```

Example configuration:

```text
k3a-lag-exporter {
    poll-interval = 30 seconds
    reporters.prometheus.port = 8000
    reporters.prometheus.metric-namespace = k3a
    clusters = [
    {
        name = "the-kafka-cluster"
        topic-allow-list = [
            "topic1"
            "topic2"
        ]
        topic-deny-list = [
            ".*secret-topic.*"
        ]
        group-allow-list = [
            "public-group.*"
        ]
        group-deny-list = [
            "internal-group.*"
        ]
        bootstrap-servers = "kafka.example.com:9092"
        consumer-properties = {
            security.protocol = "SASL_SSL"
            sasl.mechanism = "PLAIN"
            sasl.jaas.config = "org.apache.kafka.common.security.plain.PlainLoginModule required username='"${USER}"' password='"${PASSWORD}"';"
        }
        admin-properties = {
            security.protocol = SSL
            ssl.keystore.type = "PKCS12"
            ssl.keystore.location = "./foo.jks"
            ssl.keystore.password = "password"
        }
    }
    ]
}
```

Configuration is handled by
[Typesafe Config](https://github.com/lightbend/config).

| Element                               | Description                                                                                                                           |
|---------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| k3a-lag-exporter                      | The main configuration object.                                                                                                        |
| poll-interval                         | How often to poll the Kafka cluster. Default: 30 seconds                                                                              |
| reporters.prometheus.port             | Port of built-in Prometheus web server. Default: 8000                                                                                 |
| reporters.prometheus.metric-namespace | The prefix for Prometheus metrics. Default: k3a                                                                                       |
| clusters                              | List of clusters to monitor. Currently, only a single cluster is supported.                                                           |
| name                                  | The cluster name. Will be used as a label in the exported metrics.                                                                    |
| topic-allow-list                      | Optional list of topics to include. See below.                                                                                        |
| topic-deny-list                       | Optional list of topics to exclude. See below.                                                                                        |
| group-allow-list                      | Optional list of consumer groups to include. See below.                                                                               |
| group-deny-list                       | Optional list of consumer groups to exclude. See below.                                                                               |
| bootstrap-servers                     | Kafka server(s) to connect to.                                                                                                        |
| consumer-properties                   | Properties allowing connection to the Kafka cluster as a consumer. Must have DESCRIBE permissions for the cluster, groups and topics. |
| admin-properties                      | Properties allowing connection to the Kafka cluster as an admin. Must have DESCRIBE permissions for the cluster, groups and topics.   |

The allow- and deny-lists contain regular expressions that are
implicitly anchored to the beginning and the end of the string.

Filtering happens on the allow-list first, then the deny-list.

* If an allow-list is given, only topics/consumer groups that match an
  entry in the list is passed on.

* Otherwise. of no allow-list is given, every existing topic/consumer
  group is passed on to the deny-list.

* If a deny-list is given, any matching topic/consumer group will be
  removed from the list from the previous steps.

* Otherwise, if no deny-list is given, every topic/consumer group from
  the first to steps will be kept.

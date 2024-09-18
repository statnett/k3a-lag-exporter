package io.statnett.k3a.lagexporter.model;

import org.apache.kafka.common.TopicPartition;

public record ConsumerGroupData(
    TopicPartition topicPartition,
    String consumerGroupId,
    long offset,
    long lag
) {

    public ConsumerGroupData(final ConsumerGroupOffset consumerGroupOffset, final long endOffset) {
        this(
            consumerGroupOffset.topicPartition(),
            consumerGroupOffset.consumerGroupId(),
            consumerGroupOffset.offset(),
            calculateLag(consumerGroupOffset, endOffset)
        );
    }

    private static long calculateLag(final ConsumerGroupOffset consumerGroupOffset, final long endOffset) {
        if (endOffset < 0) {
            return -1;
        } else {
            return Math.max(0, endOffset - consumerGroupOffset.offset());
        }
    }
}

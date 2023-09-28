package no.statnett.k3alagexporter.model;

public final class ConsumerGroupData {

    private final String consumerGroupId;
    private long offset = -1;
    private long lag = -1;

    ConsumerGroupData(final String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(final long offset) {
        this.offset = offset;
    }

    public long getLag() {
        return lag;
    }

    public void setLag(final long lag) {
        this.lag = lag;
    }

}

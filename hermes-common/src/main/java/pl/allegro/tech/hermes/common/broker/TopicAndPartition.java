package pl.allegro.tech.hermes.common.broker;

public class TopicAndPartition {

    private String topic;
    private int partition;

    public TopicAndPartition(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }
}

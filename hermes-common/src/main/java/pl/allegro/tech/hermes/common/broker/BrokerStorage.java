package pl.allegro.tech.hermes.common.broker;

import org.apache.kafka.common.TopicPartition;

import java.util.List;

public interface BrokerStorage {

    int readLeaderForPartition(TopicPartition topicAndPartition);

    BrokerDetails readBrokerDetails(Integer leaderId);

    List<Integer> readPartitionsIds(String topicName);
}

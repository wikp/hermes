package pl.allegro.tech.hermes.common.broker;

import com.google.common.collect.Ordering;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.network.ListenerName;
import pl.allegro.tech.hermes.common.exception.BrokerInfoNotAvailableException;
import pl.allegro.tech.hermes.common.exception.BrokerNotFoundForPartitionException;
import pl.allegro.tech.hermes.common.exception.PartitionsNotFoundForGivenTopicException;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class KafkaBrokerStorage implements BrokerStorage {

    private final AdminClient kafkaAdminClient;
    private final ListenerName brokerListenerName;

    @Inject
    public KafkaBrokerStorage(AdminClient kafkaAdminClient, String brokerListenerName) {
        this.kafkaAdminClient = kafkaAdminClient;
        this.brokerListenerName = ListenerName.normalised(brokerListenerName);
    }

    @Override
    public int readLeaderForPartition(TopicPartition topicAndPartition) {
        try {
            return describeTopic(topicAndPartition.topic())
                    .thenApply(description -> description.partitions().get(topicAndPartition.partition()).leader().id())
                    .get();
        } catch (Exception exception) {
            throw new BrokerNotFoundForPartitionException(topicAndPartition.topic(), topicAndPartition.partition(), exception);
        }
    }

//    @Override
//    public BrokerDetails readBrokerDetails(Integer brokerId) {
//        try {
//            Broker broker = kafkaZkClient.getBroker(brokerId).get();
//            Option<Node> node = broker.getNode(brokerListenerName);
//            String host = node.get().host();
//            int port = node.get().port();
//            return new BrokerDetails(host, port);
//        } catch (Exception exception) {
//            throw new BrokerInfoNotAvailableException(brokerId, exception);
//        }
//    }
//TODO kbublik
    @Override
    public BrokerDetails readBrokerDetails(Integer brokerId) {
        try {
            Optional<Node> node = kafkaAdminClient.describeCluster().nodes()
                    .thenApply(nodes -> nodes.stream().filter(it -> it.id() == brokerId).findFirst()).get();
            String host = node.get().host();
            int port = node.get().port();
            return new BrokerDetails(host, port);
        } catch (Exception exception) {
            throw new BrokerInfoNotAvailableException(brokerId, exception);
        }
    }

    @Override
    public List<Integer> readPartitionsIds(String topicName) {
        try {
            List<Integer> partitions = describeTopic(topicName)
                    .thenApply(this::resolvePartitionIds)
                    .get();

            return Ordering.natural().sortedCopy(partitions);
        } catch (Exception exception) {
            throw new PartitionsNotFoundForGivenTopicException(topicName, exception);
        }
    }

    private KafkaFuture<TopicDescription> describeTopic(String topic) {
        return kafkaAdminClient.describeTopics(Collections.singletonList(topic)).all()
                .thenApply(topicsMap -> topicsMap.get(topic));
    }

    private List<Integer> resolvePartitionIds(TopicDescription topicDescription) {
        return topicDescription.partitions().stream()
                .map(TopicPartitionInfo::partition)
                .collect(Collectors.toList());
    }
}

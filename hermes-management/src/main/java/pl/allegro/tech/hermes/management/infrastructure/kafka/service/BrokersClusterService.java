package pl.allegro.tech.hermes.management.infrastructure.kafka.service;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.common.kafka.KafkaNamesMapper;
import pl.allegro.tech.hermes.common.kafka.offset.PartitionOffset;
import pl.allegro.tech.hermes.management.domain.message.RetransmissionService;
import pl.allegro.tech.hermes.management.domain.topic.BrokerTopicManagement;
import pl.allegro.tech.hermes.management.domain.topic.SingleMessageReader;

import java.util.List;
import java.util.function.Consumer;

public class BrokersClusterService {

    private final String clusterName;
    private final SingleMessageReader singleMessageReader;
    private final RetransmissionService retransmissionService;
    private final BrokerTopicManagement brokerTopicManagement;
    private final KafkaNamesMapper kafkaNamesMapper;
    private final OffsetsAvailableChecker offsetsAvailableChecker;
    private final AdminClient adminClient;

    public BrokersClusterService(String clusterName, SingleMessageReader singleMessageReader,
                                 RetransmissionService retransmissionService, BrokerTopicManagement brokerTopicManagement,
                                 KafkaNamesMapper kafkaNamesMapper, OffsetsAvailableChecker offsetsAvailableChecker,
                                 AdminClient adminClient) {

        this.clusterName = clusterName;
        this.singleMessageReader = singleMessageReader;
        this.retransmissionService = retransmissionService;
        this.brokerTopicManagement = brokerTopicManagement;
        this.kafkaNamesMapper = kafkaNamesMapper;
        this.offsetsAvailableChecker = offsetsAvailableChecker;
        this.adminClient = adminClient;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void manageTopic(Consumer<BrokerTopicManagement> manageFunction) {
        manageFunction.accept(brokerTopicManagement);
    }

    public String readMessageFromPrimary(Topic topic, Integer partition, Long offset) {
        return singleMessageReader.readMessageAsJson(topic, kafkaNamesMapper.toKafkaTopics(topic).getPrimary(), partition, offset);
    }

    public List<PartitionOffset> indicateOffsetChange(Topic topic, String subscriptionName, Long timestamp, boolean dryRun) {
        return retransmissionService.indicateOffsetChange(topic, subscriptionName, clusterName, timestamp, dryRun);
    }

    public boolean areOffsetsAvailableOnAllKafkaTopics(Topic topic) {
        return kafkaNamesMapper.toKafkaTopics(topic).allMatch(offsetsAvailableChecker::areOffsetsAvailable);
    }

    public boolean topicExists(Topic topic) {
        return brokerTopicManagement.topicExists(topic);
    }

    public boolean areOffsetsMoved(Topic topic, String subscriptionName) {
        return retransmissionService.areOffsetsMoved(topic, subscriptionName, clusterName);
    }

    public boolean allSubscriptionsHaveConsumersAssigned(Topic topic, List<String> subscriptionsNames) {
        return subscriptionsNames.stream().allMatch(name -> subscriptionHasConsumersAlreadyAssigned(topic, name));
    }

    private boolean subscriptionHasConsumersAlreadyAssigned(Topic topic, String subscriptionName) {
        String consumerGroupId = kafkaNamesMapper.toConsumerGroupId(new SubscriptionName(subscriptionName, topic.getName())).asString();

        try {
            return numberOfAssignmentsForConsumerGroup(consumerGroupId) == 4;
        } catch (Exception e) {
            // log exception
            return false;
        }
    }

    private int numberOfAssignmentsForConsumerGroup(String consumerGroupId) throws ExecutionException, InterruptedException {
        Collection<ConsumerGroupDescription> consumerGroupsDescriptions = adminClient.describeConsumerGroups(Collections.singletonList(consumerGroupId)).all().get().values();
        Collection<MemberDescription> memberDescriptions = consumerGroupsDescriptions.stream().flatMap(desc -> desc.members().stream()).collect(Collectors.toList());
        return memberDescriptions.stream().flatMap(memberDescription -> memberDescription.assignment().topicPartitions().stream()).collect(Collectors.toList()).size();
    }
}

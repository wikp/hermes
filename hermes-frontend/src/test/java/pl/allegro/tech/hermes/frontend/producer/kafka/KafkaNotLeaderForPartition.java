package pl.allegro.tech.hermes.frontend.producer.kafka;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class KafkaNotLeaderForPartition {

    @Test
    public void notLeaderForPartitionException() throws Exception {
        // given
        String topicName = "test.topic";
        List<String> brokers = new ArrayList<>();
        brokers.add("localhost:5005");
        brokers.add("localhost:5006");
        brokers.add("localhost:5007");
        KafkaServer kafkaServer = new KafkaServer(5005, 5006, 5007);
        kafkaServer.start();

        kafkaServer.createTopic(topicName, 10240);

        assert kafkaServer.isTopicPresent(topicName);
        CustomKafkaProducer producer = new CustomKafkaProducer(brokers);

        // when
        for (int i = 0; i < 10; i++) {
            producer.sendSampleMessageWithSize(topicName, 70);
        }

        producer.waitForAllMessages();

        kafkaServer.deleteTopic(topicName);
        assert !kafkaServer.isTopicPresent(topicName);

        Thread.sleep(2000); //waiting for topic to be deleted

        kafkaServer.createTopic(topicName, 10240);

        for (int i = 0; i < 1; i++) {
            producer.sendSampleMessageWithSize(topicName, 70);
        }



    }
}

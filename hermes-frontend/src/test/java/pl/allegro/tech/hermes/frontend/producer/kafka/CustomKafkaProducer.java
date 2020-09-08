package pl.allegro.tech.hermes.frontend.producer.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.METADATA_MAX_AGE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.SEND_BUFFER_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

class CustomKafkaProducer {
    private static final String ACK_LEADER = "1";

    private final KafkaProducer<byte[], byte[]> kafkaProducer;

    private final List<Future<RecordMetadata>> messagesToSend = new ArrayList<>();

    CustomKafkaProducer(List<String> kafkaBrokerList) {
        this.kafkaProducer = new KafkaProducer<>(prepareKafkaProducerProperties(kafkaBrokerList));
    }

    void sendSampleMessageWithSize(String topicName, int messageSize) {
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topicName, new byte[messageSize]);
        messagesToSend.add(kafkaProducer.send(producerRecord));
    }

    void waitForAllMessages() {
        for (Future<RecordMetadata> future : messagesToSend) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static Map<String, Object> prepareKafkaProducerProperties(List<String> kafkaBrokerList) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerList);
        properties.put(MAX_BLOCK_MS_CONFIG, 500);
        properties.put(COMPRESSION_TYPE_CONFIG, "none");
        properties.put(BUFFER_MEMORY_CONFIG, 2415917867L);
        properties.put(REQUEST_TIMEOUT_MS_CONFIG, 30 * 60 * 1000);
        properties.put(BATCH_SIZE_CONFIG, 16 * 1024);
        properties.put(SEND_BUFFER_CONFIG, 128 * 1024);
        properties.put(RETRIES_CONFIG, Integer.MAX_VALUE);
        properties.put(RETRY_BACKOFF_MS_CONFIG, 256);
        properties.put(METADATA_MAX_AGE_CONFIG, 5 * 60 * 1000);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(MAX_REQUEST_SIZE_CONFIG, 1433600);
        properties.put(LINGER_MS_CONFIG, 0);
        properties.put(METRICS_SAMPLE_WINDOW_MS_CONFIG, 30000);
        properties.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ACKS_CONFIG, ACK_LEADER);
        return properties;
    }
}

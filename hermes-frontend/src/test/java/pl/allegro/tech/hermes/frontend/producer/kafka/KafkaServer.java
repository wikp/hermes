package pl.allegro.tech.hermes.frontend.producer.kafka;

import com.google.common.io.Files;
import kafka.log.LogConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.common.utils.Time;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

class KafkaServer {
    static final int PARTITION_COUNT = 10;

    private static final int ZOOKEEPER_PORT = 2181;
    private static final String ZOOKEEPER_CONNECT_STRING = "localhost:" + ZOOKEEPER_PORT;

    private final TestingServer zookeeperServer;
    private final KafkaServerStartable kafkaServerStartable;
    private final KafkaServerStartable kafkaServerStartable2;
    private final KafkaServerStartable kafkaServerStartable3;

    private ZooKeeperClient zooKeeperClient;
    private KafkaZkClient kafkaZkClient;
    private AdminZkClient adminZkClient;

    KafkaServer(int port, int port2, int port3) throws Exception {
        zookeeperServer = new TestingServer(zkConfig(ZOOKEEPER_PORT), false);
        kafkaServerStartable = new KafkaServerStartable(prepareKafkaConfig(port, 0));
        kafkaServerStartable2 = new KafkaServerStartable(prepareKafkaConfig(port2, 1));
        kafkaServerStartable3 = new KafkaServerStartable(prepareKafkaConfig(port3, 2));
    }

    void start() throws Exception {
        zookeeperServer.start();
        kafkaServerStartable.startup();
        kafkaServerStartable2.startup();
        kafkaServerStartable3.startup();
        waitForStartup(kafkaServerStartable.staticServerConfig().port());
        waitForStartup(kafkaServerStartable2.staticServerConfig().port());
        waitForStartup(kafkaServerStartable3.staticServerConfig().port());

        zooKeeperClient = zooKeeperClient(ZOOKEEPER_CONNECT_STRING);
        kafkaZkClient = new KafkaZkClient(zooKeeperClient, false, Time.SYSTEM);
        adminZkClient = new AdminZkClient(kafkaZkClient);
    }

    void createTopic(String name, int maxMessageSize) {
        Properties config = createTopicConfig(maxMessageSize);
        adminZkClient.createTopic(name, PARTITION_COUNT, 3, config, kafka.admin.RackAwareMode.Enforced$.MODULE$);
    }

    void deleteTopic(String name) {
        adminZkClient.deleteTopic(name);
    }

    boolean isTopicPresent(String name) {
        return kafkaZkClient.topicExists(name) && !kafkaZkClient.isTopicMarkedForDeletion(name);
    }

    void stop() throws IOException {
        kafkaServerStartable.shutdown();
        kafkaServerStartable.awaitShutdown();
        zookeeperServer.stop();
    }

    private ZooKeeperClient zooKeeperClient(String zkConnect) {
        return new ZooKeeperClient(
                zkConnect,
                10000,
                10000,
                10,
                Time.SYSTEM,
                "zookeeper-metrics-group",
                "zookeeper"
        );
    }

    private static KafkaConfig prepareKafkaConfig(int port, int brokerId) {
        Properties properties = new Properties();
        properties.setProperty("port", String.valueOf(port));
        properties.setProperty("zookeeper.connect", ZOOKEEPER_CONNECT_STRING);
        properties.setProperty("broker.id", String.valueOf(brokerId));
        properties.setProperty("log.dirs", Files.createTempDir().getAbsolutePath());
        properties.setProperty("delete.topic.enable", "true");
        properties.setProperty("offsets.topic.replication.factor", "3");
        properties.setProperty("group.initial.rebalance.delay.ms", "0");
        return new KafkaConfig(properties);
    }

    private Properties createTopicConfig(int maxMessageSize) {
        Properties props = new Properties();
        props.put(LogConfig.RetentionMsProp(), String.valueOf(TimeUnit.DAYS.toMillis(1)));
        props.put(LogConfig.UncleanLeaderElectionEnableProp(), Boolean.toString(false));
        props.put(LogConfig.MaxMessageBytesProp(), String.valueOf(maxMessageSize));
        return props;
    }

    private void waitForStartup(int port) {
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", port);
        while (true) {
            try {
                Socket socket = new Socket();
                socket.connect(inetSocketAddress, 1000);
                if (socket.isConnected()) {
                    break;
                }
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private InstanceSpec zkConfig(int port) {
        File dataDirectory = Files.createTempDir();
        int electionPort = -1; //negative value means use default value
        int quorumPort = -1;
        boolean deleteDataDirectoryOnClose = true;
        int serverId = -1;
        int tickTime = -1;
        int maxClientConnections = 1000;

        return new InstanceSpec(
                dataDirectory,
                port,
                electionPort,
                quorumPort,
                deleteDataDirectoryOnClose,
                serverId,
                tickTime,
                maxClientConnections
        );
    }
}

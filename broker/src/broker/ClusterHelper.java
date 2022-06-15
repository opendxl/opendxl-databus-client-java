/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package broker;

import kafka.admin.TopicCommand;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.utils.SystemTime;
import scala.runtime.AbstractFunction0;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClusterHelper {

    private static ClusterHelper clusterHelper;
    private static boolean isStarted = false;
    private static int zookeeperPort = 2181;
    private static Zookeeper zkNode;
    private static List<KafkaBroker> brokers = new ArrayList<>();
    private static final String ZKHOST = "localhost";
    private static final int SESSION_TIMEOUT_MS = 30000;
    private static final int CONNECTION_TIMEOUT_MS = 30000;
    private static final int MAX_IN_FLIGHT_REQUESTS = 1000;
    private static final String METRIC_GROUP = "kafka.server";
    private static final String METRIC_TYPE = "SessionExpireListener";

    public static ClusterHelper getInstance() {
        if (clusterHelper == null) {
            clusterHelper = new ClusterHelper();
        }
        return clusterHelper;
    }

    public void addNewKafkaTopic(final String topicName, final int replicationFactor, final
    int partitions) throws Exception {
        String[] arguments = {
            "--create",
            "--zookeeper", ZKHOST.concat(":").concat(String.valueOf(zookeeperPort)),
            "--replication-factor", String.valueOf(replicationFactor),
            "--partitions", String.valueOf(partitions),
            "--topic", topicName
        };

        TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);
        try (KafkaZkClient zkUtils = getZkClient(opts)) {
            new TopicCommand.ZookeeperTopicService(zkUtils).createTopic(opts);
        } catch (Exception e) {
            // In case of exceptions, abort topic creation.
            throw new Exception("Error creating a new Kafka topic");
        }
    }

    public ClusterHelper addBroker(final int port) {
        checkCluster();
        Properties config = getConfig(port);
        final KafkaBroker broker = new KafkaBroker(config);
        brokers.add(broker);
        return clusterHelper;
    }

    public ClusterHelper addBroker(final Properties config) {
        checkCluster();
        final KafkaBroker broker = new KafkaBroker(config);
        brokers.add(broker);
        return clusterHelper;
    }

    public ClusterHelper zookeeperPort(final int zkPort) {
        checkCluster();
        ClusterHelper.zookeeperPort = zkPort;
        for (KafkaBroker broker : brokers) {
            broker.getBrokerConfig().setProperty("zookeeper.connect",
                    "localhost:".concat(String.valueOf(ClusterHelper.zookeeperPort)));
        }
        return clusterHelper;
    }

    public Collection<Node> start() {
        if (clusterHelper != null && !isStarted) {
            ClusterHelper.zkNode = new Zookeeper(zookeeperPort);
            zkNode.startup();
            for (final KafkaBroker broker : brokers) {
                broker.start();
            }
            isStarted = true;
            return describe();
        }
        return null;
    }

    public void stop() {
        for (final KafkaBroker broker : brokers) {
            broker.shutdown();
        }
        zkNode.shutdown();
        brokers = new ArrayList<>();
        isStarted = false;
        clusterHelper = null;
    }

    private static void checkCluster() {
        if (clusterHelper == null || isStarted) {
            throw new IllegalStateException("Cannot perform this operation when the cluster has not been created "
                    + "or it is runnig");
        }
    }

    private Properties getConfig(final int port) {
        final Properties config =  new Properties();
        try {
            File logFile = Files.createTempDirectory(Constant.LOG_PATH_PREFIX + System.currentTimeMillis()).toFile();
            config.setProperty("zookeeper.connect", "localhost:".concat(String.valueOf(zookeeperPort)));
            config.setProperty("broker.id", String.valueOf(brokers.size() + 1));
            config.setProperty("host.name", "localhost");
            config.setProperty("port", Integer.toString(port));
            config.setProperty("log.dir", logFile.getAbsolutePath());
            config.setProperty("log.flush.interval.messages", String.valueOf(1));
            config.setProperty("delete.topic.enable", String.valueOf(true));
            config.setProperty("offsets.topic.replication.factor", String.valueOf(1));
            config.setProperty("num.partitions", String.valueOf(6));
            config.setProperty("transaction.state.log.replication.factor", String.valueOf(1));
            config.setProperty("transaction.state.log.min.isr", String.valueOf(1));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return config;

    }

    private Collection<Node> describe()  {
        Map props = new HashMap<>();

        final Properties brokerConfig = brokers.get(0).getBrokerConfig();
        final String bootstrapServer = brokerConfig.getProperty("host.name")
                .concat(":")
                .concat(brokerConfig.getProperty("port"));

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        AdminClient adminClient = AdminClient.create(props);
        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        Collection<Node> nodes = new ArrayList<>();
        try {
            return describeClusterResult.nodes().get(2500, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return nodes;
    }

    private KafkaZkClient getZkClient(TopicCommand.TopicCommandOptions opts) {
        final String connectString = opts.zkConnect().getOrElse(new AbstractFunction0<String>() {
            @Override
            public String apply() {
                return "";
            } });

        return KafkaZkClient.apply(connectString,
                JaasUtils.isZkSaslEnabled(),
                SESSION_TIMEOUT_MS,
                CONNECTION_TIMEOUT_MS,
                MAX_IN_FLIGHT_REQUESTS,
                new SystemTime(),
                METRIC_GROUP,
                METRIC_TYPE, null);
    }
}

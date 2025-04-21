/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.util;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import broker.KafkaBroker;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Topic {
    org.apache.zookeeper.client.ZKClientConfig zkClientConfig = new org.apache.zookeeper.client.ZKClientConfig();
     public static String getRandomTopicName() {
         return UUID.randomUUID().toString();
    }

    public static class Builder {
        private String topicName = getRandomTopicName();
        private int partitions = 1;
        private short replicationFactor = 1;
        private String kafkaHost = Constants.KAFKA_HOST;
        private String KafkaPort = Constants.KAFKA_PORT;
        private static List<KafkaBroker> brokers = new ArrayList<>();

        public Builder partitions(final int partitions) {
            this.partitions = partitions;
            return this;
        }

        public Builder kafkaHost(final String kafkaHost) {
            this.kafkaHost = kafkaHost;
            return this;
        }

        public Builder kafkaPort(final String KafkaPort) {
            this.KafkaPort = KafkaPort;
            return this;
        }

        public Builder topicName(final String topicName) {
            this.topicName = topicName;
            return this;
        }

        public Builder replicationFactor(final short replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        public String go() {
            AdminClient adminClient = null;
        try {
            // Create an AdminClient instance
            adminClient = createAdminClient();

            // Define the new topic with specified configurations
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

            // Create the topic
            adminClient.createTopics(Collections.singleton(newTopic));//.all().get();

            System.out.println("Topic created successfully.");
        }
        catch ( Exception e) {
            System.err.println("Error creating the Kafka topic: " + e.getMessage());
            throw new RuntimeException("Error creating a new Kafka topic", e);
        } finally {
            if (adminClient != null) {
                adminClient.close();
                }
            }
            return topicName;
        }
        public static AdminClient createAdminClient() {
            final Map<String, Object> props = new HashMap<>();
            final Properties brokerConfig = brokers.get(0).getBrokerConfig();
            final String bootstrapServer = brokerConfig.getProperty("host.name")
                    .concat(":")
                    .concat(brokerConfig.getProperty("port"));
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            return AdminClient.create(props);
        }
        private KafkaZkClient getZkClient(AdminClient adminClient) {
            final String connectString = "";

            return KafkaZkClient.apply(connectString,
                    JaasUtils.isZkSaslEnabled(),
                    30000,
                    30000,
                    1000,
                    new SystemTime(),
                    "", new org.apache.zookeeper.client.ZKClientConfig(),
                    "kafka.server",
                    "SessionExpireListener", false,false);
        }
    }
}

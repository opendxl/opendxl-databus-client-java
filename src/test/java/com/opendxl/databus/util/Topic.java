/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.util;

import kafka.admin.TopicCommand;
import kafka.admin.TopicCommand.TopicService;
import kafka.zk.KafkaZkClient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.utils.SystemTime;
import java.util.UUID;

public class Topic {
    public static String getRandomTopicName() {
        return UUID.randomUUID().toString();
    }

    public static class Builder {
        private String topicName = getRandomTopicName();
        private int partitions = 1;
        private int replicationFactor = 1;
        private String kafkaHost = Constants.KAFKA_HOST;
        private String KafkaPort = Constants.KAFKA_PORT;

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

        public Builder replicationFactor(final int replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        public String go() {
            String[] arguments = {"--create",
                    "--bootstrap-server",
                    kafkaHost.concat(":").concat(KafkaPort),
                    "--replication-factor",
                    String.valueOf(replicationFactor),
                    "--partitions",
                    String.valueOf(partitions),
                    "--topic",
                    topicName};

            TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);
            TopicService topicService=null;
            try {
                final AdminClient adminClient = AdminClientHelper.createAdminClient();
                topicService = new TopicCommand.TopicService(adminClient);
                topicService.createTopic(opts);
            }
            finally{
                if (topicService!=null) {
                    topicService.close();
                }
            }
            return topicName;
        }

        private KafkaZkClient getZkClient(TopicCommand.TopicCommandOptions opts) {
            final String connectString = "";

            return KafkaZkClient.apply(connectString,
                    JaasUtils.isZkSaslEnabled(),
                    30000,
                    30000,
                    1000,
                    new SystemTime(),
                    "", null,
                    "kafka.server",
                    "SessionExpireListener", false);
        }
    }
    
}

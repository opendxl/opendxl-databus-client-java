/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.util;

import kafka.admin.TopicCommand;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.utils.SystemTime;
import scala.runtime.AbstractFunction0;

import java.util.UUID;

public class Topic {
    public static String getRandomTopicName() {
        return UUID.randomUUID().toString();
    }

    public static class Builder {
        private String topicName = getRandomTopicName();
        private int partitions = 1;
        private int replicationFactor = 1;
        private String zkHost = Constants.ZOOKEEPER_HOST;
        private String zkPort = Constants.ZOOKEEPER_PORT;

        public Builder partitions(final int partitions) {
            this.partitions = partitions;
            return this;
        }

        public Builder zkHost(final String zkHost) {
            this.zkHost = zkHost;
            return this;
        }

        public Builder zkPort(final String zkPort) {
            this.zkPort = zkPort;
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
                    "--zookeeper",
                    zkHost.concat(":").concat(zkPort),
                    "--replication-factor",
                    String.valueOf(replicationFactor),
                    "--partitions",
                    String.valueOf(partitions),
                    "--topic",
                    topicName};

            TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);
            try (KafkaZkClient zkUtils = getZkClient(opts)) {
                new TopicCommand.ZookeeperTopicService(zkUtils).createTopic(opts);
            }
            return topicName;


        }

        private KafkaZkClient getZkClient(TopicCommand.TopicCommandOptions opts) {
            final String connectString = opts.zkConnect().getOrElse(new AbstractFunction0<String>() {
                @Override
                public String apply() {
                    return "";
                } });

            return KafkaZkClient.apply(connectString,
                    JaasUtils.isZkSecurityEnabled(),
                    30000,
                    30000,
                    1000,
                    new SystemTime(),
                    "kafka.server",
                    "SessionExpireListener", null);
        }
    }
}

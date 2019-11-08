/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package broker;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

public class KafkaBroker {


    private Properties brokerConfig;

    private Zookeeper zookeeper;
    private KafkaServerStartable broker;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaBroker.class);


    public KafkaBroker(final Properties brokerConfig) {


        this.brokerConfig = brokerConfig;
    }

    public void start() {

        Runtime.getRuntime().addShutdownHook(
                new Thread(
                        new Runnable() {
                            public void run() {
                                getDeleteLogDirectoryAction();
                                shutdown();
                            }
                        }));


        broker = new KafkaServerStartable(new KafkaConfig(brokerConfig));
        broker.startup();
        LOG.info("Kafka broker started: " + brokerConfig.getProperty("host.name")
                .concat(":")
                .concat(brokerConfig.getProperty("port")));
    }

    private Runnable getDeleteLogDirectoryAction() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    if (Files.createTempDirectory(Constant.LOG_PATH_PREFIX).toFile() != null) {
                        try {
                            FileUtils.deleteDirectory(Files.createTempDirectory(Constant.LOG_PATH_PREFIX).toFile());
                        } catch (IOException e) {
                            LOG.warn("Problems deleting kafka temporary directory ", e);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    public synchronized void shutdown() {
        if (broker != null) {
            broker.shutdown();
            broker.awaitShutdown();
            LOG.info("Kafka broker stopped: " + brokerConfig.getProperty("host.name")
                    .concat(":")
                    .concat(brokerConfig.getProperty("port")));
            broker = null;
        }
    }

    public Properties getBrokerConfig() {
        return brokerConfig;
    }

}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package broker;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import scala.Option;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

public class KafkaBroker {

    private Properties brokerConfig;
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBroker.class);
    private KafkaServer kafkaServer;
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

                        try {
                            final KafkaConfig kafkaConfig = new KafkaConfig(brokerConfig);
                            kafkaServer = new KafkaServer(kafkaConfig, new SystemTime(), 
                                Option.apply(this.getClass().getName()), true);
                            kafkaServer.startup();
                
                            LOG.info("Kafka broker started: " + brokerConfig.getProperty("host.name")
                                    .concat(":")
                                    .concat(brokerConfig.getProperty("port")));
        
                            } catch (Exception e) {
                                System.out.println(e.getMessage());
                            }
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
        if(kafkaServer != null){
            kafkaServer.shutdown();
            kafkaServer.awaitShutdown();
            LOG.info("Kafka broker stopped: " + brokerConfig.getProperty("host.name")
            .concat(":")
            .concat(brokerConfig.getProperty("port")));
            kafkaServer = null;
        }
    }

    public Properties getBrokerConfig() {
        return brokerConfig;
    }
}

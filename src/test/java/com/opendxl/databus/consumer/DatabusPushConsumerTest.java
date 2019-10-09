package com.opendxl.databus.consumer;

import broker.ClusterHelper;
import com.opendxl.databus.serialization.ByteArrayDeserializer;
import com.opendxl.databus.util.Constants;
import com.opendxl.databus.util.ConsumerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class DatabusPushConsumerTest {


    private ConsumerHelper consumerHelper;

    @BeforeClass
    public static void beforeClass() throws IOException {
        ClusterHelper.getInstance()
                .addBroker(Integer.valueOf(Constants.KAFKA_PORT))
                .zookeeperPort(Integer.valueOf(Constants.ZOOKEEPER_PORT))
                .start();
    }

    @AfterClass
    public static void afterClass() {
        ClusterHelper.getInstance().stop();
    }


    @Test
    public void shouldCloseAutomatically() {

        Properties config = new Properties();
        config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfiguration.GROUP_ID_CONFIG, "consumer-group-1");
        config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");

        DatabusPushConsumerListener consumerListener = new ConsumerListener();
        try(DatabusPushConsumer consumer = new DatabusPushConsumer(config, new ByteArrayDeserializer(), consumerListener)) {
            consumer.subscribe(Arrays.asList("topic1"));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    class ConsumerListener implements DatabusPushConsumerListener {

        @Override
        public DatabusPushConsumerListenerResponse onConsume(ConsumerRecords records) {

            return DatabusPushConsumerListenerResponse.CONTINUE;

        }
    }
}

package com.opendxl.databus.consumer;

import broker.ClusterHelper;
import com.opendxl.databus.util.Constants;
import com.opendxl.databus.util.ConsumerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.*;

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

        try(DatabusPushConsumer consumer = new DatabusPushConsumer()) {

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}

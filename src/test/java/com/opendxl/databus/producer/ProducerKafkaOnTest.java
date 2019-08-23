/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer;

import broker.ClusterHelper;
import com.opendxl.databus.util.Constants;
import com.opendxl.databus.util.ProducerHelper;
import com.opendxl.databus.util.TestCallback;
import junit.framework.AssertionFailedError;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;

public class ProducerKafkaOnTest {

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
    public void produceMessageSuccessTest() {
        try {
            final ProducerHelper producerHelper = new ProducerHelper();
            final Producer<byte[]> producer = producerHelper.getProducer();
            final String producerTopic = "topic1test";
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            final ProducerRecord<byte[]> producerRecord = producerHelper.getRecord(producerTopic,
                    String.valueOf(System.currentTimeMillis()));

            TestCallback testCallback = new TestCallback(countDownLatch);

            // Send the record
            producer.send(producerRecord, testCallback);

            countDownLatch.await();
            // Callbacks success
            Assert.assertTrue(testCallback.isSuccess());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}

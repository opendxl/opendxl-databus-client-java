/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer;

import broker.ClusterHelper;
import com.opendxl.databus.util.Constants;
import com.opendxl.databus.util.ProducerHelper;
import com.opendxl.databus.util.TestCallback;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

    @Test
    public void produceMessageSuccessDefaultCallbackTest() {
        try {
            final ProducerHelper producerHelper = new ProducerHelper();
            final Producer<byte[]> producer = producerHelper.getProducer();
            final String producerTopic = "topic1test";
            final ProducerRecord<byte[]> producerRecord = producerHelper.getRecord(producerTopic,
                    String.valueOf(System.currentTimeMillis()));

            // Send the record
            producer.send(producerRecord);

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void getPartitionsForTest() {
        try {
            final ProducerHelper producerHelper = new ProducerHelper();
            final Producer<byte[]> producer = producerHelper.getProducer();
            final String producerTopic = "topic1test";
            final ProducerRecord<byte[]> producerRecord = producerHelper.getRecord(producerTopic,
                    String.valueOf(System.currentTimeMillis()));

            // Send the record
            producer.send(producerRecord);

            Assert.assertNotNull(producer.partitionsFor(producerTopic));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void closeSentWithTimeOutTest() {
        try {
            final long timeout = 2;
            final ProducerHelper producerHelper = new ProducerHelper();
            final Producer<byte[]> producer = producerHelper.getProducer();
            final String producerTopic = "topic1test";
            final ProducerRecord<byte[]> producerRecord = producerHelper.getRecord(producerTopic,
                    String.valueOf(System.currentTimeMillis()));

            // Send the record
            producer.send(producerRecord);

            producer.close(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void producerFlushSuccessTest(){
        try {
        final ProducerHelper producerHelper = new ProducerHelper();
        final Producer<byte[]> producer = producerHelper.getProducer();

        producer.flush();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void producerGetMetrics(){
        try {
            final ProducerHelper producerHelper = new ProducerHelper();
            final Producer<byte[]> producer = producerHelper.getProducer();
            String producerTopic = "topic1";


            Assert.assertNotNull(producer.recordBatchSizeAvgMetric());
            Assert.assertNotNull(producer.recordBatchSizeMaxMetric());
            Assert.assertNotNull(producer.recordErrorRateMetric());
            Assert.assertNotNull(producer.recordErrorTotalMetric());
            Assert.assertNotNull(producer.recordSendRateMetric());
            Assert.assertNotNull(producer.recordSendTotalMetric());
            Assert.assertNotNull(producer.recordSizeAvgMetric());
            Assert.assertNotNull(producer.recordSizeMaxMetric());
            Assert.assertNotNull(producer.recordSendRatePerTopicMetric(producerTopic));
            Assert.assertNotNull(producer.recordSendTotalPerTopicMetric(producerTopic));
            Assert.assertNotNull(producer.recordByteRatePerTopicMetric(producerTopic));
            Assert.assertNotNull(producer.recordByteTotalPerTopicMetric(producerTopic));
            Assert.assertNotNull(producer.recordErrorRatePerTopicMetric(producerTopic));
            Assert.assertNotNull(producer.recordErrorTotalPerTopicMetric(producerTopic));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


}

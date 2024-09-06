/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer;

import com.opendxl.databus.util.ProducerHelper;
import com.opendxl.databus.util.TestCallback;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

@Ignore
public class ProducerKafkaOffTest {

    @Test
    public void produceMessageFailTest(){
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
        // Callbacks fails with exception
        Assert.assertFalse((testCallback.isSuccess()));
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }
    }
}

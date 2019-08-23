/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.util;

import com.opendxl.databus.common.RecordMetadata;
import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.entities.RoutingData;
import com.opendxl.databus.producer.Callback;
import com.opendxl.databus.producer.DatabusProducer;
import com.opendxl.databus.producer.Producer;
import com.opendxl.databus.producer.ProducerConfig;
import com.opendxl.databus.producer.ProducerRecord;
import com.opendxl.databus.serialization.ByteArraySerializer;

import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ProducerHelper {
    private int numOfRecords = 1;
    private Producer<byte[]> producer;
    private String topicName = Topic.getRandomTopicName();
    private CountDownLatch countDownLatch;
    List<ProducerRecord<byte[]>> recordsProduced = new ArrayList<>();
    private Map config = new HashMap<String, Object>();

    public ProducerHelper() {
        config.put("bootstrap.servers", Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT));
        config.put("client.id", "test-producer");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, "150000");
    }

    public ProducerHelper config(final Map<String, Object> config) {
        for (Map.Entry<String, Object> conf : config.entrySet()) {
            config.put(conf.getKey(),conf.getValue());
        }
        return this;
    }

    public ProducerHelper numberOfRecords(final int numOfRecords) {
        this.numOfRecords = numOfRecords;
        return this;
    }

    public ProducerHelper topic(final String topicName) {
        this.topicName = topicName;
        return this;
    }

    public ProducerHelper produce() throws InterruptedException {
        producer = getProducer();
        countDownLatch = new CountDownLatch(numOfRecords);
        for(int i = 0 ; i < numOfRecords ; i++) {
            final ProducerRecord<byte[]> record = getRecord(topicName, String.valueOf(i));
            producer.send(record, new ProducerCallback(record));
        }
        countDownLatch.await();
        producer.flush();
        producer.close();
        return this;
    }

    public ProducerRecord<byte[]>  getRecord(final String topicName, final String shardingKey) {
        final RoutingData routingData = new RoutingData(topicName, shardingKey,null);
        final String message = "Hello World at:" + LocalDateTime.now();
        final byte[] payload = message.getBytes(Charset.defaultCharset());
        MessagePayload<byte[]> messagePayload = new MessagePayload<>(payload);
        return new ProducerRecord<> (routingData, new Headers(), messagePayload);
    }

    public Producer<byte[]> getProducer() {
        try {
            return new DatabusProducer<>(config, new ByteArraySerializer());
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public Map<String, ProducerRecord<byte[]>> asMap() {
        Map<String, ProducerRecord<byte[]>> map = new HashMap<>();
        for(ProducerRecord<byte[]> record : recordsProduced) {
            map.put(record.getRoutingData().getShardingKey(), record);
        }
        return map;
    }

    private class ProducerCallback implements Callback {

        private final ProducerRecord<byte[]> record;

        public ProducerCallback(ProducerRecord<byte[]> record) {
            this.record = record;
        }

        @Override
        public void onCompletion(final RecordMetadata metadata, final Exception exception) {
            if(exception == null) {
                recordsProduced.add(record) ;
            }
            countDownLatch.countDown();
        }
    }

    public static ProducerHelper produceTo(final String topicName) {
        ProducerHelper produceHelper = new ProducerHelper();
        produceHelper.topic(topicName);
        return produceHelper;
    }
}

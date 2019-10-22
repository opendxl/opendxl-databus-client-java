/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.util;

import com.opendxl.databus.common.TopicPartition;
import com.opendxl.databus.serialization.ByteArrayDeserializer;
import com.opendxl.databus.consumer.Consumer;
import com.opendxl.databus.consumer.ConsumerConfiguration;
import com.opendxl.databus.consumer.ConsumerRecord;
import com.opendxl.databus.consumer.ConsumerRecords;
import com.opendxl.databus.consumer.DatabusConsumer;
import com.opendxl.databus.consumer.OffsetAndMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ConsumerHelper {
    private Consumer<byte[]> consumer;
    private int numberOfRecords = 1;
    List<ConsumerRecord<byte[]>> recordsConsumed;
    private List<String> topics;
    final Properties config = new Properties();
    private TopicPartition topicPartition = null;
    private long offset = -1;
    private ConsumerRecords<byte[]> records;
    private int maxPolls = -1;

    public ConsumerHelper() {
        config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG,
                Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT));
        config.put(ConsumerConfiguration.GROUP_ID_CONFIG, ConsumerGroupHelper.getRandomConsumerGroupName());
        config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    public void subscribe(final String topicName) {
        this.topics = Arrays.asList(topicName);
    }


    public Consumer<byte[]> getConsumer() {
        return new DatabusConsumer<>(config, new ByteArrayDeserializer());
    }

    public ConsumerHelper consumerGroup(String consumerGroupName) {
        config.setProperty(ConsumerConfiguration.GROUP_ID_CONFIG,consumerGroupName);
        return this;
    }

    public ConsumerHelper numberOfRecords(final int numberOfRecords) {
        this.numberOfRecords = numberOfRecords;
        return this;
    }


    public ConsumerHelper consume() {
        createAndSubscribe();
        return pollAndCommit(0);
    }

    public ConsumerHelper pollAndCommit(final long timeout) {

        if (topicPartition != null && offset >= 0) {
            consumer.poll(0);
            consumer.seek(topicPartition, offset);
        }

        recordsConsumed = new ArrayList<>();
        int currentPoll = 0;
        boolean stop = false;
        while (!stop) {
            final ConsumerRecords<byte[]> records = consumer.poll(timeout);
            currentPoll++;
            for (ConsumerRecord<byte[]> record : records) {
                recordsConsumed.add(record);
                if (maxPolls < 0) {
                    if (recordsConsumed.size() == numberOfRecords) {
                        stop = true;
                    }
                }
            }
            if (maxPolls >= 0) {
                if (maxPolls == currentPoll) {
                    stop = true;
                }
            }
            boolean isAutoCommitEnabled =
                    Boolean.valueOf(config.getProperty(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG));
            if (!isAutoCommitEnabled) {
                commit();
            }
        }
        return this;

    }


    public ConsumerHelper poll(final long timeout) {
        if (topicPartition != null && offset >= 0) {
            consumer.poll(timeout);
            consumer.seek(topicPartition, offset);
        }
        recordsConsumed = new ArrayList<>();
        final ConsumerRecords<byte[]> records = consumer.poll(timeout);
        for (ConsumerRecord<byte[]> record : records) {
            recordsConsumed.add(record);
        }
        return this;
    }

    public ConsumerHelper commit() {
        consumer.commitSync();
        return this;
    }

    public ConsumerHelper close() {
        try {
            consumer.close();
        } catch (IOException e) {
        }
        return this;
    }


    public List<ConsumerRecord<byte[]>> asList() {
        return recordsConsumed;

    }

    public ConsumerHelper config(final Map<String, Object> config) {
        for (Map.Entry<String, Object> conf : config.entrySet()) {
            this.config.put(conf.getKey(), conf.getValue());
        }
        return this;
    }

    public void seek(final TopicPartition topicPartition, final long offset) {
        this.topicPartition = topicPartition;
        this.offset = offset;

    }

    public ConsumerHelper numberOfPolls(final int numberOfPoll) {
        this.maxPolls = numberOfPoll;
        return this;
    }

    public ConsumerHelper createAndSubscribe() {
        consumer = getConsumer();
        consumer.subscribe(topics);
        return this;
    }

    public void pause() {
        consumer.pause(getConsumerAssignment());
    }

    private List<TopicPartition> getConsumerAssignment() {
        List<TopicPartition>  tp = new ArrayList(consumer.assignment().size());
        int i = 0;
        for (TopicPartition element : consumer.assignment()) {
            tp.add(element);
        }
        return tp;
    }

    public void resume() {

        consumer.resume(getConsumerAssignment());

    }

    public void commited() {

        Set<TopicPartition> assignment = consumer.assignment();
        Iterator<TopicPartition> it = assignment.iterator();
        while (it.hasNext()) {
            TopicPartition tp = it.next();
            OffsetAndMetadata committed = consumer.committed(tp);

        }
    }

    public static  ConsumerHelper consumeFrom(final String topicName, final int parition, long offset) {
        ConsumerHelper consumerHelper = new ConsumerHelper();
        consumerHelper.seek(new TopicPartition(topicName,parition),offset);
        consumerHelper.subscribe(topicName);
        return consumerHelper;
    }

    public static ConsumerHelper consumeFrom(final String topicName) {
        ConsumerHelper consumerHelper = new ConsumerHelper();
        consumerHelper.subscribe(topicName);
        return consumerHelper;
    }


}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/


package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.TopicPartition;
import com.opendxl.databus.consumer.ConsumerRecord;
import com.opendxl.databus.consumer.ConsumerRecords;
import com.opendxl.databus.entities.internal.DatabusMessage;
import com.opendxl.databus.serialization.Deserializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Adapter for ConsumerRecords
 * @param <P> payload's type
 */
public class ConsumerRecordsAdapter<P>
        implements Adapter<org.apache.kafka.clients.consumer.ConsumerRecords<String, DatabusMessage>, ConsumerRecords> {


    private final Deserializer<P> messageDeserializer;

    /**
     *
     * @param messageDeserializer a {@link Deserializer} getInstance used for deserializing the payload
     */
    public ConsumerRecordsAdapter(final Deserializer<P> messageDeserializer) {
        this.messageDeserializer = messageDeserializer;
    }

    /**
     *
     * @param sourceConsumerRecords source Consumer Records
     * @return a {@link ConsumerRecords} getInstance
     */
    @Override
    public ConsumerRecords
    adapt(final org.apache.kafka.clients.consumer.ConsumerRecords<String, DatabusMessage> sourceConsumerRecords) {

        if (sourceConsumerRecords == null) {
            throw new IllegalArgumentException("consumerRecords cannot be null");
        }

        Map<TopicPartition, List<ConsumerRecord<P>>> consumerRecords = new HashMap<>();
        ConsumerRecordAdapter<P> recordAdapter = new ConsumerRecordAdapter<P>(messageDeserializer);

        sourceConsumerRecords.partitions().forEach(topicPartition -> {

            // Get a list of kafka record for a given topic / partition
            final List<org.apache.kafka.clients.consumer.ConsumerRecord<String, DatabusMessage>>
                    topicPartitionRecords = sourceConsumerRecords.records(topicPartition);

            // Get a list of databus ConsumerRecord based on kafka ConsumerRecord
            final List<ConsumerRecord<P>> databusConsumerRecords = new ArrayList<>();
            topicPartitionRecords.forEach(kafkaConsumerRecord -> {
                final ConsumerRecord<P> databusConsumerRecord = recordAdapter.adapt(kafkaConsumerRecord);
                databusConsumerRecords.add(databusConsumerRecord);
            });

            final TopicPartition adaptedTopicPartition =
                    new TopicPartitionAdapter().adapt(topicPartition);
            consumerRecords.put(adaptedTopicPartition, databusConsumerRecords);

        });
        return new ConsumerRecords(consumerRecords);
    }
}

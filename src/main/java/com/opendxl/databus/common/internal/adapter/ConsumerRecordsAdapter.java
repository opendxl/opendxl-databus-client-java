/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/


package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.TopicPartition;
import com.opendxl.databus.consumer.ConsumerRecord;
import com.opendxl.databus.consumer.ConsumerRecords;
import com.opendxl.databus.serialization.Deserializer;
import com.opendxl.databus.serialization.internal.MessageDeserializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter for ConsumerRecords
 * @param <P> payload's type
 */
public class ConsumerRecordsAdapter<P>
        implements Adapter<org.apache.kafka.clients.consumer.ConsumerRecords<String, byte[]>, ConsumerRecords> {

    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerRecordsAdapter.class);

    /**
     * The header filter to filter records.
     */
    protected Map<String, Object> headerFilter;

    /**
     * Consumer record adaptor.
     */
    private ConsumerRecordAdapter<P> recordAdapter;

    /**
     * Consumer record adaptor.
     */
    private ConsumerRecordFilterableAdapter<P> recordFilterableAdapter;

    /**
     * Is Searchable.
     */
    private boolean filterable;

    /**
     * Constructor
     * @param messageDeserializer a {@link Deserializer} getInstance used for deserializing the payload
     */
    public ConsumerRecordsAdapter(final Deserializer<P> messageDeserializer,
            final MessageDeserializer databusMessageDeserializer) {
        this.recordAdapter = new ConsumerRecordAdapter<P>(messageDeserializer, databusMessageDeserializer);
        this.recordFilterableAdapter = new ConsumerRecordFilterableAdapter<P>(messageDeserializer,
            databusMessageDeserializer);
    }

    public void setHeaderFilter(final Map<String, Object> filter) {
        if (filter != null && !filter.isEmpty()) {
            this.headerFilter = filter;
            this.recordFilterableAdapter.setHeaderFilter(filter);
            filterable = true;
            LOG.debug("Record filter is set : " + filter);
        }
    }

    /**
     * Adapter pattern implementation for ConsumerRecords instance.
     * Adapts a DatabusMessage list object for a given topic partition to a ConsumerRecords instance.
     *
     * @param sourceConsumerRecords The consumer records source composed by DatabusMessage list instance.
     * @return a {@link ConsumerRecords} instance.
     */
    @Override
    public ConsumerRecords
    adapt(final org.apache.kafka.clients.consumer.ConsumerRecords<String, byte[]> sourceConsumerRecords) {
        if (sourceConsumerRecords == null) {
            throw new IllegalArgumentException("consumerRecords cannot be null");
        }

        Map<TopicPartition, List<ConsumerRecord<P>>> consumerRecords = new HashMap<>();
        sourceConsumerRecords.partitions().forEach(topicPartition -> {
            // Get a list of kafka record for a given topic / partition
            final List<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>>
                    topicPartitionRecords = sourceConsumerRecords.records(topicPartition);

            // Get a list of databus ConsumerRecord based on kafka ConsumerRecord
            final List<ConsumerRecord<P>> databusConsumerRecords = new ArrayList<>();
            topicPartitionRecords.forEach(kafkaConsumerRecord -> {
                final ConsumerRecord<P> databusConsumerRecord = filterable
                    ? recordFilterableAdapter.adapt(kafkaConsumerRecord) : recordAdapter.adapt(kafkaConsumerRecord);
                if (databusConsumerRecord != null) {
                    databusConsumerRecords.add(databusConsumerRecord);
                }
            });

            final TopicPartition adaptedTopicPartition =
                    new TopicPartitionAdapter().adapt(topicPartition);
            consumerRecords.put(adaptedTopicPartition, databusConsumerRecords);
        });
        return new ConsumerRecords(consumerRecords);
    }
}

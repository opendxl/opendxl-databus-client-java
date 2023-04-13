/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.internal.builder.TopicNameBuilder;
import com.opendxl.databus.common.internal.util.HeaderInternalField;
import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.internal.DatabusMessage;
import com.opendxl.databus.producer.ProducerRecord;
import com.opendxl.databus.serialization.Serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;


/**
 * Adapter for Databus Producer Record
 *
 * @param <P> payload's type
 */
public final class DatabusProducerRecordAdapter<P>
        implements Adapter<ProducerRecord,
        org.apache.kafka.clients.producer.ProducerRecord<String, DatabusMessage>> {

    /**
     * The message deserializer.
     */
    private final Serializer<P> messageSerializer;

    /**
     * Constructor
     *
     * @param messageSerializer a {@link Serializer} instance used for Serializing the payload.
     */
    public DatabusProducerRecordAdapter(final Serializer<P> messageSerializer) {
        this.messageSerializer = messageSerializer;
    }

    /**
     * Adapter pattern implementation for DatabusProducerRecord instance.
     * Adapts a ProducerRecord to a DatabusProducerRecord instance.
     *
     * @param sourceProducerRecord a {@link ProducerRecord} instance.
     * @return a DatabusProducerRecord with a String key and a DatabusMessage value.
     */
    @Override
    public org.apache.kafka.clients.producer.ProducerRecord<String, DatabusMessage>
    adapt(final ProducerRecord sourceProducerRecord) {
        return adapt(sourceProducerRecord, false);
    }

    /**
     * Adapter pattern implementation for DatabusProducerRecord instance.
     * Adapts a ProducerRecord to a DatabusProducerRecord instance.
     *
     * @param sourceProducerRecord a {@link ProducerRecord} instance.
     * @return a DatabusProducerRecord with a String key and a DatabusMessage value.
     */
    public org.apache.kafka.clients.producer.ProducerRecord<String, DatabusMessage>
    adapt(final ProducerRecord sourceProducerRecord, final boolean produceKafkaHeaders) {

        final Headers clonedHeaders = sourceProducerRecord.getHeaders().clone();

        // Add internals header to let the consumer knows a tenantGroup name is part of the topic.
        if (!StringUtils.isBlank(sourceProducerRecord.getRoutingData().getTenantGroup())) {
            clonedHeaders.put(HeaderInternalField.TENANT_GROUP_NAME_KEY,
                    sourceProducerRecord.getRoutingData().getTenantGroup());

            clonedHeaders.put(HeaderInternalField.TOPIC_NAME_KEY,
                    sourceProducerRecord.getRoutingData().getTopic());
        }

        final DatabusMessage databusMessage =
                new MessagePayloadAdapter(messageSerializer, clonedHeaders)
                        .adapt(sourceProducerRecord.payload());

        final String targetTopic =
                TopicNameBuilder.getTopicName(sourceProducerRecord.getRoutingData().getTopic(),
                        sourceProducerRecord.getRoutingData().getTenantGroup());
        final List<Header> kafkaHeaders = produceKafkaHeaders
                ? generateKafkaHeaders(databusMessage.getHeaders()) : null;
        final org.apache.kafka.clients.producer.ProducerRecord<String, DatabusMessage> targetProducerRecord =
                new org.apache.kafka.clients.producer.ProducerRecord<>(targetTopic,
                sourceProducerRecord.getRoutingData().getPartition(),
                sourceProducerRecord.getRoutingData().getShardingKey(),
                databusMessage,
                kafkaHeaders);

        return targetProducerRecord;
    }

    private List<Header> generateKafkaHeaders(Headers headers) {
        final List<Header> kafkaHeaders = new ArrayList<>();
        final Map<String, String> headerMap = headers.getAll();
        for (final String key : headerMap.keySet()) {
                kafkaHeaders.add(new RecordHeader(key, headers.get(key).getBytes()));
        }
        return kafkaHeaders.size() > 0 ? kafkaHeaders : null;
    }
}

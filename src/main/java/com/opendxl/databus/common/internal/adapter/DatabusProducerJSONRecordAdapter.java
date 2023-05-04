/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.internal.builder.TopicNameBuilder;
import com.opendxl.databus.common.internal.util.HeaderInternalField;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.producer.ProducerRecord;
import com.opendxl.databus.serialization.Serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * Adapter for Databus producer record in JSON message format. The incoming message headers are set as kafka message \
 * headers. Message payload is stored as is in JSON format in kafka message payload.
 *
 * @param <P> payload's type
 */
public final class DatabusProducerJSONRecordAdapter<P>
                implements Adapter<ProducerRecord, org.apache.kafka.clients.producer.ProducerRecord<String, byte[]>> {

        /**
         * The message Serializer.
         */
        private final Serializer<P> messageSerializer;

        /**
         * Constructor
         *
         * @param messageSerializer a {@link Serializer} instance used for Serializing
         *                          the payload.
         */
        public DatabusProducerJSONRecordAdapter(final Serializer<P> messageSerializer) {
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
        public org.apache.kafka.clients.producer.ProducerRecord<String, byte[]> adapt(
                        final ProducerRecord sourceProducerRecord) {

                Map<String, String> headers = sourceProducerRecord.getHeaders().getAll();
                final List<Header> kafkaHeaders = new ArrayList<>();

                for (final String key : headers.keySet()) {
                        String val = headers.get(key);
                        if (null == val) {
                                val = "";
                        }
                        kafkaHeaders.add(new RecordHeader(key, val.getBytes()));
                }

                // Add internals header to let the consumer knows a tenantGroup name is part of
                // the topic.
                if (!StringUtils.isBlank(sourceProducerRecord.getRoutingData().getTenantGroup())) {
                        kafkaHeaders.add(new RecordHeader(HeaderInternalField.TENANT_GROUP_NAME_KEY,
                                        sourceProducerRecord.getRoutingData().getTenantGroup().getBytes()));
                }
                if (!StringUtils.isBlank(sourceProducerRecord.getRoutingData().getTopic())) {
                        kafkaHeaders.add(new RecordHeader(HeaderInternalField.TOPIC_NAME_KEY,
                                        sourceProducerRecord.getRoutingData().getTopic().getBytes()));
                }

                final String targetTopic = TopicNameBuilder.getTopicName(
                                sourceProducerRecord.getRoutingData().getTopic(),
                                sourceProducerRecord.getRoutingData().getTenantGroup());


                final org.apache.kafka.clients.producer.ProducerRecord<String, byte[]> targetJSONProducerRecord;
                MessagePayload<P> messagePayload = sourceProducerRecord.payload();
                final byte[] payload = messageSerializer.serialize(messagePayload.getPayload());

                targetJSONProducerRecord = new org.apache.kafka.clients.producer.ProducerRecord<>(targetTopic,
                                sourceProducerRecord.getRoutingData().getPartition(),
                                sourceProducerRecord.getRoutingData().getShardingKey(),
                                payload,
                                kafkaHeaders);

                return targetJSONProducerRecord;
        }

}

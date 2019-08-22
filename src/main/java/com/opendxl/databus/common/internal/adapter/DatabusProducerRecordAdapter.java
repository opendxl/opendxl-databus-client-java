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
import org.apache.commons.lang.StringUtils;


/**
 * Adapter for Databus Producer Record
 *
 * @param <P> payload's type
 */
public final class DatabusProducerRecordAdapter<P>
        implements Adapter<ProducerRecord,
        org.apache.kafka.clients.producer.ProducerRecord<String, DatabusMessage>> {

    private final Serializer<P> messageSerializer;


    /**
     * @param messageSerializer a {@link Serializer} getInstance used for Serializing the payload
     */
    public DatabusProducerRecordAdapter(final Serializer<P> messageSerializer) {
        this.messageSerializer = messageSerializer;
    }


    /**
     * @param sourceProducerRecord a {@link ProducerRecord} getInstance
     * @return a Databus Producer Record with a String key and a DatabusMessage value
     */
    @Override
    public org.apache.kafka.clients.producer.ProducerRecord<String, DatabusMessage>
    adapt(final ProducerRecord sourceProducerRecord) {

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

        final org.apache.kafka.clients.producer.ProducerRecord<String, DatabusMessage> targetProducerRecord;

        if (sourceProducerRecord.getRoutingData().getPartition() == null) {
            targetProducerRecord = new org.apache.kafka.clients.producer.ProducerRecord<>(targetTopic,
                    sourceProducerRecord.getRoutingData().getShardingKey(),
                    databusMessage);
        } else {
            targetProducerRecord = new org.apache.kafka.clients.producer.ProducerRecord<>(targetTopic,
                    sourceProducerRecord.getRoutingData().getPartition(),
                    sourceProducerRecord.getRoutingData().getShardingKey(),
                    databusMessage);
        }
        return targetProducerRecord;
    }
}

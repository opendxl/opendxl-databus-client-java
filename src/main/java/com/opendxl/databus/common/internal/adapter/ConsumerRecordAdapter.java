/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/


package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.internal.util.HeaderInternalField;
import com.opendxl.databus.consumer.ConsumerRecord;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.entities.internal.DatabusMessage;
import com.opendxl.databus.serialization.Deserializer;
import org.apache.commons.lang.StringUtils;

/**
 * Adapter for ConsumerRecord.
 */
public final class ConsumerRecordAdapter<P> implements
        Adapter<org.apache.kafka.clients.consumer.ConsumerRecord<String, DatabusMessage>,
                ConsumerRecord<P>> {

    /**
     * The message deserializer.
     */
    private final Deserializer<P> messageDeserializer;

    /** Constructor
     * @param messageDeserializer a {@link Deserializer} getInstance getInstance used for deserialize the payload.
     */
    public ConsumerRecordAdapter(final Deserializer<P> messageDeserializer) {
        this.messageDeserializer = messageDeserializer;
    }

    /**
     * Adapter pattern implementation for ConsumerRecord.
     * Adapts a DatabusMessage object to a ConsumerRecord.
     *
     * @param sourceConsumerRecord the ConsumerRecord to be adapted.
     * @return A Databus {@link ConsumerRecord}
     */
    @Override
    public ConsumerRecord<P>
    adapt(final org.apache.kafka.clients.consumer.ConsumerRecord<String, DatabusMessage> sourceConsumerRecord) {

        MessagePayload<P> payload =
                new DatabusMessageAdapter<P>(messageDeserializer).adapt(sourceConsumerRecord.value());

        String topic = getTopic(sourceConsumerRecord);
        String tenantGroup = getTenantGroup(sourceConsumerRecord);

        final ConsumerRecord<P> targetConsumerRecord = new ConsumerRecord<P>(sourceConsumerRecord.key(),
                sourceConsumerRecord.value().getHeaders(),
                payload,
                topic,
                tenantGroup,
                sourceConsumerRecord.partition(),
                sourceConsumerRecord.offset(),
                sourceConsumerRecord.timestamp());
        return targetConsumerRecord;
    }

    /**
     * Get the topic name from headers if it exists. Otherwise, it is taken from source Consumer Record.
     * <p>
     * If the user produce a message by using tenantGroup the the topic name comes into headers.
     *
     * @param sourceConsumerRecord source Consumer Record.
     * @return The topic name.
     */
    private String
    getTopic(final org.apache.kafka.clients.consumer.ConsumerRecord<String, DatabusMessage> sourceConsumerRecord) {
        if (!StringUtils.isBlank(sourceConsumerRecord.value().getHeaders().get(HeaderInternalField.TOPIC_NAME_KEY))) {
            final String topic = sourceConsumerRecord.value().getHeaders().get(HeaderInternalField.TOPIC_NAME_KEY);
            sourceConsumerRecord.value().removeHeader(HeaderInternalField.TOPIC_NAME_KEY);
            return topic;
        } else {
            return sourceConsumerRecord.topic();
        }
    }

    /**
     * Get the tenantGroup name from headers if it exists. Otherwise, it is empty.
     *
     * @param sourceConsumerRecord source Consumer Record.
     * @return The tenantGroup name from headers if it exists. Otherwise, it return an empty String.
     */
    private String
    getTenantGroup(final org.apache.kafka.clients.consumer.ConsumerRecord<String, DatabusMessage>
                           sourceConsumerRecord) {
        if (!StringUtils
                .isBlank(sourceConsumerRecord.value().getHeaders().get(HeaderInternalField.TENANT_GROUP_NAME_KEY))) {
            final String tenantGroup =
                    sourceConsumerRecord.value().getHeaders().get(HeaderInternalField.TENANT_GROUP_NAME_KEY);
            sourceConsumerRecord.value().removeHeader(HeaderInternalField.TENANT_GROUP_NAME_KEY);
            return tenantGroup;
        } else {
            return "";
        }
    }

}

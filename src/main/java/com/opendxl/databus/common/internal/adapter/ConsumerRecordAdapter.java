/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/


package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.internal.util.HeaderInternalField;
import com.opendxl.databus.consumer.ConsumerRecord;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.entities.internal.DatabusMessage;
import com.opendxl.databus.serialization.Deserializer;
import com.opendxl.databus.serialization.internal.MessageDeserializer;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.header.Headers;

/**
 * Adapter for ConsumerRecord.
 */
public final class ConsumerRecordAdapter<P> implements
        Adapter<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>,
                ConsumerRecord<P>> {

    /**
     * The message deserializer.
     */
    private final Deserializer<P> messageDeserializer;

    /**
     * The message deserializer.
     */
    private final MessageDeserializer databusMessageDeserializer;

    /** Constructor
     * @param messageDeserializer a {@link Deserializer} getInstance getInstance used for deserialize the payload.
     */
    public ConsumerRecordAdapter(final Deserializer<P> messageDeserializer,
            final MessageDeserializer databusMessageDeserializer) {
        this.messageDeserializer = messageDeserializer;
        this.databusMessageDeserializer = databusMessageDeserializer;
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
    adapt(final org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]> sourceConsumerRecord) {
        final Headers headers = sourceConsumerRecord.headers();
        final byte[] value = sourceConsumerRecord.value();
        final DatabusMessage databusMessage = getDatabusMessage(sourceConsumerRecord.topic(), headers, value);
        if (databusMessage != null) {
            MessagePayload<P> payload =
            new DatabusMessageAdapter<P>(messageDeserializer).adapt(databusMessage);
            String topic = getTopic(databusMessage, sourceConsumerRecord);
            String tenantGroup = getTenantGroup(databusMessage);
            final ConsumerRecord<P> targetConsumerRecord = new ConsumerRecord<P>(sourceConsumerRecord.key(),
            databusMessage.getHeaders(),
            payload,
            topic,
            tenantGroup,
            sourceConsumerRecord.partition(),
            sourceConsumerRecord.offset(),
            sourceConsumerRecord.timestamp());
            return targetConsumerRecord;
        } else {
            return null;
        }
    }

    /**
     * Get the topic name from headers if it exists. Otherwise, it is taken from source Consumer Record.
     * <p>
     * If the user produce a message by using tenantGroup the the topic name comes into headers.
     * @param sourceConsumerRecord
     *
     * @param sourceConsumerRecord source Consumer Record.
     * @return The topic name.
     */
    private String
    getTopic(final DatabusMessage databusMessage,
             final org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]> sourceConsumerRecord) {
        if (!StringUtils.isBlank(databusMessage.getHeaders().get(HeaderInternalField.TOPIC_NAME_KEY))) {
            final String topic = databusMessage.getHeaders().get(HeaderInternalField.TOPIC_NAME_KEY);
            databusMessage.removeHeader(HeaderInternalField.TOPIC_NAME_KEY);
            return topic;
        } else {
            return sourceConsumerRecord.topic();
        }
    }

    /**
     * Get the tenantGroup name from headers if it exists. Otherwise, it is empty.
     *
     * @param databusMessage source Consumer Record.
     * @return The tenantGroup name from headers if it exists. Otherwise, it return an empty String.
     */
    private String
    getTenantGroup(final DatabusMessage databusMessage) {
        if (!StringUtils
                .isBlank(databusMessage.getHeaders().get(HeaderInternalField.TENANT_GROUP_NAME_KEY))) {
            final String tenantGroup =
                    databusMessage.getHeaders().get(HeaderInternalField.TENANT_GROUP_NAME_KEY);
            databusMessage.removeHeader(HeaderInternalField.TENANT_GROUP_NAME_KEY);
            return tenantGroup;
        } else {
            return "";
        }
    }

    private DatabusMessage getDatabusMessage(final String topic, final Headers headers, final byte[] value) {
        final DatabusMessage databusMessage = value == null ? null
            : this.databusMessageDeserializer.deserialize(topic, headers, value);
        return databusMessage;
    }

}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.internal.builder.TopicNameBuilder;
import com.opendxl.databus.common.internal.util.HeaderInternalField;
import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.TierStorageMetadata;
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
        implements Adapter<ProducerRecord<P>,
        org.apache.kafka.clients.producer.ProducerRecord<String, DatabusMessage>> {

    /**
     * The message deserializer.
     */
    private final Serializer<P> userSerializer;

    /**
     * Constructor
     *
     * @param userSerializer a {@link Serializer} instance used for Serializing the payload.
     */
    public DatabusProducerRecordAdapter(final Serializer<P> userSerializer) {
        this.userSerializer = userSerializer;
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
    adapt(final ProducerRecord<P> sourceProducerRecord) {

        final Headers clonedHeaders = sourceProducerRecord.getHeaders().clone();

        // Add internals header to let the consumer knows a tenantGroup name is part of the topic.
        if (!StringUtils.isBlank(sourceProducerRecord.getRoutingData().getTenantGroup())) {
            clonedHeaders.put(HeaderInternalField.TENANT_GROUP_NAME_KEY,
                    sourceProducerRecord.getRoutingData().getTenantGroup());

            clonedHeaders.put(HeaderInternalField.TOPIC_NAME_KEY,
                    sourceProducerRecord.getRoutingData().getTopic());
        }

        // Add internal headers to let consumer knows the payload is tiered storage
        final TierStorageMetadata tierStorageMetadata = sourceProducerRecord.getRoutingData().getTierStorageMetadata();
        if (tierStorageMetadata != null
                && tierStorageMetadata.getBucketName() != null && !tierStorageMetadata.getBucketName().isEmpty()
                && tierStorageMetadata.getObjectName() != null && !tierStorageMetadata.getObjectName().isEmpty()
        ) {
            clonedHeaders.put(HeaderInternalField.TIER_STORAGE_BUCKET_NAME_KEY, tierStorageMetadata.getBucketName());
            clonedHeaders.put(HeaderInternalField.TIER_STORAGE_OBJECT_NAME_KEY, tierStorageMetadata.getObjectName());
        }

        final DatabusMessage databusMessage =
                new MessagePayloadAdapter<>(userSerializer)
                        .adapt(sourceProducerRecord.payload(), clonedHeaders);

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

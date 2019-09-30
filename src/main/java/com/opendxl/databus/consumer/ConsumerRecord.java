/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.common.internal.builder.TopicNameBuilder;

/**
 * It represents a consumer record created by a {@link DatabusConsumer} when a new incoming message comes up.
 *
 * @param <P> Payload's type
 */
public class ConsumerRecord<P> {

    /**
     * The key
     */
    private final String key;

    /**
     * The message payload
     */
    private final MessagePayload<P> messagePayload;

    /**
     * The partition number
     */
    private final Integer partition;

    /**
     * The offset number
     */
    private final long offset;

    /**
     * The timestamp when the consumer record is created
     */
    private final long timestamp;

    /**
     * The Headers
     */
    private final Headers headers;

    /**
     * The Topic name
     */
    private final String topic;

    /**
     * The tenant group
     */
    private final String tenantGroup;

    /**
     * Construct a record received from a specified topic and partition
     * @param key sharding key
     * @param headers headers
     * @param messagePayload messagePayload
     * @param topic topic
     * @param tenantGroup tenant group
     * @param partition parition
     * @param offset offset
     * @param timestamp timestamp
     */
    public ConsumerRecord(final String key,
                          final Headers headers,
                          final MessagePayload<P> messagePayload,
                          final String topic,
                          final String tenantGroup,
                          final Integer partition,
                          final long offset,
                          final long timestamp) {
        this.key = key;
        this.headers = headers;
        this.messagePayload = messagePayload;
        this.topic = topic;
        this.tenantGroup = tenantGroup;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    /**
     * The Composed Topic name
     *
     * @return The Composed Topic name which includes the String sum of topic name with the tenant group.
     */
    public String getComposedTopic() {
        return TopicNameBuilder.getTopicName(getTopic(), getTenantGroup());
    }

    /**
     * The Topic name
     *
     * @return Topic name as String.
     */
    public String getTopic() {

        return topic;
    }

    /**
     * The Tenant Group
     *
     * @return Tenant Group as String.
     */
    public String getTenantGroup() {
        return tenantGroup;
    }

    /**
     * The Key
     *
     * @return The Key as String.
     */
    public String getKey() {
        return key;
    }

    /**
     * The Headers
     *
     * @return The Headers.
     */
    public Headers getHeaders() {
        return headers;
    }

    /**
     * The message payload
     *
     * @return A MessagePayload instance
     *
     */
    public MessagePayload<P> getMessagePayload() {
        return messagePayload;
    }

    /**
     * The Offset number
     *
     * @return Offset
     */

    public long getOffset() {
        return offset;
    }

    /**
     * The Partition number
     *
     * @return The partition number
     */
    public int getPartition() {
        return partition;
    }

    /**
     * The Timestamp as Long
     *
     * @return Timestamp when the consumer record was created
     */
    public long getTimestamp() {
        return timestamp;
    }

}

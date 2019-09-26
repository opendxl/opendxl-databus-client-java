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

    private final String key;
    private final MessagePayload<P> messagePayload;
    private final Integer partition;
    private final long offset;
    private final long timestamp;
    private final Headers headers;
    private final String topic;
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
     * @return The Composed Topic name.
     */
    public String getComposedTopic() {
        return TopicNameBuilder.getTopicName(getTopic(), getTenantGroup());
    }

    /**
     * The Topic
     *
     * @return Topic name.
     */
    public String getTopic() {

        return topic;
    }

    /**
     * The Tenant Group
     *
     * @return Tenant Group.
     */
    public String getTenantGroup() {
        return tenantGroup;
    }

    /**
     * The Key
     *
     * @return Key
     */
    public String getKey() {
        return key;
    }

    /**
     * The Headers
     *
     * @return Headers.
     */
    public Headers getHeaders() {
        return headers;
    }

    /**
     * The messagePayload
     *
     * @return Payload
     */
    public MessagePayload<P> getMessagePayload() {
        return messagePayload;
    }

    /**
     * The Offset
     *
     * @return Offset
     */

    public long getOffset() {
        return offset;
    }

    /**
     *  The Partition
     *
     * @return Partition
     */
    public int getPartition() {
        return partition;
    }

    /**
     * The Timestamp
     *
     * @return Timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

}

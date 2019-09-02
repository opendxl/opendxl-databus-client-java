/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli.entity;

import java.util.Map;

/**
 * <p>Helper class use to deserialize the Consumer instance </p>
 *
 */
public class ConsumerRecordResult {

    private final String shardingKey;
    private final String payload;
    private final String composedTopic;
    private final String topic;
    private final String tenantGroup;
    private final Map<String, String> headers;
    private final long offset;
    private final int partition;
    private final long timestamp;

    /**
     *  Constructor
     *
     * @param shardingKey Sharding Key
     * @param payload message
     * @param composedTopic topic name including the tenant group if apply
     * @param topic topic name
     * @param tenantGroup tenant group
     * @param headers headers
     * @param offset message offset
     * @param partition partition
     * @param timestamp message timestamp
     */
    public ConsumerRecordResult(final String shardingKey,
                                final String payload,
                                final String composedTopic,
                                final String topic,
                                final String tenantGroup,
                                final Map<String, String> headers,
                                final long offset,
                                final int partition,
                                final long timestamp) {

        this.shardingKey = shardingKey;
        this.payload = payload;
        this.composedTopic = composedTopic;
        this.topic = topic;
        this.tenantGroup = tenantGroup;
        this.headers = headers;
        this.offset = offset;
        this.partition = partition;
        this.timestamp = timestamp;

    }
}

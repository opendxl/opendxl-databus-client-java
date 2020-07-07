/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.entities;

import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.producer.DatabusProducer;
import com.opendxl.databus.producer.ProducerRecord;
import org.apache.commons.lang.StringUtils;

import java.util.Optional;

/**
 * <p>
 * Represent a address where a message must be sent.
 * It is used by {@link ProducerRecord}
 * to know what the destination is.
 * It contains a mandatory topic name as well as optionals sharding key, tenant group,
 * partitions and tier storage metadata.
 * </p>
 * <p>
 * See how to use in {@link DatabusProducer} example
 * </p>
 */
public class RoutingData {

    /**
     * A default tenant group with default value "group0"
     */
    private static final String DEFAULT_TENANT_GROUP = "group0";

    /**
     * The partition number
     */
    private final Integer partition;

    /**
     * The topic name
     */
    private String topic;

    /**
     * The sharding key value
     */
    private String shardingKey;

    /**
     * The tenant group
     */
    private String tenantGroup = DEFAULT_TENANT_GROUP;

    /**
     * Tier Storage Metadata
     */
    private TierStorageMetadata tierStorageMetadata;

    /**
     * RoutingData constructor with only topic name parameter
     *
     * @param topic The topic name where the message must be sent
     */
    public RoutingData(final String topic) {
        this(topic, null, null, null, null);
    }

    public RoutingData(final String topic, final TierStorageMetadata tierStorageMetadata) {
        this(topic, null, null, null, tierStorageMetadata);
    }
    /**
     * RoutingData constructor with topic name sharding key and tenant group parameters
     *
     * @param topic The topic name where the message must be sent
     * @param shardingKey The Databus sharding key
     * @param tenantGroup The name that groups topics
     */
    public RoutingData(final String topic, final String shardingKey, final String tenantGroup) {
        this(topic, shardingKey, tenantGroup, null, null);
    }

    public RoutingData(final String topic,
                       final String shardingKey,
                       final String tenantGroup,
                       final TierStorageMetadata tierStorageMetadata) {
        this(topic, shardingKey, tenantGroup, null, tierStorageMetadata);
    }
    /**
     * RoutingData constructor with all parameters
     *
     * @param topic The topic name where the message must be sent
     * @param shardingKey The Databus sharding key
     * @param tenantGroup The name that groups topics
     * @param partition The partition number
     * @param tierStorageMetadata Tier Storage Metadata
     *
     */
    public RoutingData(final String topic, final String shardingKey, final String tenantGroup,
                       final Integer partition, final TierStorageMetadata tierStorageMetadata) {
        if (StringUtils.isBlank(topic)) {
            throw new DatabusClientRuntimeException("topic cannot be empty or null", RoutingData.class);
        }
        this.topic = topic;
        this.tenantGroup = Optional.ofNullable(tenantGroup).orElse("").trim();
        this.shardingKey = shardingKey;
        this.partition = partition;
        this.tierStorageMetadata = tierStorageMetadata;
    }

    /**
     * Gets the topic name
     *
     * @return A String with the topic name
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Gets the sharding key
     *
     * @return A String with the sharding key
     */
    public String getShardingKey() {
        return shardingKey;
    }

    /**
     * Gets the tenant group
     *
     * @return A String with the tentant group
     */
    public String getTenantGroup() {
        return tenantGroup;
    }

    /**
     * Gets the partition number
     *
     * @return An Integer with the partition number
     */
    public Integer getPartition() {
        return partition;
    }

    public TierStorageMetadata getTierStorageMetadata() {
        return tierStorageMetadata;
    }

}

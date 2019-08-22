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
 * It contains a mandatory topic name as well as optionals sharding key and tenant group and partitions.
 * </p>
 * <p>
 * See how to use in {@link DatabusProducer} example
 * </p>
 */
public class RoutingData {
    private static final String DEFAULT_TENANT_GROUP = "group0";
    private final Integer partition;
    private String topic = null;
    private String shardingKey = null;
    private String tenantGroup = DEFAULT_TENANT_GROUP;

    /**
     * @param topic       name where the message must be sent
     */
    public RoutingData(final String topic) {
        this(topic, null, null, null);
    }

    /**
     * @param topic       name where the message must be sent
     * @param shardingKey databus key
     * @param tenantGroup a name that groups topics
     */
    public RoutingData(final String topic, final String shardingKey, final String tenantGroup) {
        this(topic, shardingKey, tenantGroup, null);
    }

    /**
     * @param topic       name where the message must be sent
     * @param shardingKey databus key
     * @param tenantGroup a name that groups topics
     * @param partition   partition
     */
    public RoutingData(final String topic, final String shardingKey, final String tenantGroup,
                       final Integer partition) {

        if (StringUtils.isBlank(topic)) {
            throw new DatabusClientRuntimeException("topic cannot be empty or null", RoutingData.class);
        }
        this.topic = topic;
        this.tenantGroup = Optional.ofNullable(tenantGroup).orElse("").trim();
        this.shardingKey = shardingKey;
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public String getShardingKey() {
        return shardingKey;
    }

    public String getTenantGroup() {
        return tenantGroup;
    }

    public Integer getPartition() {
        return partition;
    }
}

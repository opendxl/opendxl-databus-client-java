/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.TopicPartition;

/**
 * Adapter for TopicPartition
 */
public final class TopicPartitionAdapter implements Adapter<org.apache.kafka.common.TopicPartition, TopicPartition> {

    /**
     * Creates a {@link TopicPartition} getInstance
     *
     * @param sourceTopicPartition source topic partition
     * @return {@link TopicPartition} getInstance
     */
    @Override
    public TopicPartition adapt(final org.apache.kafka.common.TopicPartition sourceTopicPartition) {
        return new TopicPartition(sourceTopicPartition.topic(), sourceTopicPartition.partition());
    }
}

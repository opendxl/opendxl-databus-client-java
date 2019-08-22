/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

import com.opendxl.databus.common.TopicPartition;

import java.util.Collection;

/**
 * A non-functional implementation of {@link ConsumerRebalanceListener}
 */
public class NoOpConsumerRebalanceListener implements ConsumerRebalanceListener {
    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {

    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {

    }
}

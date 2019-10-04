/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/


package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.PartitionInfo;

/**
 * Adapter for PartitionInfo.
 */
public final class PartitionInfoAdapter implements Adapter<org.apache.kafka.common.PartitionInfo, PartitionInfo> {

    /**
     * Adapter pattern implementation for PartitionInfo instance.
     * Adapts a {@code org.apache.kafka.common.PartitionInfo} to a
     * {@code com.opendxl.databus.common.PartitionInfo} instance.
     *
     * @param sourcePartitionInfo Instance of source org.apache.kafka.common.PartitionInfo.
     * @return A {@link PartitionInfo} instance.
     */
    @Override
    public PartitionInfo adapt(final org.apache.kafka.common.PartitionInfo sourcePartitionInfo) {
        final PartitionInfo partitionInfo =  new PartitionInfo(sourcePartitionInfo.topic(),
                sourcePartitionInfo.partition(),
                new NodeAdapter().adapt(sourcePartitionInfo.leader()),
                new NodeArrayAdapter().adapt(sourcePartitionInfo.replicas()),
                new NodeArrayAdapter().adapt(sourcePartitionInfo.inSyncReplicas()));
        return partitionInfo;
    }
}

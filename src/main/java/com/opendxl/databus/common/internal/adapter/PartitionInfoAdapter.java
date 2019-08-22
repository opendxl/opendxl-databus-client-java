/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/


package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.PartitionInfo;

/**
 * Adapter for PartitionInfo
 */
public final class PartitionInfoAdapter implements Adapter<org.apache.kafka.common.PartitionInfo, PartitionInfo> {

    /**
     *
     * @param sourcePartitionInfo getInstance of source Partition Info
     * @return a {@link PartitionInfo} getInstance
     */
    @Override
    public PartitionInfo adapt(final org.apache.kafka.common.PartitionInfo sourcePartitionInfo) {
        final PartitionInfo partitionInfo =  new PartitionInfo(sourcePartitionInfo.topic(),
                sourcePartitionInfo.partition(),
                new NodeAdater().adapt(sourcePartitionInfo.leader()),
                new NodeArrayAdapter().adapt(sourcePartitionInfo.replicas()),
                new NodeArrayAdapter().adapt(sourcePartitionInfo.inSyncReplicas()));
        return partitionInfo;
    }
}

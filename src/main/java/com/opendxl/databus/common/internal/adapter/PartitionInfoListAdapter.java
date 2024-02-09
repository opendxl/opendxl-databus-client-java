/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/


package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.PartitionInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Adapter a List of PartitionInfo.
 */
public class PartitionInfoListAdapter
        implements Adapter<List<org.apache.kafka.common.PartitionInfo>, List<PartitionInfo>> {

    /**
     * Adapter pattern implementation for PartitionInfo List instance.
     * Adapts a {@code org.apache.kafka.common.PartitionInfo} to a {@code com.opendxl.databus.common.PartitionInfo}
     * instance.
     *
     * @param sourcePartitionInfoList a List of source org.apache.kafka.common.PartitionInfo.
     * @return a List of {@link PartitionInfo}
     */
    @Override
    public List<PartitionInfo> adapt(final List<org.apache.kafka.common.PartitionInfo> sourcePartitionInfoList) {

        final List<PartitionInfo> targetParitionInfoList = new ArrayList<>();

        sourcePartitionInfoList.forEach(partition -> {
            targetParitionInfoList.add(new PartitionInfoAdapter().adapt(partition));
        });
        return targetParitionInfoList;
    }
}

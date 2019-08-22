/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/


package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.PartitionInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Adapter a List of PartitionInfo
 */
public class PartitionInfoListAdapter
        implements Adapter<List<org.apache.kafka.common.PartitionInfo>, List<PartitionInfo>> {

    /**
     * @param sourcePartitionInfoList a List of source Partition Info
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

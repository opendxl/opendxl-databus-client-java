/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.PartitionInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Adapter for a Map of Topic and Partition Info
 */
public final class TopicPartitionInfoListAdapter
        implements Adapter<Map<String, List<org.apache.kafka.common.PartitionInfo>>, Map<String, List<PartitionInfo>>> {

    /**
     *
     * @param sourceTopicPartitionInfoList a source Map of Topic and Partition Info List
     * @return a Map of Topic and {@link PartitionInfo } List
     */
    @Override
    public Map<String, List<PartitionInfo>>
    adapt(final Map<String, List<org.apache.kafka.common.PartitionInfo>> sourceTopicPartitionInfoList) {

        final Map<String, List<PartitionInfo>> targetTopicPartitionInfoList =
                new HashMap<>(sourceTopicPartitionInfoList.size());

        sourceTopicPartitionInfoList.forEach((topic, sourcePartitionInfoList) -> {

            final List<PartitionInfo> targetPartitionInfoList =
                    new PartitionInfoListAdapter().adapt(sourcePartitionInfoList);

            targetTopicPartitionInfoList.put(topic, targetPartitionInfoList);
        });

        return targetTopicPartitionInfoList;
    }
}

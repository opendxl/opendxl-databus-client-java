/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer.metric;

import com.opendxl.databus.common.TopicPartition;

import java.util.Map;

public class ConsumerMetricPerClientIdAndTopicPartitions {

    private final String clientId;
    private final Map<TopicPartition, ConsumerMetric> topicPartitionsMetrics;

    public
    ConsumerMetricPerClientIdAndTopicPartitions(final String clientId,
                                                final Map<TopicPartition, ConsumerMetric> topicPartitionsMetrics) {
        this.clientId = clientId;
        this.topicPartitionsMetrics = topicPartitionsMetrics;
    }

    public Map<TopicPartition, ConsumerMetric> getTopicPartitionsMetrics() {
        return topicPartitionsMetrics;
    }

    public String getClientId() {
        return clientId;
    }

}

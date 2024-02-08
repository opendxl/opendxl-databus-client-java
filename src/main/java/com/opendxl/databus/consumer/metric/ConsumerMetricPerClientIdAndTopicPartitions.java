/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer.metric;

import com.opendxl.databus.common.TopicPartition;

import java.util.Map;

public class ConsumerMetricPerClientIdAndTopicPartitions {

    /**
     * The client id.
     */
    private final String clientId;

    /**
     * A map object with contains topic partition as key and a consumer metric as value.
     * This map stores the consumer metric data for each topic partition of the consumer.
     */
    private final Map<TopicPartition, ConsumerMetric> topicPartitionsMetrics;

    /**
     * ConsumerMetricPerClientIdAndTopicPartitions constructor.
     * @param clientId An associated clientId.
     * @param topicPartitionsMetrics An associated map with topic-partition as key an consumerMetric as value.
     */
    public
    ConsumerMetricPerClientIdAndTopicPartitions(final String clientId,
                                                final Map<TopicPartition, ConsumerMetric> topicPartitionsMetrics) {
        this.clientId = clientId;
        this.topicPartitionsMetrics = topicPartitionsMetrics;
    }

    /**
     * Returns the map of topic partitions with an associated consumer metric value.
     * @return A map object of topic-partition as key and consumer metric as value.
     */
    public Map<TopicPartition, ConsumerMetric> getTopicPartitionsMetrics() {
        return topicPartitionsMetrics;
    }

    /**
     * Returns the client id.
     * @return The client id.
     */
    public String getClientId() {
        return clientId;
    }

}

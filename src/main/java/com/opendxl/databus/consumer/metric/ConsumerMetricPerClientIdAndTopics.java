/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer.metric;

import com.opendxl.databus.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

public class ConsumerMetricPerClientIdAndTopics {

    /**
     * The client id.
     */
    private final String clientId;

    /**
     * The consumer metric associated.
     */
    private final ConsumerMetric consumerMetric;

    /**
     * A map topic-partition as key and consumer metric as value.
     */
    private final Map<TopicPartition, ConsumerMetric> topicMetrics;

    /**
     * ConsumerMetricPerClientIdAndTopicPartitions constructor.
     * @param clientId An associated clientId.
     * @param topicMetrics An associated map with topic-partition as key an consumerMetric as value.
     */
    public ConsumerMetricPerClientIdAndTopics(final String clientId,
                                              final ConsumerMetric consumerMetric,
                                              final Map<TopicPartition, ConsumerMetric> topicMetrics) {
        this.clientId = clientId;
        this.consumerMetric = consumerMetric;
        this.topicMetrics = topicMetrics;
    }

    /**
     * Gets the client id.
     * @return The client id.
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Gets the metric value.
     * @return The metric value.
     */
    public double getValue() {
        return consumerMetric.getValue();
    }

    /**
     * Gets the metric name.
     * @return The metric name.
     */
    public String getMetricName() {
        return consumerMetric.getMetricName();
    }

    /**
     * Gets the metric description.
     * @return The metric description.
     */
    public String getMetricDescription() {
        return consumerMetric.getMetricDescription();
    }

    /**
     * Gets the metric group.
     * @return The metric group.
     */
    public String getMetricGroup() {
        return consumerMetric.getMetricGroup();
    }

    /**
     * Gets the metric tag.
     * @return The metric tag.
     */
    public Map<String, String> getMetricTags() {
        return consumerMetric.getMetricTags();
    }

    /**
     * Gets a map with the topic name and the associated metric value.
     * @return A map with the topic name and the associated metric value.
     */
    public Map<String, ConsumerMetric> getTopicMetrics() {
        Map<String, ConsumerMetric> topicMetrics = new HashMap<>();
        for (Map.Entry<TopicPartition, ConsumerMetric> topicMetric : this.topicMetrics.entrySet()) {
            topicMetrics.put(topicMetric.getKey().topic(), topicMetric.getValue());
        }
        return topicMetrics;
    }
}

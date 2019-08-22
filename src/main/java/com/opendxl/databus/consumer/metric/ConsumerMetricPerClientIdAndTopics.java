/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer.metric;

import com.opendxl.databus.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

public class ConsumerMetricPerClientIdAndTopics {

    private final String clientId;
    private final ConsumerMetric consumerMetric;
    private final Map<TopicPartition, ConsumerMetric> topicMetrics;


    public ConsumerMetricPerClientIdAndTopics(final String clientId,
                                              final ConsumerMetric consumerMetric,
                                              final Map<TopicPartition, ConsumerMetric> topicMetrics) {

        this.clientId = clientId;
        this.consumerMetric = consumerMetric;
        this.topicMetrics = topicMetrics;
    }

    public String getClientId() {
        return clientId;
    }

    public double getValue() {
        return consumerMetric.getValue();
    }

    public String getMetricName() {
        return consumerMetric.getMetricName();
    }

    public String getMetricDescription() {
        return consumerMetric.getMetricDescription();
    }

    public String getMetricGroup() {
        return consumerMetric.getMetricGroup();
    }

    public Map<String, String> getMetricTags() {
        return consumerMetric.getMetricTags();
    }

    public Map<String, ConsumerMetric> getTopicMetrics() {
        Map<String, ConsumerMetric> topicMetrics = new HashMap<>();
        for (Map.Entry<TopicPartition, ConsumerMetric> topicMetric : this.topicMetrics.entrySet()) {
            topicMetrics.put(topicMetric.getKey().topic(), topicMetric.getValue());
        }
        return topicMetrics;
    }
}

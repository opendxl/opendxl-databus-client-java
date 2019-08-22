/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer.metric;

import com.opendxl.databus.common.MetricName;
import com.opendxl.databus.common.TopicPartition;
import org.apache.kafka.common.Metric;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerMetricsBuilder {

    private static final String METRIC_GROUP_NAME = "consumer-fetch-manager-metrics";

    private ConsumerMetricsBuilder() {

    }


    public static ConsumerMetricPerClientId
    buildClientMetric(final Map<MetricName, ? extends Metric> metrics,
                      final String metricAttributeName,
                      final String clientId) {

        final Map<String, String> tags = TagsBuilder
                .newInstance()
                .addClientIdTag(clientId)
                .build();

        return new ConsumerMetricPerClientId(clientId,
                buildMetric(metrics, metricAttributeName, tags));
    }


    public static ConsumerMetricPerClientIdAndTopics
    buildClientTopicMetric(final Map<MetricName, ? extends Metric> metrics,
                           final String metricAttributeName,
                           final String clientId,
                           final List<TopicPartition> topics) {


        final TagsBuilder tagsBuilder = TagsBuilder.newInstance();

        final Map<String, String> tags = tagsBuilder
                .addClientIdTag(clientId)
                .build();

        final ConsumerMetric clientIdMetric = buildMetric(metrics, metricAttributeName, tags);

        final Map<TopicPartition, ConsumerMetric> topicPartitionMetrics = new HashMap<>();
        for (TopicPartition topicPartition : topics) {
            tagsBuilder.addTopicTag(topicPartition.topic());
            topicPartitionMetrics.put(topicPartition, buildMetric(metrics, metricAttributeName, tagsBuilder.build()));
        }

        return new ConsumerMetricPerClientIdAndTopics(clientId, clientIdMetric, topicPartitionMetrics);

    }


    public static ConsumerMetricPerClientIdAndTopicPartitions
    buildClientTopicPartitionMetric(final Map<MetricName, ? extends Metric> metrics,
                                    final String metricAttributeName,
                                    final String clientId,
                                    final List<TopicPartition> topicPartitions) {

        TagsBuilder tagsBuilder = TagsBuilder.newInstance().addClientIdTag(clientId);

        Map<TopicPartition, ConsumerMetric> topicPartitionMetrics = new HashMap<>();
        for (TopicPartition topicPartition : topicPartitions) {
            tagsBuilder.addTopicTag(topicPartition.topic());
            tagsBuilder.addPartitionTag(topicPartition.partition());
            topicPartitionMetrics.put(topicPartition, buildMetric(metrics, metricAttributeName, tagsBuilder.build()));
        }

        return new ConsumerMetricPerClientIdAndTopicPartitions(clientId, topicPartitionMetrics);
    }


    private static ConsumerMetric buildMetric(final Map<MetricName, ? extends Metric> metrics,
                                              final String metricAttributeName,
                                              final Map<String, String> tags) {

        MetricName metricName = new MetricName(metricAttributeName, METRIC_GROUP_NAME, "", tags);

        final Metric metric = metrics.get(metricName);
        double value = 0;
        if (metric != null) {
            value = Double.valueOf(metric.metricValue().toString()).doubleValue();
        } else {

        }
        return new ConsumerMetric(metricName, value);
    }


    private static class TagsBuilder {

        private static final String CLIENT_ID_TAG = "client-id";
        private static final String TOPIC_TAG = "topic";
        private static final String PARTITION_TAG = "partition";

        Map<String, String> tags;

        private TagsBuilder() {
            tags = new HashMap<>();
        }

        public static TagsBuilder newInstance() {
            return new TagsBuilder();
        }

        public TagsBuilder addClientIdTag(final String clientId) {
            tags.put(CLIENT_ID_TAG, clientId);
            return this;
        }

        public TagsBuilder addTopicTag(final String topic) {
            tags.put(TOPIC_TAG, topic);
            return this;
        }

        public TagsBuilder addPartitionTag(final int partition) {
            tags.put(PARTITION_TAG, String.valueOf(partition));
            return this;
        }

        public Map<String, String> build() {
            return tags;
        }
    }

}

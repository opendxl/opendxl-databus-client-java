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

/**
 *  ConsumerMetricsBuilder. This class is responsible to build a {@link ConsumerMetric} instance.<br>
 *  ConsumerMetricsBuilder can build three consumer metrics:<br>
 *      <ol>
 *      <li>ClientIdMetric: a {@link ConsumerMetricPerClientId} which gives a result value related to metric
 *      associated to the clientId.
 *      <li>ClientIdTopicMetric: a {@link ConsumerMetricPerClientIdAndTopics} which gives a result value related
 *      to metric associated to the clientId and a specific topic.
 *      <li>ClientIdTopicPartitionMetric: a {@link ConsumerMetricPerClientIdAndTopicPartitions} which gives a
 *      result value related to metric associated to the clientId and a specific topic and partition.
 *      </ol>
 */

public class ConsumerMetricsBuilder {

    /**
     * The metric group name.
     */
    private static final String METRIC_GROUP_NAME = "consumer-fetch-manager-metrics";

    private ConsumerMetricsBuilder() {

    }

    /**
     * This method builds an instance of a {@link ConsumerMetricPerClientId} which gives a result value related to
     * metric associated to the clientId.<br>
     * @param metrics The Kafka list metrics.
     * @param metricAttributeName The name of the Kafka metric that will be measured.
     * @param clientId The clientId associated to the consumer.
     * @return A {@link ConsumerMetricPerClientId} instance.
     */
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

    /**
     * This method builds an instance of a {@link ConsumerMetricPerClientIdAndTopics} which gives a resultvalue
     * related to metric associated to an specific topic provided as a parameter associated to the clientId.<br>
     * @param metrics The Kafka list metrics.
     * @param metricAttributeName The name of the Kafka metric that will be measured.
     * @param clientId The clientId associated to the consumer.
     * @param topics The topicPartition which contains the topic name.
     * @return A {@link ConsumerMetricPerClientIdAndTopics} instance.
     */
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

    /**
     * This method builds an instance of a {@link ConsumerMetricPerClientIdAndTopicPartitions} which gives a
     * result value related to metric associated to an specific topic-partition provided as a parameter associated to
     * the clientId.<br>
     * @param metrics The Kafka list metrics.
     * @param metricAttributeName The name of the Kafka metric that will be measured.
     * @param clientId The clientId associated to the consumer.
     * @param topicPartitions The topicPartitions which contains the topic-partition asociated.
     * @return A {@link ConsumerMetricPerClientIdAndTopics} instance.
     */
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

    /**
     * This method builds an instance of a {@link ConsumerMetric} which gives a
     * result value related to a consumer metric.<br>
     * @param metrics The Kafka list metrics.
     * @param metricAttributeName The name of the Kafka metric that will be measured.
     * @param tags A list of tags to build the metric.
     * @return A {@link ConsumerMetric} instance.
     */
    private static ConsumerMetric buildMetric(final Map<MetricName, ? extends Metric> metrics,
                                              final String metricAttributeName,
                                              final Map<String, String> tags) {

        MetricName metricName = new MetricName(metricAttributeName, METRIC_GROUP_NAME, "", tags);

        final Metric metric = metrics.get(metricName);
        double value = 0;
        if (metric != null) {
            value = Double.valueOf(metric.metricValue().toString()).doubleValue();
        }

        return new ConsumerMetric(metricName, value);
    }

    /**
     *  TagsBuilder. This class is responsible to build a map of tags instance.<br>
     *  TagsBuilder can build with two specifications:<br>
     *      <ol>
     *      <li> addClientIdTag: a TagsBuilder which gives a result value related to metric associated to
     *      the clientId.
     *      <li> addTopicTag: a TagsBuilder which gives a result value related to metric associated
     *      to the clientId and a specific topic.
     *      <li> addPartitionTag: a TagsBuilder which gives a result value related to metric associated
     *      to the clientId and a specific partition.
     *      </ol>
     */
    private static class TagsBuilder {

        /**
         * The client id tag.
         */
        private static final String CLIENT_ID_TAG = "client-id";

        /**
         * The topic name tag.
         */
        private static final String TOPIC_TAG = "topic";

        /**
         * The partition number tag.
         */
        private static final String PARTITION_TAG = "partition";

        /**
         * The key value map of tags.
         */
        Map<String, String> tags;

        private TagsBuilder() {
            tags = new HashMap<>();
        }

        /**
         * Creates an new instance of TagsBuilder.
         * @return A TagsBuilder instance.
         */
        public static TagsBuilder newInstance() {
            return new TagsBuilder();
        }

        /**
         * This method adds clientId parameter in order to build the tags instance.
         * @param clientId An String which contains the cliendId associated.
         */
        public TagsBuilder addClientIdTag(final String clientId) {
            tags.put(CLIENT_ID_TAG, clientId);
            return this;
        }

        /**
         * This method adds topic parameter in order to build the tags instance.
         * @param topic An String which contains the topic associated.
         */
        public TagsBuilder addTopicTag(final String topic) {
            tags.put(TOPIC_TAG, topic);
            return this;
        }

        /**
         * This method adds partition parameter in order to build the tags instance.
         * @param partition An String which contains the partition associated.
         */
        public TagsBuilder addPartitionTag(final int partition) {
            tags.put(PARTITION_TAG, String.valueOf(partition));
            return this;
        }

        /**
         * This method build the full tag instance object with the specified parameters.
         * @return A map object of tags.
         */
        public Map<String, String> build() {
            return tags;
        }
    }

}

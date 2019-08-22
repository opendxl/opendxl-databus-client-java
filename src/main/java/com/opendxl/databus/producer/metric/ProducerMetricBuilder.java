/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer.metric;

import com.opendxl.databus.common.MetricName;
import com.opendxl.databus.common.TopicPartition;
import org.apache.kafka.common.Metric;

import java.util.HashMap;
import java.util.Map;

/**
 *  ProducerMetricBuilder. This class is responsible to build a {@link ProducerMetric} instance.<br>
 *  ProducerMetricBuilder can build two producer metrics:<br>
 *      <ol>
 *      <li>ClientIdMetric: a {@link ProducerMetric} which gives a result value related to metric associated to
 *      the clientId.
 *      <li>ClientIdTopicMetric: a {@link ProducerMetric} which gives a result value related to metric associated
 *      to the clientId and a specific topic.
 *      </ol>
 */
public class ProducerMetricBuilder {

    /**
     * A String which contains metrics group name for clientId producer metrics.
     */
    private static final String METRIC_GROUP_NAME_PER_CLIENT_ID = "producer-metrics";

    /**
     * A String which contains metrics group name for clientId and Topic producer metrics.
     */
    private static final String METRIC_GROUP_NAME_CLIENT_ID_AND_TOPIC = "producer-topic-metrics";

    private ProducerMetricBuilder() {

    }

    /**
     * This method builds an instance of a {@link ProducerMetric} which gives a result value related to
     * metric associated to the clientId.<br>
     * @param metrics The Kafka list metrics.
     * @param metricAttributeName The name of the Kafka metric that will be measured.
     * @param clientId The clientId associated to the producer.
     * @return A {@link ProducerMetric} instance.
     */
    public static ProducerMetric buildClientIdMetric(final Map<MetricName, ? extends Metric> metrics,
                                                     final String metricAttributeName, final String clientId) {
        final Map<String, String> tags = TagsBuilder.newInstance().withClientIdTag(clientId)
                .build();

        return getProducerMetric(metricAttributeName, METRIC_GROUP_NAME_PER_CLIENT_ID, tags, metrics, clientId);
    }

    /**
     * This method builds an instance of a {@link ProducerMetric} which gives a result value related to
     * metric associated to an specific topic provided as a parameter associated to the clientId.<br>
     * @param metrics The Kafka list metrics.
     * @param metricAttributeName The name of the Kafka metric that will be measured.
     * @param clientId The clientId associated to the producer.
     * @param topicPartition The topicPartition which contains the topic name.
     * @return A {@link ProducerMetric} instance.
     */
    public static ProducerMetric buildClientIdTopicMetric(final Map<MetricName, ? extends Metric> metrics,
                                                          final String metricAttributeName,
                                                          final String clientId,
                                                          final TopicPartition topicPartition) {
        final TagsBuilder tagsBuilder = TagsBuilder.newInstance();

        Map<String, String> tags = tagsBuilder.withTopicTag(topicPartition.topic()).withClientIdTag(clientId).build();

        return getProducerMetric(metricAttributeName, METRIC_GROUP_NAME_CLIENT_ID_AND_TOPIC, tags, metrics, clientId);
    }

    /**
     * This method builds an instance of a {@link ProducerMetric} which gives a metric and associate his value.<br>
     * If metric is not found, default value associated is 0.<br>
     * @param metrics The Kafka list metrics.
     * @param metricAttributeName The name of the Kafka metric that will be measured.
     * @param clientId The clientId associated to the producer.
     * @return A {@link ProducerMetric} instance with an associated value.
     */
    private static ProducerMetric getProducerMetric(final String metricAttributeName, final String metricGroup,
                                                    Map<String, String> tags,
                                                    final Map<MetricName, ? extends Metric> metrics,
                                                    final String clientId) {
        MetricName metricName = new MetricName(metricAttributeName, metricGroup, "", tags);

        final Metric metric = metrics.get(metricName);

        double value = 0;
        if (metric != null) {
            value = Double.valueOf(metric.metricValue().toString()).doubleValue();
        }

        return new ProducerMetric(metricName, value, clientId);
    }

    /**
     *  TagsBuilder. This class is responsible to build a map of tags instance.<br>
     *  TagsBuilder can build with two specifications:<br>
     *      <ol>
     *      <li> withClientIdTag: a TagsBuilder which gives a result value related to metric associated to
     *      the clientId.
     *      <li> withTopicTag: a TagsBuilder which gives a result value related to metric associated
     *      to the clientId and a specific topic.
     *      </ol>
     */
    private static class TagsBuilder {
        private static final String CLIENT_ID_TAG = "client-id";
        private static final String TOPIC_TAG = "topic";

        Map<String, String> tags;

        private TagsBuilder() {
            tags = new HashMap<>();
        }

        /**
         * Creates an new instance of TagsBuilder.
         * @return A TagsBuilder instance.
         */
        private static TagsBuilder newInstance() {
            return new TagsBuilder();
        }

        /**
         * This method adds clientId parameter in order to build the tags instance.
         * @param clientId An String which contains the cliendId associated.
         */
        private TagsBuilder withClientIdTag(final String clientId) {
            tags.put(CLIENT_ID_TAG, clientId);
            return this;
        }

        /**
         * This method adds topic parameter in order to build the tags instance.
         * @param topic An String which contains the topic associated.
         */
        private TagsBuilder withTopicTag(final String topic) {
            tags.put(TOPIC_TAG, topic);
            return this;
        }

        /**
         * This method build the full tag instance object with the specified parameters.
         * @return A map object of tags.
         */
        private Map<String, String> build() {
            return tags;
        }
    }
}

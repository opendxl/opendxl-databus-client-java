/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer.metric;

import com.opendxl.databus.common.MetricName;

import java.util.Map;

/**
 *  ProducerMetric. This class is responsible for handling a Kafka producer metric.<br>
 *  ProducerMetric provides and abstraction to get a Kafka metric related to the producer.<br>
 *  ProducerMetric contains 3 attributes:<br>
 *      <ol>
 *      <li>metricName: an specific metric that maps with a Kafka metric.
 *      <li>clientId: is a String which contains the clientId associated to the producer.
 *      <li>value: the value associated to the metric.
 *      </ol>
 */

public class ProducerMetric {

    /**
     * A {@link MetricName} instance associated to the producer metric.
     */
    private MetricName metricName;

    /**
     * A String which contains the clientId associated.
     */
    private final String clientId;

    /**
     * The metric value.
     */
    private double value;

    /**
     * Create an instance of ProducerMetric
     *
     * @param metricName - An associated metric
     * @param value - The metric value.
     * @param clientId - The clientId associated to the metric.
     * @return An instance of ProducerMetric.
     */
    protected ProducerMetric(final MetricName metricName, final double value, String clientId) {
        this.metricName = metricName;
        this.value = value;
        this.clientId = clientId;
    }

    /**
     * Get the value of the metric.
     *
     * @return The metric's value.
     */
    public double getValue() {
        return value;
    }

    /**
     * Get the metric name.
     *
     * @return The metric's name.
     */
    public String getMetricName() {
        return metricName.name();
    }

    /**
     * Get the metric description.
     *
     * @return The metric's description.
     */
    public String getMetricDescription() {
        return metricName.description();
    }

    /**
     * Get the metric group.
     *
     * @return The metric's group.
     */
    public String getMetricGroup() {
        return metricName.group();
    }

    /**
     * Get the metric tags.
     *
     * @return The metric's tags.
     */
    public Map<String, String> getMetricTags() {
        return metricName.tags();
    }

    /**
     * Get the metric clientId.
     *
     * @return The metric's clientId.
     */
    public String getClientId() {
        return clientId;
    }
}

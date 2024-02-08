/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer.metric;

import com.opendxl.databus.common.MetricName;

import java.util.Map;

/**
 *  ConsumerMetric. This class is responsible for handling a Kafka consumer metric.<br>
 *  ConsumerMetric provides and abstraction to get a Kafka metric related to the consumer.<br>
 *  ConsumerMetric contains 3 attributes:<br>
 *      <ol>
 *      <li>metricName: an specific metric that maps with a Kafka metric.
 *      <li>value: the value associated to the metric.
 *      </ol>
 */

public class ConsumerMetric {

    /**
     * A {@link MetricName} instance associated to the consumer metric.
     */
    private MetricName metricName;

    /**
     * The metric value.
     */
    private double value;

    /**
     * Create an instance of ConsumerMetric
     *
     * @param metricName - An associated metric
     * @param value - The metric value.
     * @return An instance of ConsumerMetric.
     */
    protected ConsumerMetric(final MetricName metricName, final double value) {
        this.metricName = metricName;
        this.value = value;
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
     * Get the value of the metric.
     *
     * @return The metric's value.
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

}

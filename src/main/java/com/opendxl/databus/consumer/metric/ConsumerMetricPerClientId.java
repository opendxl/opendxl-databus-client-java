/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer.metric;

import java.util.Map;

public class ConsumerMetricPerClientId {

    /**
     * The client id.
     */
    private final String clientId;

    /**
     * The consumer metric associated.
     */
    private final ConsumerMetric consumerMetric;

    /**
     * ConsumerMetricPerClientId constructor.
     * @param clientId An associated clientId.
     * @param consumerMetric An associated consumer metric.
     */
    public ConsumerMetricPerClientId(final String clientId, final ConsumerMetric consumerMetric) {
        this.clientId = clientId;
        this.consumerMetric = consumerMetric;
    }

    /**
     * Gets the clientId of the metric.
     *
     * @return The metric's clientId.
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Gets the value of the metric.
     *
     * @return The metric's value.
     */
    public double getValue() {
        return consumerMetric.getValue();
    }

    /**
     * Gets the name of the metric.
     *
     * @return The metric's name.
     */
    public String getMetricName() {
        return consumerMetric.getMetricName();
    }

    /**
     * Gets the description of the metric.
     *
     * @return The metric's description.
     */
    public String getMetricDescription() {
        return consumerMetric.getMetricDescription();
    }

    /**
     * Gets the metric group of the metric.
     *
     * @return The metric's group.
     */
    public String getMetricGroup() {
        return consumerMetric.getMetricGroup();
    }

    /**
     * Gets the tags of the metric.
     *
     * @return The metric's tags.
     */
    public Map<String, String> getMetricTags() {
        return consumerMetric.getMetricTags();
    }

}


/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer.metric;

import java.util.Map;

public class ConsumerMetricPerClientId {

    private final String clientId;
    private final ConsumerMetric consumerMetric;

    public ConsumerMetricPerClientId(final String clientId, final ConsumerMetric consumerMetric) {
        this.clientId = clientId;
        this.consumerMetric = consumerMetric;
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


}


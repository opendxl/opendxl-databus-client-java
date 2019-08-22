/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer.metric;

import com.opendxl.databus.common.MetricName;

import java.util.Map;

public class ConsumerMetric {

    private MetricName metricName;

    private double value;

    protected ConsumerMetric(final MetricName metricName, final double value) {
        this.metricName = metricName;
        this.value = value;
    }

    public double getValue() {
        return value;
    }

    public String getMetricName() {
        return metricName.name();
    }

    public String getMetricDescription() {
        return metricName.description();
    }

    public String getMetricGroup() {
        return metricName.group();
    }

    public Map<String, String> getMetricTags() {
        return metricName.tags();
    }

}

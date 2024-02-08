/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.adapter;


import com.opendxl.databus.common.Metric;
import com.opendxl.databus.common.MetricName;

import java.util.HashMap;
import java.util.Map;

/**
 * Adapter for a Metric Name Map
 */
public final class MetricNameMapAdapter implements
        Adapter<Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric>,
                Map<MetricName, ? extends org.apache.kafka.common.Metric>> {

    /**
     * Adapter pattern implementation for a {@code Map<MetricName, Metric>} instance.
     * Adapts a {@code Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric>}
     * to a {@code Map<MetricName, ? extends org.apache.kafka.common.Metric>>} instance.
     *
     * @param sourceMetricNameMap a {@code Map<org.apache.kafka.common.MetricName,
     * ? extends org.apache.kafka.common.Metric>} sourceMetricNameMap.
     * @return a Map of {@link MetricName} as key and {@link Metric} as value.
     */
    @Override
    public Map<MetricName, ? extends org.apache.kafka.common.Metric>
    adapt(final Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric>
                  sourceMetricNameMap) {

        Map<MetricName, org.apache.kafka.common.Metric> targetMetricNameMap = new HashMap<>();

        sourceMetricNameMap.forEach((metricName, kafkaMetric) ->
                targetMetricNameMap.put(
                        new MetricName(metricName.name(),
                                metricName.group(),
                                metricName.description(),
                                metricName.tags()), kafkaMetric)
        );
        return targetMetricNameMap;
    }
}

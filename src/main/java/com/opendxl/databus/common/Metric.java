/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common;

/**
 * A Databus Metric
 */
public interface Metric {

    MetricName metricName();

    double value();

}

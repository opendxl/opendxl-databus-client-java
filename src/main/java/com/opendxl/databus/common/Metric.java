/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common;

/**
 * A Databus Metric
 */
public interface Metric {

    MetricName metricName();

    double value();

}

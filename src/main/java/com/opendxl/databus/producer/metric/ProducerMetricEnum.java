/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer.metric;

/**
 *  ProducerMetricEnum. This class is responsible to list and get a ProducerMetricEnum instance.<br>
 *  ProducerMetricEnum has two fields:<br>
 *      <ol>
 *      <li>name: the Kafka given name for the metric.
 *      <li>error: an error message if the metric is not able to be returned.
 *      </ol>
 */
public enum ProducerMetricEnum {
    // Producer Metrics per clientId

    /**
     * ProducerMetricEnum per clientId which gets the max batch size
     */
    RECORD_BATCH_SIZE_MAX("batch-size-max", "recordBatchSizeMaxMetric cannot be preformed: "),

    /**
     * ProducerMetricEnum per clientId which gets the average batch size
     */
    RECORD_BATCH_SIZE_AVG("batch-size-avg", "recordBatchSizeAvgMetric cannot be preformed: "),

    /**
     * ProducerMetricEnum per clientId which gets the total records sent
     */
    RECORD_SEND_TOTAL("record-send-total", "recordSendTotalMetric cannot be preformed: "),

    /**
     * ProducerMetricEnum per clientId which gets the average records sent
     */
    RECORD_SEND_RATE("record-send-rate", "recordSendRateMetric cannot be preformed: "),

    /**
     * ProducerMetricEnum per clientId which gets the average record size
     */
    RECORD_SIZE_AVG("record-size-avg", "recordSizeAvgMetric cannot be preformed: "),

    /**
     * ProducerMetricEnum per clientId which gets the max record size
     */
    RECORD_SIZE_MAX("record-size-max", "recordSizeMaxMetric cannot be preformed: "),

    /**
     * ProducerMetricEnum per clientId which gets the total error
     */
    RECORD_ERROR_TOTAL("record-error-total", "recordErrorTotalMetric cannot be preformed: "),

    /**
     * ProducerMetricEnum per clientId which gets the average error
     */
    RECORD_ERROR_RATE("record-error-rate", "recordErrorRateMetric cannot be preformed: "),


    // Producer Metrics per clientId and Topic

    /**
     * ProducerMetricEnum per clientId and topic which gets the total records sent
     */
    RECORD_SEND_TOTAL_PER_TOPIC("record-send-total", "recordSendTotalPerTopicMetric cannot be preformed: "),

    /**
     * ProducerMetricEnum per clientId and topic which gets the average records sent
     */
    RECORD_SEND_RATE_PER_TOPIC("record-send-rate", "recordSendRatePerTopicMetric cannot be preformed: "),

    /**
     * ProducerMetricEnum per clientId and topic which gets the total errors
     */
    RECORD_ERROR_TOTAL_PER_TOPIC("record-error-total", "recordErrorTotalPerTopicMetric "
            + "cannot be preformed: "),

    /**
     * ProducerMetricEnum per clientId and topic which gets the average error
     */
    RECORD_ERROR_RATE_PER_TOPIC("record-error-rate", "recordErrorRatePerTopicMetric cannot be preformed: "),

    /**
     * ProducerMetricEnum per clientId and topic which gets the total bytes sent
     */
    RECORD_BYTE_TOTAL_PER_TOPIC("byte-total", "recordByteTotalPerTopicMetric cannot be preformed: "),

    /**
     * ProducerMetricEnum per clientId and topic which gets the average bytes sent
     */
    RECORD_BYTE_RATE_PER_TOPIC("byte-rate", "recordByteRatePerTopicMetric cannot be preformed: ");

    /**
     * The Kafka given name for the metric.
    */
    private String name;

    /**
     * An error message if the metric is not able to be returned.
     */
    private String error;

    /**
     * The constructor to create a ProducerMetricEnum.
     */
    ProducerMetricEnum(final String name, final String error) {
        this.name = name;
        this.error = error;
    }

    /**
     * Gets the error message for the ProducerMetricEnum instance
     *
     * @return The Kafka given name of the ProducerEnum metric.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the error message for the ProducerMetricEnum instance
     *
     * @return The error message of the ProducerEnum metric if the metric will not able to be obtained.
     */
    public String getError() {
        return error;
    }

}

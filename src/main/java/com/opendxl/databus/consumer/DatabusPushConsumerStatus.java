/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

/**
 * This class tracks DatabusPushConsumer internal status {@link Status} and
 * let the user knows how is going on the internal message listener processor
 * {@link DatabusPushConsumerListenerResponse}
 */
public class DatabusPushConsumerStatus {

    /**
     * Tracks the exception thrown by a {@link DatabusPushConsumer}
     */
    private Exception exception = null;

    /**
     * Tracks the {@link DatabusPushConsumerListener} status
     */
    private DatabusPushConsumerListenerResponse listenerResult = null;

    /**
     * Tracks the {@link DatabusPushConsumer} status
     */
    private Status status = Status.STOPPED;

    /**
     * DatabusPushConsumer Status
     */
    public enum Status {
        STOPPED,
        PROCESSING
    }

    private DatabusPushConsumerStatus() {

    }

    public Exception getException() {
        return exception;
    }

    public DatabusPushConsumerListenerResponse getListenerResult() {
        return listenerResult;
    }

    public Status getStatus() {
        return status;
    }


    /**
     * DatabusPushConsumerStatus builder
     */
    static class Builder {
        private Exception exception = null;
        private DatabusPushConsumerListenerResponse listenerResult = null;
        private Status status = Status.STOPPED;

        /**
         *
         * @param exception instance
         * @return DatabusPushConsumerStatus.Builder instance
         */
        public DatabusPushConsumerStatus.Builder withException(final Exception exception) {
            this.exception = exception;
            return this;
        }

        /**
         *
         * @param listenerResult DatabusPushConsumerListenerResponse instance
         * @return DatabusPushConsumerStatus.Builder instance
         */
        public DatabusPushConsumerStatus.Builder
        withListenerResult(final DatabusPushConsumerListenerResponse listenerResult) {
            this.listenerResult = listenerResult;
            return this;
        }

        /**
         *
         * @param status DatabusPushConsumer status
         * @return DatabusPushConsumerStatus.Builder instance
         */
        public DatabusPushConsumerStatus.Builder withStatus(final Status status) {
            this.status = status;
            return this;
        }

        public DatabusPushConsumerStatus build() {
            DatabusPushConsumerStatus databusPushConsumerStatus
                    = new DatabusPushConsumerStatus();
            databusPushConsumerStatus.exception = this.exception;
            databusPushConsumerStatus.listenerResult = this.listenerResult;
            databusPushConsumerStatus.status = this.status;
            return databusPushConsumerStatus;
        }

    }

}

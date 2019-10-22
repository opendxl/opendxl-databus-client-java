package com.opendxl.databus.consumer;

public class DatabusPushConsumerListenerStatus {

    private Exception exception = null;
    private DatabusPushConsumerListenerResponse listenerResult = null;
    private Status status = Status.STOPPED;
    public enum Status {
        STOPPED,
        PROCESSING
    }

    private DatabusPushConsumerListenerStatus() {

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


    static class Builder {
        private Exception exception = null;
        private DatabusPushConsumerListenerResponse listenerResult = null;
        private Status status = Status.STOPPED;

        public DatabusPushConsumerListenerStatus.Builder withException(final Exception exception) {
            this.exception = exception;
            return this;
        }

        public DatabusPushConsumerListenerStatus.Builder
        withListenerResult(final DatabusPushConsumerListenerResponse listenerResult) {
            this.listenerResult = listenerResult;
            return this;
        }

        public DatabusPushConsumerListenerStatus.Builder withStatus(final Status status) {
            this.status = status;
            return this;
        }

        public DatabusPushConsumerListenerStatus build() {
            DatabusPushConsumerListenerStatus databusPushConsumerListenerStatus
                    = new DatabusPushConsumerListenerStatus();
            databusPushConsumerListenerStatus.exception = this.exception;
            databusPushConsumerListenerStatus.listenerResult = this.listenerResult;
            databusPushConsumerListenerStatus.status = this.status;
            return databusPushConsumerListenerStatus;
        }

    }

}

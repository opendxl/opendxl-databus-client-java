package com.opendxl.databus.consumer;

public enum DatabusPushConsumerListenerResponse {
    CONTINUE_AND_COMMIT,
    RETRY,
    STOP_AND_COMMIT,
    STOP_NO_COMMIT

}

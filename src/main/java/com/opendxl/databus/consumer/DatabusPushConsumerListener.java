package com.opendxl.databus.consumer;

public interface DatabusPushConsumerListener {

    DatabusPushConsumerListenerResponse onConsume(final ConsumerRecords records);

}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common;


/**
 * The metadata for a record that has been acknowledged by the server
 * An object of this class is built only for Databus as a result
 * of a {@link com.opendxl.databus.producer.Producer
 * #send( com.opendxl.streaming.nativeclient.producer.ProducerRecord,
 * com.opendxl.streaming.nativeclient.producer.Callback)}  method
 */
public class RecordMetadata {

    private final org.apache.kafka.clients.producer.RecordMetadata recordMetadata;

    /**
     *
     * @param recordMetadata record metadata
     */
    public RecordMetadata(final org.apache.kafka.clients.producer.RecordMetadata recordMetadata) {
        if (recordMetadata == null) {
            throw new IllegalArgumentException("recordMetadata cannot be null");
        }
        this.recordMetadata = recordMetadata;
    }

    public long offset() {

        return  recordMetadata.offset();
    }

    public String topic() {


        return recordMetadata.topic();
    }

    public int partition() {

        return recordMetadata.partition();
    }
}

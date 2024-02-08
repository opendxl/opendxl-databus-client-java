/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer;


import com.opendxl.databus.common.RecordMetadata;

/**
 * A callback class which can be implemented to provide asynchronous handling of request completion. This callback
 * will generally execute in the background I/O thread so it should be fast.
 */
public interface Callback {

    /**
     * A callback method which the user can implement to provide asynchronous handling of request completion.
     * This method will be called when a record sent to the server has been acknowledged. Exactly one of the arguments
     * will be non-null.
     * @param metadata The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *        occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     *                  Possible thrown exceptions include:
     *
     *                  Non-Retriable exceptions (fatal, the message will never be sent):
     *
     *                  InvalidTopicException
     *                  OffsetMetadataTooLargeException
     *                  RecordBatchTooLargeException
     *                  RecordTooLargeException
     *                  UnknownServerException
     *
     *                  Retriable exceptions (transient, may be covered by increasing #.retries):
     *
     *                  CorruptRecordException
     *                  InvalidMetadataException
     *                  NotEnoughReplicasAfterAppendException
     *                  NotEnoughReplicasException
     *                  OffsetOutOfRangeException
     *                  TimeoutException
     *                  UnknownTopicOrPartitionException
     */
    void onCompletion(RecordMetadata metadata, Exception exception);

}

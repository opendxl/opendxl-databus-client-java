/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.common.TopicPartition;

import java.util.Map;

/**
 * A callback interface that the user can implement to trigger custom actions when a commit request completes.
 * The callback
 * may be executed in any thread calling {@link Consumer#poll(long) poll()}.
 */
public interface OffsetCommitCallback {
    /**
     * A callback method the user can implement to provide asynchronous handling of commit request completion.
     * This method will be called when the commit request sent to the server has been acknowledged.
     *
     * @param offsets A map of the offsets and associated metadata that this callback applies to
     * @param exception The exception thrown during processing of the request, or null if the commit completed
     *                  successfully
     *
     * @throws DatabusClientRuntimeException If send method fails.
     * The original cause could be any of
     * these exceptions:
     *                                       <p> CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management
     *             with {@link Consumer#subscribe(List)} },
     *             or if there is an active group with the same groupId which is using group management.
     *                                       <p> WakeupException if {@link Consumer#wakeup()} is called before or
     *                                       while this
     *             function is called
     *                                       <p> AuthorizationException if not authorized to the topic or to the
     *             configured groupId
     *                                       <p> KafkaException for any other unrecoverable errors (e.g. if offset
     *                                       metadata
     *             is too large or if the committed offset is invalid).
     */
    void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception);
}

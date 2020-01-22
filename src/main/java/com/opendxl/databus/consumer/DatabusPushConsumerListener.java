/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

/**
 * This is the interface that SDK Databus client has to implement to receive and process records
 * when consume them by using a {@link DatabusPushConsumer} instance. {@link DatabusPushConsumer#pushAsync()}
 * main loop read messages from an already-subscribed topic and send them
 * to {@link DatabusPushConsumerListener#onConsume(ConsumerRecords)} method.
 * It will process messages and will return a {@link DatabusPushConsumerListenerResponse}.
 * According to it, DatabusPushConsumer will act accordingly.
 * <p>
 * {@link DatabusPushConsumerListenerResponse#CONTINUE_AND_COMMIT }
 * {@link DatabusPushConsumerListenerResponse#RETRY }
 * {@link DatabusPushConsumerListenerResponse#STOP_AND_COMMIT }
 * {@link DatabusPushConsumerListenerResponse#STOP_NO_COMMIT }
 * </p>
 *
 *
 *
 */
public interface DatabusPushConsumerListener<P> {

    /**
     * It is called by {@link DatabusPushConsumer} main loop in a separated thread.  It receives records read from
     * Databus and return a {@link DatabusPushConsumerListenerResponse} enum value.
     * The return value let {@link DatabusPushConsumer} main loop know which action take.
     * <p>
     * {@link DatabusPushConsumerListenerResponse#CONTINUE_AND_COMMIT }
     * states to commit records and continue from last topic-partition offset position
     * </p>
     * <p>
     * {@link DatabusPushConsumerListenerResponse#RETRY}
     * states do not commit and to get the same records already sent .
     * </p>
     * <p>
     * {@link DatabusPushConsumerListenerResponse#STOP_AND_COMMIT}
     * states commit records and stop {@link DatabusPushConsumer} main loop.
     * </p>
     * <p>
     * {@link DatabusPushConsumerListenerResponse#STOP_NO_COMMIT}
     * states do not commit records and stop {@link DatabusPushConsumer} main loop.
     * </p>
     * <p>
     * All exceptions should be managed inside this method. If a unexpected exception is thrown,
     * it will be logged by {@link DatabusPushConsumer} main loop, but it won't be rethrow.
     * </p>
     *
     *
     * @param records pushed from Databus
     * @return A {@link DatabusPushConsumerListenerResponse} enum value response to let {@link DatabusPushConsumer}
     * know how to proceed.
     */
    DatabusPushConsumerListenerResponse onConsume(ConsumerRecords<P> records);

}

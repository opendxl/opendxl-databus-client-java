package com.opendxl.databus.consumer;

import java.time.Duration;

/**
 * This is the interface that SDK Databus client has to implement to receive and process records
 * when consume them by using a {@link DatabusPushConsumer} instance. {@link DatabusPushConsumer#pushAsync(Duration)}
 * main loop read messages from a already-subscribed topic and send them
 * to {@link DatabusPushConsumerListener#onConsume(ConsumerRecords)} method.
 * It process messages and returns a {@link DatabusPushConsumerListenerResponse}.
 * According this returned value, DatabusPushConsumer will act will act accordingly.
 * <p>
 * {@link DatabusPushConsumerListenerResponse#CONTINUE_AND_COMMIT }
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
     * @param records
     * @return
     */
    DatabusPushConsumerListenerResponse onConsume(ConsumerRecords<P> records);

}

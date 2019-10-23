/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class allows SDK Databus client to know the status of a {@link DatabusPushConsumer} instance
 */
public class DatabusPushConsumerFuture implements Future<DatabusPushConsumerListenerStatus> {

    /**
     * A Listener Status instance
     */
    private final AtomicReference<DatabusPushConsumerListenerStatus> databusPushConsumerListenerStatus
            = new AtomicReference<>();

    /**
     * A latch to signal that the DatabusPushConsumer has finished
     */
    private final CountDownLatch countDownLatch;


    /**
     * Constructor
     *
     * @param databusPushConsumerListenerStatus Listener Status instance
     * @param countDownLatch A latch to signal that the DatabusPushConsumer has finished
     */
    public DatabusPushConsumerFuture(final DatabusPushConsumerListenerStatus databusPushConsumerListenerStatus,
                                     final CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
        this.databusPushConsumerListenerStatus.set(databusPushConsumerListenerStatus);
    }


    /**
     *  Set the listener status
     * @param databusPushConsumerListenerStatus listener status
     */
    public void setDatabusPushConsumerListenerStatus(final DatabusPushConsumerListenerStatus
                                                             databusPushConsumerListenerStatus) {

        this.databusPushConsumerListenerStatus.set(databusPushConsumerListenerStatus);
    }


    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }


    @Override
    public boolean isDone() {
        return countDownLatch.getCount() == 0;
    }

    @Override
    public DatabusPushConsumerListenerStatus get() throws InterruptedException, ExecutionException,
            CancellationException {
        countDownLatch.await();
        return databusPushConsumerListenerStatus.get();
    }

    @Override
    public DatabusPushConsumerListenerStatus get(final long timeout, final TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        try {
            countDownLatch.await(timeout, unit);

        } catch (Exception e) {
                e.printStackTrace();

        }
        return databusPushConsumerListenerStatus.get();
    }
}


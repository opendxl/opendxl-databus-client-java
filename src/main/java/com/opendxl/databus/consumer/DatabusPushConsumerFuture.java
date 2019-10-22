package com.opendxl.databus.consumer;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class DatabusPushConsumerFuture implements Future<DatabusPushConsumerListenerStatus> {

    private final AtomicReference<DatabusPushConsumerListenerStatus> databusPushConsumerListenerStatus
            = new AtomicReference<>();
    private final CountDownLatch countDownLatch;



    private AtomicBoolean isDone = new AtomicBoolean(false);

    public DatabusPushConsumerFuture(final DatabusPushConsumerListenerStatus databusPushConsumerListenerStatus,
                                     final CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
        this.databusPushConsumerListenerStatus.set(databusPushConsumerListenerStatus);
    }

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
        countDownLatch.await(timeout, unit);
        return databusPushConsumerListenerStatus.get();
    }
}


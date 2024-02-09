/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.util;

import com.opendxl.databus.producer.Callback;
import com.opendxl.databus.common.RecordMetadata;

import java.util.concurrent.CountDownLatch;

public class TestCallback implements Callback {
    private boolean isSuccess = false;
    private CountDownLatch countDownLatch;

    public TestCallback(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        countDownLatch.countDown();
        if (exception != null) {
            return;
        }
        isSuccess = true;
    }
}

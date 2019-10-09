package com.opendxl.databus.consumer;

import java.io.Closeable;
import java.io.IOException;

/**
 * Implements a Databus Consumer with a push model
 *
 * @param <P> Message payload type
 *
 */
public final class DatabusPushConsumer<P> extends Consumer<P> implements Closeable {


    @Override
    public void close()  throws IOException {
        System.out.println("Close");

    }
}

package com.opendxl.databus.consumer;

import com.opendxl.databus.credential.Credential;
import com.opendxl.databus.serialization.Deserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Implements a Databus Consumer with a push model
 *
 * @param <P> Message payload type
 *
 */
public final class DatabusPushConsumer<P> extends DatabusConsumer<P> implements Closeable {


    private final DatabusPushConsumerListener consumerListener;

    ExecutorService executor = Executors.newFixedThreadPool(1);


    public DatabusPushConsumer(final Map<String, Object> configs,
                               final Deserializer<P> messageDeserializer,
                               final DatabusPushConsumerListener consumerListener) {
        super(configs, messageDeserializer);
        this.consumerListener = consumerListener;
    }

    public DatabusPushConsumer(final Map<String, Object> configs,
                               final Deserializer<P> messageDeserializer,
                               final DatabusPushConsumerListener consumerListener,
                               final Credential credential) {
        super(configs, messageDeserializer, credential);
        this.consumerListener = consumerListener;
    }

    public DatabusPushConsumer(final Properties properties,
                               final Deserializer<P> messageDeserializer,
                               final DatabusPushConsumerListener consumerListener) {
        super(properties, messageDeserializer);
        this.consumerListener = consumerListener;

    }

    public DatabusPushConsumer(final Properties properties,
                               final Deserializer<P> messageDeserializer,
                               final DatabusPushConsumerListener consumerListener,
                               final Credential credential) {
        super(properties, messageDeserializer, credential);
        this.consumerListener = consumerListener;
    }


    @Override
    public ConsumerRecords poll(final Duration timeout) {

        ConsumerRecords records = super.poll(timeout);

        if(records.count() > 0) {
            super.pause(assignment());
            Callable<DatabusPushConsumerListenerResponse> backgroundTask = () -> consumerListener.onConsume(records);
            Future<DatabusPushConsumerListenerResponse> future = executor.submit(backgroundTask);

            while(!future.isDone()) {
                super.poll(0);
            }


            try {
                DatabusPushConsumerListenerResponse onConsumeResponse = future.get();

            } catch (InterruptedException e) {
                //TODO:
            } catch (ExecutionException e) {
                //TODO:
            } finally {
                super.resume();
            }


        }
        return null;

    }

    @Override
    public void close()  throws IOException {
        System.out.println("Close");

    }



}


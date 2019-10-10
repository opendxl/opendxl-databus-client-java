package com.opendxl.databus.consumer;

import com.opendxl.databus.common.TopicPartition;
import com.opendxl.databus.credential.Credential;
import com.opendxl.databus.serialization.Deserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
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

        boolean stopRequested = false;
        while(!stopRequested) {

            final Map<TopicPartition, Long> lastPositionPerTopicPartition = new HashMap();

            for(TopicPartition tp : assignment()) {
                lastPositionPerTopicPartition.put(tp, position(tp));
            }

            ConsumerRecords records = super.poll(timeout);

            if(records.count() > 0) {

                pause(assignment());

                final Callable<DatabusPushConsumerListenerResponse> backgroundTask
                        = () -> consumerListener.onConsume(records);
                final Future<DatabusPushConsumerListenerResponse> future = executor.submit(backgroundTask);

                try {
                    while(!future.isDone()) {
                        super.poll(Duration.ofMillis(1000));
                    }

                    DatabusPushConsumerListenerResponse onConsumeResponse = future.get();
                    switch(onConsumeResponse) {
                        case STOP_AND_COMMIT:
                            commitSync();
                            stopRequested = true;
                            break;
                        case STOP_NO_COMMIT:
                            stopRequested = true;
                            break;
                        case RETRY:
                            for(TopicPartition tp : assignment()) {
                                if(lastPositionPerTopicPartition.containsKey(tp)) {
                                    Long position= lastPositionPerTopicPartition.get(tp);
                                    seek(tp, position);
                                }
                            }
                            break;
                        case CONTINUE_AND_COMMIT:
                        default:
                            commitSync();
                            break;
                    }

                } catch (InterruptedException e) {
                    //TODO:
                } catch (ExecutionException e) {
                    //TODO:
                } finally {
                    resume(assignment());
                }
            }
        }


        return null;

    }

    @Override
    public void close()  throws IOException {
        System.out.println("Close");

    }



}


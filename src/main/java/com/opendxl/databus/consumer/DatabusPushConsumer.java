/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

import com.opendxl.databus.common.TopicPartition;
import com.opendxl.databus.credential.Credential;
import com.opendxl.databus.entities.TierStorage;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.serialization.Deserializer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Extends a {@link DatabusConsumer} to replace poll model for push message model.
 * {@link DatabusPushConsumer#pushAsync()} reads messages from an
 * already-subscribed topic and push them to a {@link DatabusPushConsumerListener} listener
 * instance implemented by the SDK Databus client.
 * The listener receives messages read from Databus and it should implement some logic to process them.
 * The listener returns a {@link DatabusPushConsumerListenerResponse} enum value to let Push Databus Consumer known
 * which action it should take.
 *
 * @param <P> Message payload type
 */
public final class DatabusPushConsumer<P> extends DatabusConsumer<P> implements Closeable {

    /**
     * Poll timeout default value
     */
    private static final long POLL_TIMEOUT_MS = 1000;

    /**
     * The listener implemented by the Databus client to process records
     */
    private final DatabusPushConsumerListener consumerListener;

    /**
     * Logger instance
     */
    private static final Logger LOG = LoggerFactory.getLogger(DatabusPushConsumer.class);

    /**
     * An executor to spawn the listener thread in async way
     */
    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    /**
     * A future instance to control the listener thread
     */
    private Future<DatabusPushConsumerListenerResponse> listenerFuture;

    /**
     * it states if the main loop should continue or stop
     */
    private AtomicBoolean stopRequested = new AtomicBoolean(false);

    /**
     * An executor to spawn the main loop thread in async way.
     */
    private ExecutorService pushAsyncExecutor = null;

    /**
     * An future instance to control the main loop
     */
    private DatabusPushConsumerFuture databusPushConsumerFuture = null;

    /**
     * A latch to signal that the main loop has finished.
     */
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * An boolean to signal if pause operation has to be refreshed
     */
    private AtomicBoolean refreshPause = new AtomicBoolean(false);

    /**
     * Constructor
     *
     * @param configs             consumer configuration
     * @param messageDeserializer consumer message deserializer
     * @param consumerListener    consumer listener
     */
    public DatabusPushConsumer(final Map<String, Object> configs,
                               final Deserializer<P> messageDeserializer,
                               final DatabusPushConsumerListener consumerListener) {
        super(configs, messageDeserializer);
        this.consumerListener = consumerListener;
    }

    /**
     * Constructor
     *
     * @param configs             consumer configuration
     * @param messageDeserializer consumer message deserializer
     * @param consumerListener    consumer listener
     * @param tierStorage Tier storage
     */
    public DatabusPushConsumer(final Map<String, Object> configs,
                               final Deserializer<P> messageDeserializer,
                               final DatabusPushConsumerListener consumerListener,
                               final TierStorage tierStorage) {
        super(configs, messageDeserializer, null, tierStorage);
        this.consumerListener = consumerListener;
    }

    /**
     * @param configs             consumer configuration
     * @param messageDeserializer consumer message deserializer
     * @param consumerListener    consumer listener
     * @param credential          credential to get access to Databus in case security is enabled
     */
    public DatabusPushConsumer(final Map<String, Object> configs,
                               final Deserializer<P> messageDeserializer,
                               final DatabusPushConsumerListener consumerListener,
                               final Credential credential) {
        super(configs, messageDeserializer, credential, null);
        this.consumerListener = consumerListener;
    }

    /**
     * @param configs             consumer configuration
     * @param messageDeserializer consumer message deserializer
     * @param consumerListener    consumer listener
     * @param credential          credential to get access to Databus in case security is enabled
     * @param tierStorage Tier storage
     */
    public DatabusPushConsumer(final Map<String, Object> configs,
                               final Deserializer<P> messageDeserializer,
                               final DatabusPushConsumerListener consumerListener,
                               final Credential credential,
                               final TierStorage tierStorage) {
        super(configs, messageDeserializer, credential, tierStorage);
        this.consumerListener = consumerListener;
    }

    /**
     * @param properties          consumer configuration
     * @param messageDeserializer consumer message deserializer
     * @param consumerListener    consumer listener
     */
    public DatabusPushConsumer(final Properties properties,
                               final Deserializer<P> messageDeserializer,
                               final DatabusPushConsumerListener consumerListener) {
        super(properties, messageDeserializer);
        this.consumerListener = consumerListener;

    }

    /**
     * @param properties          consumer configuration
     * @param messageDeserializer consumer message deserializer
     * @param consumerListener    consumer listener
     * @param tierStorage Tier storage
     */
    public DatabusPushConsumer(final Properties properties,
                               final Deserializer<P> messageDeserializer,
                               final DatabusPushConsumerListener consumerListener,
                               final TierStorage tierStorage) {
        super(properties, messageDeserializer,  null, tierStorage);
        this.consumerListener = consumerListener;

    }

    /**
     * @param properties          consumer configuration
     * @param messageDeserializer consumer message deserializer
     * @param consumerListener    consumer listener
     * @param credential          credential to get access to Databus in case security is enabled
     */
    public DatabusPushConsumer(final Properties properties,
                               final Deserializer<P> messageDeserializer,
                               final DatabusPushConsumerListener consumerListener,
                               final Credential credential) {
        super(properties, messageDeserializer, credential, null);
        this.consumerListener = consumerListener;
    }

    /**
     * @param properties          consumer configuration
     * @param messageDeserializer consumer message deserializer
     * @param consumerListener    consumer listener
     * @param credential          credential to get access to Databus in case security is enabled
     * @param tierStorage Tier storage
     */
    public DatabusPushConsumer(final Properties properties,
                               final Deserializer<P> messageDeserializer,
                               final DatabusPushConsumerListener consumerListener,
                               final Credential credential,
                               final TierStorage tierStorage) {
        super(properties, messageDeserializer, credential, tierStorage);
        this.consumerListener = consumerListener;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(final Map<String, List<String>> groupTopics) {
        super.subscribe(groupTopics, new PushConsumerRebalanceListener(null));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(final Map<String, List<String>> groupTopics,
                          final ConsumerRebalanceListener consumerRebalanceListener) {
        super.subscribe(groupTopics, new PushConsumerRebalanceListener(consumerRebalanceListener));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(final List<String> topics,
                          final ConsumerRebalanceListener consumerRebalanceListener) {
        super.subscribe(topics, new PushConsumerRebalanceListener(consumerRebalanceListener));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(final List<String> topics) {
        super.subscribe(topics, new PushConsumerRebalanceListener(null));
    }

    private class PushConsumerRebalanceListener implements ConsumerRebalanceListener {

        private final ConsumerRebalanceListener customerListener;

        PushConsumerRebalanceListener(final ConsumerRebalanceListener customerListener) {
            this.customerListener = Optional.ofNullable(customerListener).orElse(new NoOpConsumerRebalanceListener());

        }

        @Override
        public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
            customerListener.onPartitionsRevoked(partitions);

        }

        @Override
        public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
            refreshPause.set(true);
            customerListener.onPartitionsAssigned(partitions);

        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerRecords poll(final Duration timeout) {
        if (stopRequested.get()) {
            throw new DatabusClientRuntimeException("poll method cannot be performed because "
                    + "DatabusPushConsumer is closed.", DatabusPushConsumer.class);
        }
        if (databusPushConsumerFuture != null) {
            throw new DatabusClientRuntimeException("poll cannot be performed because a pushAsync is already working.",
                    DatabusPushConsumer.class);
        }
        return super.poll(timeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerRecords poll(final long timeout) {
        if (stopRequested.get()) {
            throw new DatabusClientRuntimeException("poll cannot be performed because DatabusPushConsumer is closed.",
                    DatabusPushConsumer.class);
        }
        if (databusPushConsumerFuture != null) {
            throw new DatabusClientRuntimeException("poll cannot be performed because a pushAsync is already working.",
                    DatabusPushConsumer.class);
        }
        return super.poll(timeout);
    }

    /**
     * Reads messages from Databus and push them to {@link DatabusPushConsumerListener} instance which was passed
     * in the constructor
     *
     * @param timeout The time, in milliseconds, spent waiting in poll if data is not available. If 0, returns
     *                immediately with any records that are available now. Must not be negative.
     * @throws DatabusClientRuntimeException if {@link DatabusPushConsumer}is closed.
     * @return Future to monitoring the message processing
     */
    public DatabusPushConsumerFuture pushAsync(final Duration timeout) {

        if (stopRequested.get()) {
            throw new DatabusClientRuntimeException("pushAsync cannot be performed because DatabusPushConsumer is "
                    + "closed.", DatabusPushConsumer.class);
        }

        // If a future was already created and returned (e.g. pushAsync was already called),
        // it will return the same future to prevent to spawn a new thread and listener.
        // It allows user to call pushAsync just once
        if (databusPushConsumerFuture != null) {
            return databusPushConsumerFuture;
        }

        final DatabusPushConsumerStatus databusPushConsumerStatus
                = new DatabusPushConsumerStatus.Builder().build();
        databusPushConsumerFuture = new DatabusPushConsumerFuture(databusPushConsumerStatus, countDownLatch);

        pushAsyncExecutor = Executors.newFixedThreadPool(1);
        Runnable pushRecordTask = () -> push(databusPushConsumerFuture, timeout);
        pushAsyncExecutor.submit(pushRecordTask);
        return databusPushConsumerFuture;
    }

    /**
     * Reads messages from Databus and push them to {@link DatabusPushConsumerListener} instance which was passed
     * in the constructor
     *
     * @return Future to monitoring the message processing
     */
    public DatabusPushConsumerFuture pushAsync() {
        return pushAsync(Duration.ofMillis(POLL_TIMEOUT_MS));
    }

    /**
     * It implements the read-push main loop. The loop is controlled by the listener return value.
     * According to {@link DatabusPushConsumerListenerResponse} listener return value, the main loop will continue
     * or stop reading messages. Messages read from Databus are sent to listener in an separated and parallel thread.
     * While the listener is working on messages received from Databus, the Push Consumer pauses topic-partitions
     * assigned to it and sends heartbeats the Kafka broker in background.
     * Once the listener has finished, it should returns a
     * {@link DatabusPushConsumerListenerResponse} value or an Exception in case something was wrong.
     * Then the main loop will act accordingly. The main loop keep a {@code databusPushConsumerFuture} future argument
     *
     * @param databusPushConsumerFuture a future instance shared which is updated to let the
     *                                  SDK Databus client know about the process status.
     * @param timeout                   The time, in milliseconds, spent waiting in poll if data is not available.
     *                                  If 0, returns
     *                                  immediately with any records that are available now. Must not be negative.
     */
    private void push(final DatabusPushConsumerFuture databusPushConsumerFuture,
                      final Duration timeout) {

        LOG.info("Consumer " + super.getClientId() + " start");
        try {

            // Main loop. It keeps the reading-pushing mechanism running until either a close() was called or
            // the listener asks to stop or the listener throws an exception.
            while (!stopRequested.get()) {

                // It stores the current consumer position based on the partitions assigned.
                // This is the offset position per topicPartition before polling messages from Databus.  Position
                // will be used to rewind the consumer in case the listener returns RETRY enum value.
                final Set<TopicPartition> assignment = super.assignment();
                final Map<TopicPartition, Long> lastKnownPositionPerTopicPartition =
                        getCurrentConsumerPosition(assignment);

                // poll records from Databus
                ConsumerRecords records = super.poll(timeout);
                LOG.info("Consumer " + super.getClientId() + " number of records read: " + records.count());

                if (records.count() > 0) {
                    super.pause(assignment);
                    LOG.info("Consumer " + super.getClientId() + " is paused");
                } else {
                    continue;
                }

                // It calls the listener in a separated thread.
                listenerFuture = runListenerAsync(databusPushConsumerFuture, records);

                DatabusPushConsumerListenerResponse onConsumeResponse ;
                boolean listenerIsFinished = false;

                // This loop waits for the listener the result and it also manages exceptions that listener might throw.
                while (!listenerIsFinished && !stopRequested.get()) {
                    try {
                        onConsumeResponse = listenerFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                        listenerIsFinished = true;
                        switch (onConsumeResponse) {
                            case STOP_AND_COMMIT:
                                stopRequested.set(true);
                                super.commitSync();
                                LOG.info("Consumer " + getClientId() + " listener returns "
                                        + onConsumeResponse.toString());
                                break;
                            case STOP_NO_COMMIT:
                                stopRequested.set(true);
                                LOG.info("Consumer " + getClientId() + " listener returns "
                                        + onConsumeResponse.toString());
                                break;
                            case RETRY:
                                seek(lastKnownPositionPerTopicPartition);
                                LOG.info("Consumer " + getClientId() + " listener returns "
                                        + onConsumeResponse.toString());
                                break;
                            case CONTINUE_AND_COMMIT:
                            default:
                                super.commitSync();
                                LOG.info("Consumer " + getClientId() + " listener returns "
                                        + onConsumeResponse.toString());
                                break;
                        }
                        resume(assignment());
                        databusPushConsumerFuture
                                .setDatabusPushConsumerListenerStatus(
                                        new DatabusPushConsumerStatus.Builder()
                                                .withListenerResult(onConsumeResponse)
                                                .build());

                        LOG.info("Consumer " + super.getClientId() + " is resumed");
                    } catch (TimeoutException e) {
                        // refreshPause == true means a rebalance was performed and partitions might be reassigned.
                        // Then, in order to avoid reading messages and just sends the heartbeat when poll(),
                        // a pause() method has to be invoked with the updated partitions assignment.
                        if (refreshPause.get()) {
                            refreshPause.set(false);
                            pause(assignment());
                        }
                        // TimeoutException means that listener is still working.
                        // So, a poll is performed to heartbeat Databus
                        super.poll(Duration.ofMillis(0));
                        LOG.info("Consumer " + super.getClientId() + " sends heartbeat to coordinator. "
                                + "The listener continue processing messages...");
                    } catch (ExecutionException | InterruptedException e) {
                        LOG.error("Consumer " + super.getClientId()
                                + " listener throws an Exception while it was working: " + e.getMessage(), e);
                        databusPushConsumerFuture
                                .setDatabusPushConsumerListenerStatus(
                                        new DatabusPushConsumerStatus.Builder()
                                                .withException(e)
                                                .build());
                        stopRequested.set(true);
                        break;
                    } catch (CancellationException e) {
                        LOG.warn("Consumer " + super.getClientId() + " was cancelled: " + e.getMessage(), e);
                        databusPushConsumerFuture
                                .setDatabusPushConsumerListenerStatus(
                                        new DatabusPushConsumerStatus.Builder().build());
                        stopRequested.set(true);
                        break;
                    } catch (Exception e) {
                        LOG.warn("Consumer " + super.getClientId() + " exception: " + e.getMessage(), e);
                        databusPushConsumerFuture
                                .setDatabusPushConsumerListenerStatus(
                                        new DatabusPushConsumerStatus.Builder()
                                                .withException(e)
                                                .build());
                        stopRequested.set(true);
                        break;

                    }
                }
            }
        } catch (DatabusClientRuntimeException e) {
            // Prevents to logging  if a WakeupException was thrown because of calling close
            if ((e.getCause() instanceof WakeupException)  && stopRequested.get()) {
                LOG.error("Consumer " + super.getClientId() + "Error: " + e.getMessage(), e);
            }
        } catch (Exception e) {
            LOG.error("Consumer " + super.getClientId() + "Error: " + e.getMessage(), e);
        } finally {
            countDownLatch.countDown();
            LOG.info("Consumer " + super.getClientId() + " end");
        }

    }

    /**
     * It sets Consumer position
     *
     * @param consumerPosition A Map with the offset position per topic-partition
     */
    private void seek(final Map<TopicPartition, Long> consumerPosition) {
        for (TopicPartition tp : super.assignment()) {
            if (consumerPosition.containsKey(tp)) {
                final Long position = consumerPosition.get(tp);
                super.seek(tp, position);
            }
        }
    }

    /**
     * It spawns a thread to execute the listener
     *
     * @param databusPushConsumerFuture The future that track the Pusg COnsumer
     * @param records records read from Databus
     * @return a Future instance to
     */
    private Future<DatabusPushConsumerListenerResponse> runListenerAsync(
            final DatabusPushConsumerFuture databusPushConsumerFuture,
            final ConsumerRecords records) {

        // launch the listener in a separate thread
        Future<DatabusPushConsumerListenerResponse> listenerTaskFuture = executor.submit(
                () -> consumerListener.onConsume(records));

        // Update the Push Consumer status
        databusPushConsumerFuture
                .setDatabusPushConsumerListenerStatus(
                        new DatabusPushConsumerStatus.Builder()
                                .withStatus(DatabusPushConsumerStatus.Status.PROCESSING)
                                .build());

        LOG.info("Consumer " + getClientId() + " Listener was called");
        return listenerTaskFuture;
    }


    /**
     * It stores the current consumer position based on the partitions assigned.
     * This is the offset position per topicPartition before polling messages from Databus.  Position
     * will be used to rewind the consumer in case the listener returns RETRY enum value.
     *
     * @return The map with the current offset position for each topic partition assigned to consumer
     */
    private Map<TopicPartition, Long> getCurrentConsumerPosition(final Set<TopicPartition> assignment) {
        final Map<TopicPartition, Long> lastKnownPositionPerTopicPartition = new HashMap(assignment.size());
        for (TopicPartition tp : assignment) {
            lastKnownPositionPerTopicPartition.put(tp, super.position(tp));
        }
        return lastKnownPositionPerTopicPartition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {

        try {
            stopRequested.set(true);
            wakeup();
            databusPushConsumerFuture = null;
            if (listenerFuture != null) {
                listenerFuture.cancel(true);
            }
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {

        } finally {
            executor.shutdownNow();
            if (pushAsyncExecutor != null) {
                pushAsyncExecutor.shutdownNow();
                pushAsyncExecutor = null;
            }
            super.close();
            LOG.info("Consumer " + super.getClientId() + " is closed");

        }
    }


}


/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer;

import com.opendxl.databus.common.RecordMetadata;
import com.opendxl.databus.common.internal.adapter.DatabusProducerRecordAdapter;
import com.opendxl.databus.common.internal.adapter.MessagePayloadAdapter;
import com.opendxl.databus.credential.Credential;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.entities.TierStorage;
import com.opendxl.databus.entities.TierStorageMetadata;
import com.opendxl.databus.entities.internal.DatabusMessage;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.serialization.Serializer;
import com.opendxl.databus.serialization.internal.MessageSerializer;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * It writes a Message to kafka and stores Payload and Header in a Tier Storage. The kafka message is used like
 * offsets control and to point to payload which is stored in the Tier Storage.
 *
 * @param <P> Payload's type,  tipically a byte[]
 */
public class DatabusTierStorageProducer<P> extends DatabusProducer<P> {

    /**
     * The logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(DatabusTierStorageProducer.class);

    /**
     * Used to save the message in a separated store
     */
    private TierStorage tierStorage;

    /**
     * Transform a user payload in a {@link DatabusMessage}
     */
    private MessagePayloadAdapter<P> messagePayloadAdapter;

    /**
     * Constructor
     *
     * @param configs Producer configuration
     * @param userSerializer user serializer
     * @param tierStorage tier storage
     */
    public DatabusTierStorageProducer(final Map<String, Object> configs, final Serializer<P> userSerializer,
                                      final TierStorage tierStorage) {
        this(configs, userSerializer, null, tierStorage);
    }


    /**
     * Constructor
     *
     * @param configs producer configuration
     * @param userSerializer user serializer
     * @param credential credentials
     * @param tierStorage tier storage
     */
    public DatabusTierStorageProducer(final Map<String, Object> configs, final Serializer<P> userSerializer,
                                      final Credential credential, final TierStorage tierStorage) {
        super(configs, userSerializer, credential);
        if (tierStorage == null) {
            throw new IllegalArgumentException("Tier Storage cannot be null");
        }
        validateConfiguration(configs);
        this.tierStorage = tierStorage;
        setFieldMembers(userSerializer);
        initTransactions();
    }


    /**
     * Constructor
     *
     * @param properties producer configuration
     * @param userSerializer user serializer
     * @param tierStorage tier storage
     */
    public DatabusTierStorageProducer(final Properties properties, final Serializer<P> userSerializer,
                                      final TierStorage tierStorage) {
        this(properties, userSerializer, null, tierStorage);
    }


    /**
     * Constructor
     *
     * @param properties producer configuration
     * @param userSerializer user serializer
     * @param credential credential
     * @param tierStorage tier storage
     */
    public DatabusTierStorageProducer(final Properties properties, final Serializer<P> userSerializer,
                                      final Credential credential, final TierStorage tierStorage) {

        super(properties, userSerializer, credential);
        if (tierStorage == null) {
            throw new IllegalArgumentException("Tier Storage cannot be null");
        }
        validateConfiguration(properties);
        this.tierStorage = tierStorage;
        setFieldMembers(userSerializer);
        initTransactions();
    }

    private void setFieldMembers(Serializer<P> userSerializer) {
        setKafkaValueSerializer(new MessageSerializer()); // The serializer used bu Kafka
        this.messagePayloadAdapter = new MessagePayloadAdapter<>(userSerializer);
        setDatabusProducerRecordAdapter(new DatabusProducerRecordAdapter<>(userSerializer));
    }

    private void validateConfiguration(final Map<String, Object> config) {
        Properties properties = new Properties();
        try {
            properties.putAll(config);
        } catch (Exception e) {
            throw new IllegalArgumentException("Producer configuration is invalid ERROR:" + e.getMessage());
        }
        validateConfiguration(properties);
    }

    private void validateConfiguration(final Properties config) {
        if (config.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG) == null) {
            throw new IllegalArgumentException("Transaction Id cannot be null or empty");
        }
        final String transactionId = config.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG).toString();
        if (transactionId == null || transactionId.trim().isEmpty()) {
            throw new IllegalArgumentException("Transaction Id cannot be null or empty");
        }
    }

    /**
     * It writes a Message to kafka and stores Payload and Header in Tier Storage.
     * The kafka message has headers information pointing to Tier Storage payload.
     * Both operation are in the same tansaction. If something goes wrong, they will be consistently aborted
     *
     * @param producerRecord producer record
     */
    @Override
    public void send(final ProducerRecord<P> producerRecord) {
        try {
            validateTierStorageMetadata(producerRecord);

            // Get the Tier Storage from RoutindData which was already created by the user
            final TierStorageMetadata tierStorageMetadata =
                    producerRecord.getRoutingData().getTierStorageMetadata();

            // Serialize the producerRecord payload to be stored with TieredStorage
            // when callback being invoked by Kafka
            final DatabusMessage databusMessage =
                    messagePayloadAdapter.adapt(producerRecord.payload(), producerRecord.getHeaders());
            final byte[] databusMessageSerialized = getKafkaValueSerializer().serialize("", databusMessage);

            // Remove the producerRecord payload to be written in kafka and keeps Headers.
            final ProducerRecord<P> adaptedProducerRecord = new ProducerRecord<>(producerRecord.getRoutingData(),
                    producerRecord.getHeaders(),
                    new MessagePayload<>(null));

            // Transform a Databus ProducerRecord in a Kafka Producer Record
            org.apache.kafka.clients.producer.ProducerRecord<String, DatabusMessage> targetProducerRecord =
                    getDatabusProducerRecordAdapter().adapt(adaptedProducerRecord);

            try {
                beginTransaction();
                super.sendKafkaRecord(targetProducerRecord);
                tierStorage.put(tierStorageMetadata.getBucketName(),
                        tierStorageMetadata.getObjectName(),
                        databusMessageSerialized);
                commitTransaction();
                LOG.info("Send Ok. Message was sent and payload was stored in Tier Storage");
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                super.flush();
                super.close();
                final String errMsg = "Send cannot be performed. Producer throws an irrecoverable exception "
                        + "during a transaction. Producer is closed effective immediately. ERROR:" + e.getMessage();
                LOG.error(errMsg, e);
                throw new DatabusClientRuntimeException(errMsg, e, this.getClass());

            } catch (Exception e) {
                abortTransaction();
                final String errMsg = "Send cannot be performed. Producer throws an exception during a transaction. "
                 + "Producer continues active. Message should be sent again to retry. ERROR:" + e.getMessage();
                LOG.error(errMsg, e);
                throw new DatabusClientRuntimeException(errMsg, e, Producer.class);
            }
        } catch (Exception e) {
            final String errMsg = "send cannot be performed: ERROR:" + e.getMessage();
            LOG.error(errMsg, e);
            throw new DatabusClientRuntimeException(errMsg, e, Producer.class);
        }

    }

    protected void validateTierStorageMetadata(ProducerRecord<P> producerRecord) {
        if (producerRecord.getRoutingData().getTierStorageMetadata() == null
                || producerRecord.getRoutingData().getTierStorageMetadata().getBucketName() == null
                || producerRecord.getRoutingData().getTierStorageMetadata().getBucketName().isEmpty()
                || producerRecord.getRoutingData().getTierStorageMetadata().getObjectName() == null
                || producerRecord.getRoutingData().getTierStorageMetadata().getObjectName().isEmpty()
        ) {
            final String errMsg = "Send cannot be performed. Bucket metadatada is invalid";
            LOG.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }
    }

    /**
     * It writes a Message to kafka and stores Payload in Tier Storage.
     * The kafka message has headers information pointing to Tier Storage payload. So that a Consumer can recover
     * Both operation are in the same tansaction. If something goes wrong, they will be consistently aborted
     *
     * @param producerRecord The non-null record to send
     * @param callback       A user-supplied callback to execute when the record has been acknowledged by the server
     *                       (null indicates no callback)
     */
    @Override
    public void send(ProducerRecord<P> producerRecord, final Callback callback) {

        validateTierStorageMetadata(producerRecord);

        if (callback == null) {
            final String errMsg = "Send cannot be performed. Producer Callback is invalid";
            LOG.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        try {

            // Get the Tier Storage from RoutindData which was already created by the user
            final TierStorageMetadata tierStorageMetadata =
                    producerRecord.getRoutingData().getTierStorageMetadata();

            // Serialize the producerRecord payload to be stored with TieredStorage when callback being invoked by Kafka
            final DatabusMessage databusMessage =
                    messagePayloadAdapter.adapt(producerRecord.payload(), producerRecord.getHeaders());
            final byte[] kafkaValueSerialized = getKafkaValueSerializer().serialize("", databusMessage);

            // Remove the producerRecord payload to be written in kafka and keeps Headers.
            final ProducerRecord<P> adaptedProducerRecord = new ProducerRecord<>(producerRecord.getRoutingData(),
                    producerRecord.getHeaders(),
                    new MessagePayload<>(null));

            // Transform a Databus ProducerRecord in a Kafka Producer Record
            org.apache.kafka.clients.producer.ProducerRecord<String, DatabusMessage> targetProducerRecord =
                    getDatabusProducerRecordAdapter().adapt(adaptedProducerRecord);

            // Create the callback
            CountDownLatch latch = new CountDownLatch(1);
            final CallbackAdapterTierStorage callbackAdapterTierStorage;
            callbackAdapterTierStorage = new CallbackAdapterTierStorage(callback,
                    kafkaValueSerialized,
                    latch,
                    tierStorageMetadata);

            try {
                beginTransaction();
                super.sendKafkaRecord(targetProducerRecord, callbackAdapterTierStorage);
                // wait for callback ends
                final boolean callbackFinished = latch.await(10000, TimeUnit.MILLISECONDS);
                if (callbackFinished) { // means the callback finished before timeout
                    if (callbackAdapterTierStorage.isMessageAndPayloadStored()) {
                        commitTransaction();
                        LOG.info("Send OK. Message was sent and payload was stored in Tier Storage");
                    } else { // means something was wrong in kafka or tier storage
                        abortTransaction(); // Logging is already performed in the Callback
                        throw new DatabusClientRuntimeException("Send cannot be performed. Record not produced. "
                                + "Something was wrong producing the message in Kafka or "
                                + " storing the payload in Tier Storage", this.getClass());
                    }
                } else { // means that the callback has not finished in time
                    abortTransaction();
                    final String errMsg = "Send cannot be performed. Record not produced. "
                            + "Timeout: Too long time taken by Kafka or Tier Storage.";
                    LOG.error(errMsg);
                    throw new DatabusClientRuntimeException(errMsg, this.getClass());
                }
            } catch (InterruptedException e) {
                abortTransaction();
                final String errMsg = "Send cannot be performed. Producer was interrupted while "
                        + "waiting for a Callback response. "
                        + "Producer continues active. Message should be sent again to retry. ERROR:" + e.getMessage();
                LOG.error(errMsg, e);
                throw new DatabusClientRuntimeException(errMsg, e, Producer.class);
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                super.flush();
                super.close();
                final String errMsg = "Send cannot be performed. Producer throws an irrecoverable exception "
                        + "during a transaction. Producer is closed effective immediately. ERROR:" + e.getMessage();
                LOG.error(errMsg, e);
                throw new DatabusClientRuntimeException(errMsg, e, this.getClass());

            } catch (Exception e) {
                abortTransaction();
                final String errMsg = "Producer throws an exception during a transaction. "
                        + "Producer continues active. Message should be sent again to retry. ERROR:" + e.getMessage();
                LOG.error(errMsg);
                throw new DatabusClientRuntimeException(errMsg, e, Producer.class);
            }

        } catch (Exception e) {
            if (e instanceof DatabusClientRuntimeException) {
                throw e;
            }
            final String errMsg = "Send cannot be performed: " + e.getMessage();
            LOG.error(errMsg, e);
            throw new DatabusClientRuntimeException(errMsg, e, Producer.class);
        }

    }


    /**
     * Callback Adapter
     * <p>
     * It forwards a kafka callback to databus callback
     */
    private class CallbackAdapterTierStorage implements org.apache.kafka.clients.producer.Callback {
        /**
         * Callback defined by the user when invoking send method
         */
        private final Callback userCallback;

        /**
         * the kafka value serializer
         */
        private final byte[] kafkaValueSerialized;

        /**
         * An object to signal when callback has finished
         */
        private CountDownLatch latch;

        /**
         * The Tier Storage in charged to store payload
         */
        private TierStorageMetadata tierStorageMetadata;

        /**
         * storage operation result
         */
        private AtomicBoolean isMessageAndPayloadStored = new AtomicBoolean(false);

        /**
         * @param userCallback user callback
         * @param kafkaValueSerialized kafka serializer
         * @param latch a object to signal when callback
         */
        CallbackAdapterTierStorage(final Callback userCallback,
                                   final byte[] kafkaValueSerialized,
                                   final CountDownLatch latch,
                                   final TierStorageMetadata tierStorageMetadata) {
            this.userCallback = userCallback;
            this.kafkaValueSerialized = kafkaValueSerialized;
            this.latch = latch;
            this.tierStorageMetadata = tierStorageMetadata;
        }

        /**
         * It is called as a send result. Then it is forwarded and adapted to databus callback
         *
         * @param recordMetadata Kafka RecordMetadata
         * @param exception      An exception thrown by Databus broker
         */
        @Override
        public void onCompletion(final org.apache.kafka.clients.producer.RecordMetadata recordMetadata,
                                 final Exception exception) {

            if (exception != null) {
                LOG.error("Send cannot be performed. The record was not produced. ERROR:"
                        + exception.getMessage(), exception);
                response(recordMetadata, exception);
                return;
            }

            try {

                tierStorage.put(tierStorageMetadata.getBucketName(),
                        tierStorageMetadata.getObjectName(),
                        kafkaValueSerialized);
                response(recordMetadata, exception);
            } catch (DatabusClientRuntimeException databusException) {
                LOG.error("Send cannot be performed. The record was not produced. ERROR:"
                        + databusException.getMessage(), databusException);
                response(recordMetadata, databusException);
            }
        }

        /**
         * Send callback response
         *
         * @param kafkaRecordMetadata recordMetadata
         * @param exception exception
         */
        private void response(final org.apache.kafka.clients.producer.RecordMetadata kafkaRecordMetadata,
                              final Exception exception) {
            isMessageAndPayloadStored.set(exception == null);
            latch.countDown();

            RecordMetadata databusRecordMetadata = null;
            if (kafkaRecordMetadata != null) {
                databusRecordMetadata = new RecordMetadata(kafkaRecordMetadata);
            }
            userCallback.onCompletion(databusRecordMetadata, exception);
        }

        protected boolean isMessageAndPayloadStored() {
            return isMessageAndPayloadStored.get();
        }
    }


}

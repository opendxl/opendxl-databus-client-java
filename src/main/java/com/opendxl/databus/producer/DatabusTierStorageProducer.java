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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class DatabusTierStorageProducer<P> extends DatabusProducer<P> {

    private static final Logger LOG = LoggerFactory.getLogger(DatabusTierStorageProducer.class);

    private TierStorage tierStorage;
    private MessagePayloadAdapter<P> messagePayloadAdapter;

    public DatabusTierStorageProducer(final Map<String, Object> configs, final Serializer<P> userSerializer,
                                      final TierStorage tierStorage) {
        this(configs, userSerializer, null, tierStorage);
    }


    public DatabusTierStorageProducer(final Map<String, Object> configs, final Serializer<P> userSerializer,
                                      final Credential credential, final TierStorage tierStorage) {
        super(configs, userSerializer, credential);
        if (tierStorage == null) {
            throw new IllegalArgumentException("Tier Storage cannot be null");
        }
        this.tierStorage = tierStorage;
        setFieldMembers(userSerializer);
        initTransactions();
    }


    public DatabusTierStorageProducer(final Properties properties, final Serializer<P> userSerializer,
                                      final TierStorage tierStorage) {
        this(properties, userSerializer, null, tierStorage);
    }


    public DatabusTierStorageProducer(final Properties properties, final Serializer<P> userSerializer,
                                      final Credential credential, final TierStorage tierStorage) {

        super(properties, userSerializer, credential);
        if (tierStorage == null) {
            throw new IllegalArgumentException("Tier Storage cannot be null");
        }
        setFieldMembers(userSerializer);
        initTransactions();
    }

    private void setFieldMembers(Serializer<P> userSerializer) {
        setKafkaValueSerializer(new MessageSerializer()); // The serializer used bu Kafka
        this.messagePayloadAdapter = new MessagePayloadAdapter<P>(userSerializer);
        setDatabusProducerRecordAdapter(new DatabusProducerRecordAdapter<P>(userSerializer));
    }


    @Override
    public void send(final ProducerRecord record) {
        send(record, null);
    }

    @Override
    public void send(ProducerRecord<P> producerRecord, final Callback callback) {

        if (producerRecord.getRoutingData().getTierStorageMetadata() == null
            || producerRecord.getRoutingData().getTierStorageMetadata().getBucketName() == null
            || producerRecord.getRoutingData().getTierStorageMetadata().getBucketName().isEmpty()
            || producerRecord.getRoutingData().getTierStorageMetadata().getObjectName() == null
            || producerRecord.getRoutingData().getTierStorageMetadata().getObjectName().isEmpty()
        ) {
            final String errMsg = "Bucket metadatada is invalid";
            LOG.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        final TierStorageMetadata tierStorageMetadata =
                producerRecord.getRoutingData().getTierStorageMetadata();

        // Serialize the producerRecord payload to be stored with TieredStorage when callback being invoked by Kafka
        final DatabusMessage databusMessage =
                messagePayloadAdapter.adapt(producerRecord.payload(), producerRecord.getHeaders());
        final byte[] databusMessageSerialized = getKafkaValueSerializer().serialize("", databusMessage);

        // Remove the producerRecord headers and payload
        final ProducerRecord<P> adaptedProducerRecord = new ProducerRecord(producerRecord.getRoutingData(),
                producerRecord.getHeaders(),
                new MessagePayload(null));


        // Get a Kafka Producer Record made up by a DatabusMessage:
        // version = AVRO_1_S3_TIER_STORAGE_VERSION_NUMBER
        // headers = empty
        // payload = empty
        org.apache.kafka.clients.producer.ProducerRecord<String, DatabusMessage> targetProducerRecord =
                getDatabusProducerRecordAdapter().adapt(adaptedProducerRecord);

        // Create the callback
        CountDownLatch latch = new CountDownLatch(1);
        final CallbackAdapterTierStorage callbackAdapterTierStorage;
        if (callback != null) {
            callbackAdapterTierStorage = new CallbackAdapterTierStorage(callback,
                    databusMessageSerialized,
                    latch,
                    tierStorageMetadata);
        } else {
            callbackAdapterTierStorage = null;
        }

        beginTransaction();
        super.sendKafkaRecord(targetProducerRecord, callbackAdapterTierStorage);
        try {
            // wait for callback ends
            final boolean callbackFinished = latch.await(10000, TimeUnit.MILLISECONDS);
            if (callbackFinished) {
                if (callbackAdapterTierStorage.isOk()) {
                    commitTransaction();
                } else {
                    abortTransaction();
                }
            } else { // means that the callback has not finished in time
                LOG.error("Record not produced. Too long time taken by tier storage.");
                abortTransaction();
            }

        } catch (InterruptedException e) {
            abortTransaction();
        }
    }


    /**
     * Callback Adapter
     * <p>
     * It forwards a kafka callback to databus callback
     */
    private class CallbackAdapterTierStorage implements org.apache.kafka.clients.producer.Callback {
        private final Callback callback;
        private final byte[] databusMessageSerialized;
        private CountDownLatch latch;
        private TierStorageMetadata tierStorageMetadata;
        private AtomicBoolean isOk = new AtomicBoolean(false);

        /**
         * @param callback                 Databus callback
         * @param databusMessageSerialized
         * @param latch
         */
        CallbackAdapterTierStorage(final Callback callback,
                                   final byte[] databusMessageSerialized,
                                   final CountDownLatch latch,
                                   final TierStorageMetadata tierStorageMetadata) {
            this.callback = callback;
            this.databusMessageSerialized = databusMessageSerialized;
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
                LOG.error("The record was not produced. " + exception.getMessage(), exception);
                response(recordMetadata, exception);
                return;
            }

            try {

                tierStorage.put(tierStorageMetadata.getBucketName(),
                        tierStorageMetadata.getObjectName(),
                        databusMessageSerialized);
                response(recordMetadata, exception);
            } catch (DatabusClientRuntimeException databusException) {
                LOG.error("The record was not produced. " + databusException.getMessage(), databusException);
                response(recordMetadata, databusException);
            }
        }

        /**
         * Send callback response
         *
         * @param recordMetadata recordMetadata
         * @param exception exception
         */
        private void response(final org.apache.kafka.clients.producer.RecordMetadata kafkaRecordMetadata,
                              final Exception exception) {
            isOk.set(exception == null);
            latch.countDown();
            final RecordMetadata databusRecordMetadata =
                    Optional.ofNullable(new RecordMetadata(kafkaRecordMetadata))
                            .orElse(null);
            callback.onCompletion(databusRecordMetadata, exception);
        }

        protected boolean isOk() {
            return isOk.get();
        }
    }


}

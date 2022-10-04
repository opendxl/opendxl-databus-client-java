/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer;

import com.opendxl.databus.consumer.Consumer;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.common.MetricName;
import com.opendxl.databus.common.PartitionInfo;
import com.opendxl.databus.common.RecordMetadata;
import com.opendxl.databus.common.TopicPartition;
import com.opendxl.databus.common.internal.adapter.DatabusProducerRecordAdapter;
import com.opendxl.databus.common.internal.adapter.DatabusProducerJSONRecordAdapter;
import com.opendxl.databus.common.internal.adapter.MetricNameMapAdapter;
import com.opendxl.databus.common.internal.adapter.PartitionInfoListAdapter;
import com.opendxl.databus.consumer.OffsetAndMetadata;
import com.opendxl.databus.consumer.OffsetCommitCallback;
import com.opendxl.databus.entities.internal.DatabusMessage;
import com.opendxl.databus.producer.metric.ProducerMetric;
import com.opendxl.databus.producer.metric.ProducerMetricBuilder;
import com.opendxl.databus.producer.metric.ProducerMetricEnum;
import com.opendxl.databus.serialization.internal.DatabusKeySerializer;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 *  A abstract producer, responsible for handling Databus outgoing messages.
 * @param <P> payload's type
 */
public abstract class Producer<P> {

    /**
     * A DatabusKeySerializer associated to the producer.
     */
    private DatabusKeySerializer keySerializer;

    /**
     * A Kafka Serializer of {@link DatabusMessage}.
     */
    private org.apache.kafka.common.serialization.Serializer<DatabusMessage> valueSerializer;

    /**
     * A Kafka Serializer for JSON Records.
     */
    private org.apache.kafka.common.serialization.Serializer<byte[]> jsonValueSerializer;

    /**
     * A configuration map for the producer.
     */
    private Map<String, Object> configuration;

    /**
     * A Kafka Producer associated to the producer.
     */
    private org.apache.kafka.clients.producer.Producer<String, DatabusMessage> producer;

    /**
     * A Kafka Producer associated to the producer generating JSON records.
     */
    private org.apache.kafka.clients.producer.Producer<String, byte[]> jsonProducer;

    /**
     * A {@link DatabusProducerRecordAdapter} Producer associated to the producer.
     */
    private DatabusProducerRecordAdapter<P> databusProducerRecordAdapter;

    /**
     * A {@link DatabusProducerJSONRecordAdapter} Producer associated to the
     * producer.
     */
    private DatabusProducerJSONRecordAdapter<P> databusProducerJSONRecordAdapter;

    /**
     * A String which represents the clientId associated to the producer.
     */
    private String clientId;

    /**
     * A boolean value if set
     * true  - Will produce the records with original JSON payload.
     *         Headers and other routing information are set in Kafka headers section.
     * false - Will produce records in DataBusMessage format
     */
    private boolean produceRecordAsJSON = false;

    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    /**
     * Gets the configuration to a producer instance
     *
     * @return The configuration map
     */
    public Map<String, Object> getConfiguration() {
        return configuration;
    }

    /**
     * A boolean which indicates to produce Kafka Header with specified values.
     */
    protected boolean produceKafkaHeaders;

    /**
     * This method add credential kept into a configuration for the {@link Producer} instance
     *
     * @param configuration It is the configuration that a SDK's user sends when a new instance of DatabusProducer
     *                      is created
     * @param credential Identity to authentication/authorization
     */
    public void setProduceKafkaHeader(final boolean produceKafkaHeaders) {
        this.produceKafkaHeaders = produceKafkaHeaders;
    }

    /**
     * Asynchronously send a record to a topic. Equivalent to <code>send(record, null)</code>.
     * See {@link #send(ProducerRecord, Callback)} for details.
     *
     * @param record The record to send
     * @throws IllegalArgumentException      If record argumet is null
     * @throws DatabusClientRuntimeException If send method fails. The original cause could be any of these exceptions:
     *                                       <p> SerializationException   If the key or value are not valid objects
     *                                       given the configured
     *                                       serializers
     *                                       <p> BufferExhaustedException If <code>block.on.buffer.full=false</code>
     *                                       and the buffer is full.
     *                                       <p> InterruptException       If the thread is interrupted while blocked
     */
    public void send(final ProducerRecord record) {
        send(record, null);
    }

    /**
     * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
     * <p>
     * The send is asynchronous and this method will return immediately once the record has been stored in the buffer of
     * records waiting to be sent. This allows sending many records in parallel without blocking to wait for the
     * response after each one.
     * <p>
     * Fully non-blocking usage can make use of the {@link Callback} parameter to provide a callback that
     * will be invoked when the request is complete.
     * </p>
     * <pre>
     * {@code
     * ProducerRecord record = new ProducerRecord(RoutingData, Headers, Payload);
     * producer.send(myRecord,
     *               new Callback() {
     *                   public void onCompletion(RecordMetadata metadata, Exception e) {
     *                       if(e != null)
     *                           e.printStackTrace();
     *                       System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *                   }
     *               });
     * }
     * </pre>
     * <p>
     * Callbacks for records being sent to the same partition are guaranteed to execute in order. That is, in the
     * following example <code>callback1</code> is guaranteed to execute before <code>callback2</code>:
     * </p>
     * <pre>
     * {@code
     * producer.send(new ProducerRecord(RoutingData, Headers, Payload), callback1);
     * producer.send(new ProducerRecord(RoutingData, Headers, Payload), callback2);
     * }
     * </pre>
     * <p>
     * Note that callbacks will generally execute in the I/O thread of the producer and so should be reasonably fast or
     * they will delay the sending of messages from other threads. If you want to execute blocking or computationally
     * expensive callbacks it is recommended to use your own {@link java.util.concurrent.Executor} in the callback body
     * to parallelize processing.
     *
     * @param producerRecord   The non-null record to send
     * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
     *                 indicates no callback)
     * @throws IllegalArgumentException      If record argumet is null
     * @throws DatabusClientRuntimeException If send method fails. The original cause could be any of these exceptions:
     *                                       <p> SerializationException   If the key or value are not valid objects
     *                                       given the configured
     *                                       serializers
     *                                       <p> BufferExhaustedException If <code>block.on.buffer.full=false</code>
     *                                       and the buffer is full.
     *                                       <p> InterruptException       If the thread is interrupted while blocked
     */
    public void send(final ProducerRecord<P> producerRecord, final Callback callback) {
        if (producerRecord == null) {
            throw new IllegalArgumentException("record cannot be null");
        }

        org.apache.kafka.clients.producer.ProducerRecord<String, DatabusMessage> targetProducerRecord = null;
        org.apache.kafka.clients.producer.ProducerRecord<String, byte[]> targetProducerJSONRecord = null;

        try {
            final CallbackAdapter callbackAdapter;
            if (callback != null) {
                callbackAdapter = new CallbackAdapter(callback);
            } else {
                callbackAdapter = null;
            }

            if (produceRecordAsJSON) {
                targetProducerJSONRecord = databusProducerJSONRecordAdapter.adapt(producerRecord);
                jsonProducer.send(targetProducerJSONRecord, callbackAdapter);
            } else {
                targetProducerRecord = databusProducerRecordAdapter.adapt(producerRecord);
                producer.send(targetProducerRecord, callbackAdapter);
            }

        } catch (Exception e) {
            throw new DatabusClientRuntimeException("send cannot be performed: " + e.getMessage(), e, Producer.class);
        }
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
     * greater than 0) and blocks on the completion of the requests associated with these records. The post-condition
     * of <code>flush()</code> is that any previously sent record will have completed
     * (e.g. <code>Future.isDone() == true</code>).
     * A request is considered completed when it is successfully acknowledged
     * according to the <code>acks</code> configuration you have specified or else it results in an error.
     * <p>
     * Other threads can continue sending records while one thread is blocked waiting for a flush call to complete,
     * however no guarantee is made about the completion of records sent after the flush call begins.
     * <p>
     * This method can be useful when consuming from some input system and producing into Kafka.
     * The <code>flush()</code> call
     * gives a convenient way to ensure all previously sent messages have actually completed.
     * <p>
     * This example shows how to consume from one Kafka topic and produce to another Kafka topic:
     * <pre>
     * {@code
     * for(ProducerRecord record: consumer.poll(100))
     *     // Create here RoutingData, Headers and  Payload objects properly
     *     producer.send(new ProducerRecord(RoutingData, Headers, Payload);
     * producer.flush();
     * consumer.commit();
     * }
     * </pre>
     * <p>
     * Note that the above example may drop records if the produce request fails.
     * If we want to ensure that this does not occur
     * we need to set <code>retries=&lt;large_number&gt;</code> in our config.
     *
     * @throws DatabusClientRuntimeException If flush method fails. The original cause could be the following exception:
     *                                       <p> InterruptException If the thread is interrupted while blocked
     */
    public void flush() {
        try {
            producer.flush();
        } catch (Exception e) {
            throw new DatabusClientRuntimeException("flush cannot be performed :" + e.getMessage(), e, Producer.class);

        }
    }

    /**
     * Get the partition metadata for the give topic. This can be used for custom partitioning.
     *
     * @param topic to get info
     * @return List of {@link PartitionInfo}
     * @throws DatabusClientRuntimeException If partitionsFor method fails.
     * The original cause could be the following exception:
     *                                       <p> InterruptException If the thread is interrupted while blocked
     */
    public List<PartitionInfo> partitionsFor(final String topic) {
        try {
            List<org.apache.kafka.common.PartitionInfo> partitions = producer.partitionsFor(topic);
            return new PartitionInfoListAdapter().adapt(partitions);
        } catch (Exception e) {
            throw new DatabusClientRuntimeException("partitionsFor cannot be performed :"
                    + e.getMessage(), e, Producer.class);
        }
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     *
     * @return a Map of metrics
     * @throws DatabusClientRuntimeException If metrics method fails.
     */
    public Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics() {
        try {
            Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> metrics
                    = producer.metrics();

            return new MetricNameMapAdapter().adapt(metrics);
        } catch (Exception e) {
            throw new DatabusClientRuntimeException("metrics cannot be performed :"
                    + e.getMessage(), e, Producer.class);

        }
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * This method is equivalent to <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     * <p>
     * <strong>If close() is called from {@link Callback}, a warning message will be
     * logged and close(0, TimeUnit.MILLISECONDS)
     * will be called instead. We do this because the sender thread would otherwise try to join itself and
     * block forever.</strong>
     * <p>
     *
     * @throws DatabusClientRuntimeException If close method fails. The original cause could be the following exception:
     *                                       <p> InterruptException If the thread is interrupted while blocked
     */
    public void close() {
        try {
            producer.close();
        } catch (Exception e) {
            throw new DatabusClientRuntimeException("close cannot be performed :" + e.getMessage(), e, Producer.class);
        }
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail
     * any unsent and unacknowledged records immediately.
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(0, TimeUnit.MILLISECONDS)</code>. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     *
     * @param timeout  The maximum time to wait for producer to complete any pending requests. The value should be
     *                 non-negative. Specifying a timeout of zero means do not wait for pending send
     *                 requests to complete.
     * @param timeUnit The time unit for the <code>timeout</code>l
     * @throws DatabusClientRuntimeException If close method fails. The original cause could be any of these exceptions:
     *                                       <p> InterruptException       If the thread is interrupted while blocked
     *                                       <p> IllegalArgumentException If the <code>timeout</code> is negative.
     */
    public void close(final long timeout, final TimeUnit timeUnit) {
        try {
            producer.close(timeout, timeUnit);
        } catch (Exception e) {
            throw new DatabusClientRuntimeException("close cannot be performed :" + e.getMessage(), e, Producer.class);
        }

    }

    /**
     * Sets the record format. If the parameter is
     * true  - Records are produced in original JSON payload format.
     *         Headers and other routing information are set in Kafka headers section.
     * false - Records are produced in DataBusMessage format
     * @param produceRecordAsJSON sets the record format
     */
    public void produceRecordAsJSON(final boolean produceRecordAsJSON) {
        this.produceRecordAsJSON = produceRecordAsJSON;
    }

    /**
     * Set the DatabusKeySerializer in producer
     *
     * @param keySerializer A DatabusKeySerializer Instance
     */
    protected void setKeySerializer(final DatabusKeySerializer keySerializer) {
        this.keySerializer = keySerializer;
    }

    /**
     * Set the value serializer in producer
     *
     * @param valueSerializer A Serializer object instance for the value serializer
     */
    protected void
    setValueSerializer(final org.apache.kafka.common.serialization.Serializer<DatabusMessage> valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    /**
     * Set the JSON value serializer in producer
     *
     * @param jsonValueSerializer A byte array Serializer object instance for the JSON value serializer
     */
    protected void
    setJSONValueSerializer(final org.apache.kafka.common.serialization.Serializer<byte[]> jsonValueSerializer) {
        this.jsonValueSerializer = jsonValueSerializer;
    }

    /**
     * Get the key serializer from producer
     *
     * @return  A {@link DatabusKeySerializer} object instance
     */
    protected DatabusKeySerializer getKeySerializer() {
        return keySerializer;
    }

    /**
     * Get the value serializer from producer
     *
     * @return  A {@link org.apache.kafka.common.serialization.Serializer} object instance
     */
    protected org.apache.kafka.common.serialization.Serializer<DatabusMessage> getValueSerializer() {
        return valueSerializer;
    }

    /**
     * Get the JSON value serializer from producer
     *
     * @return  A {@link org.apache.kafka.common.serialization.Serializer} object instance
     */
    protected org.apache.kafka.common.serialization.Serializer<byte[]> getJSONValueSerializer() {
        return jsonValueSerializer;
    }

    /**
     * Set a Kafka producer instance to the producer.
     *
     * @return  A {@link org.apache.kafka.clients.producer.Producer} object instance to set in the producer
     */
    protected void setProducer(final org.apache.kafka.clients.producer.Producer<String, DatabusMessage> producer) {
        this.producer = producer;
    }

    /**
     * Set a JSON Kafka producer instance to the producer.
     *
     * @return  A {@link org.apache.kafka.clients.producer.Producer} object instance to set in the producer
     */
    protected void setJSONProducer(final org.apache.kafka.clients.producer.Producer<String, byte[]> producer) {
        this.jsonProducer = producer;
    }

    /**
     * Set a {@link DatabusProducerRecordAdapter} associated to the producer.
     *
     * @param databusProducerRecordAdapter The {@link DatabusProducerRecordAdapter} to set to the producer
     */
    protected void setDatabusProducerRecordAdapter(final DatabusProducerRecordAdapter<P> databusProducerRecordAdapter) {
        this.databusProducerRecordAdapter = databusProducerRecordAdapter;
    }

    /**
     * Set a {@link DatabusProducerJSONRecordAdapter} associated to the producer.
     *
     * @param databusProducerJSONRecordAdapter The {@link DatabusProducerJSONRecordAdapter} to set to the producer
     */
    protected void setDatabusProducerJSONRecordAdapter(final DatabusProducerJSONRecordAdapter<P>
    databusProducerJSONRecordAdapter) {
        this.databusProducerJSONRecordAdapter = databusProducerJSONRecordAdapter;
    }

    /**
     * Set the clientId to the producer
     *
     * @param clientId The clientId associated to the producer
     */
    protected void setClientId(final String clientId) {
        this.clientId = clientId;
    }

    /**
     * Callback Adapter
     * <p>
     * It forwards a kafka callback to databus callback
     */
    private static class CallbackAdapter implements org.apache.kafka.clients.producer.Callback {
        private final Callback callback;

        /**
         * @param callback Databus callback
         */
        CallbackAdapter(final Callback callback) {
            this.callback = callback;
        }

        /**
         * It is called as a send result. Then it is forwarded and adapted to databus callback
         *
         * @param recordMetadata Kafka RecordMetadata
         * @param e              An exception thrown by Databus broker
         */
        @Override
        public void onCompletion(final org.apache.kafka.clients.producer.RecordMetadata recordMetadata,
                                 final Exception e) {

            RecordMetadata rMetadata = null;

            if (recordMetadata != null) {
                rMetadata = new RecordMetadata(recordMetadata);
            }

            callback.onCompletion(rMetadata, e);
        }
    }

    /**
     * Set configuration to a producer instance
     *
     * @param configuration A map which contains the configuration to be assigned
     */
    public void setConfiguration(final Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    /**
     * Needs to be called before any other methods when the transactional.id is set in the configuration.
     *
     * This method does the following:
     *   1. Ensures any transactions initiated by previous instances of the producer with the same
     *      transactional.id are completed. If the previous instance had failed with a transaction in
     *      progress, it will be aborted. If the last transaction had begun completion,
     *      but not yet finished, this method awaits its completion.
     *   2. Gets the internal producer id and epoch, used in all future transactional
     *      messages issued by the producer.
     *
     * @throws DatabusClientRuntimeException If method fails. The original cause could be any of these exceptions:
     * <p>IllegalStateException if no transactional.id has been configured
     * <p>org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * <p>org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * <p>KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    public void initTransactions() {
        try {
            producer.initTransactions();
        } catch (Exception e) {
            throw new DatabusClientRuntimeException("initTransactions cannot be performed: "
                    + e.getMessage(), e, Producer.class);
        }
    }

    /**
     * Should be called before the start of each new transaction. Note that prior to the first invocation
     * of this method, you must invoke {@link #initTransactions()} exactly one time.
     *
     * @throws DatabusClientRuntimeException If method fails. The original cause could be any of these exceptions:
     * <p> IllegalStateException if no transactional.id has been configured or if {@link #initTransactions()}
     *         has not yet been invoked
     * <p> ProducerFencedException if another producer with the same transactional.id is active
     * <p> org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * <p> org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * <p> KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    public void beginTransaction() {
        try {
            producer.beginTransaction();
        } catch (Exception e) {
            throw new DatabusClientRuntimeException("beginTransaction cannot be performed: "
                    + e.getMessage(), e, Producer.class);
        }

    }

    /**
     * Sends a list of specified offsets to the consumer group coordinator, also marks
     * those offsets as part of the current transaction. These offsets will be considered
     * committed only if the transaction is committed successfully. The committed offset should
     * be the next message your application will consume, i.e. lastProcessedMessageOffset + 1.
     * <p>
     * This method should be used when you need to batch consumed and produced messages
     * together, typically in a consume-transform-produce pattern. Thus, the specified
     * {@code consumerGroupId} should be the same as config parameter {@code group.id} of the used
     * {@link Consumer consumer}. Note, that the consumer should
     * have {@code enable.auto.commit=false}
     * and should also not commit offsets manually
     * (via {@link Consumer#commitSync(Map) sync} or
     * {@link Consumer#commitAsync(OffsetCommitCallback)} commits).
     *
     * @param offsets offsets
     * @param consumerGroupId consumer group id
     * @throws DatabusClientRuntimeException If method fails. The original cause could be any of these exceptions:
     * <p> IllegalStateException if no transactional.id has been configured or no transaction has been started
     * <p> ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * <p> org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * <p> org.apache.kafka.common.errors.UnsupportedForMessageFormatException  fatal error indicating the message
     *         format used for the offsets topic on the broker does not support transactions
     * <p> org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * <p> KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     */
    public void sendOffsetsToTransaction(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                         final String consumerGroupId)  {
        try {
            Map<org.apache.kafka.common.TopicPartition,
                    org.apache.kafka.clients.consumer.OffsetAndMetadata> adaptedOffsets = new HashMap();

            offsets.forEach((topicPartition, offsetAndMetadata) -> {
                adaptedOffsets.put(
                        new org.apache.kafka.common.TopicPartition(topicPartition.topic(),
                                topicPartition.partition()),
                        new org.apache.kafka.clients.consumer.OffsetAndMetadata(offsetAndMetadata.offset(),
                                offsetAndMetadata.metadata()));
            });

            producer.sendOffsetsToTransaction(adaptedOffsets, consumerGroupId);
        } catch (Exception e) {
            throw new DatabusClientRuntimeException("sendOffsetsToTransaction cannot be performed: "
                    + e.getMessage(), e, Producer.class);
        }
    }

    /**
     * Commits the ongoing transaction. This method will flush any unsent records before actually
     * committing the transaction.
     *
     * Further, if any of the {@link #send(ProducerRecord)} calls which were part of the transaction hit irrecoverable
     * errors, this method will throw the last received exception immediately and the transaction will not be committed.
     * So all {@link #send(ProducerRecord)} calls in a transaction must succeed in order for this method to succeed.
     *
     * DatabusClientRuntimeException If method fails. The original cause could be any of these exceptions:
     * <p> IllegalStateException if no transactional.id has been configured or no transaction has been started
     * <p> ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * <p> org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * <p> org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * <p> KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     */
    public void commitTransaction() {
        try {
            producer.commitTransaction();
        } catch (Exception e) {
            throw new DatabusClientRuntimeException("commitTransaction cannot be performed: "
                    + e.getMessage(), e, Producer.class);
        }

    }

    /**
     * Aborts the ongoing transaction. Any unflushed produce messages will be aborted when this call is made.
     * This call will throw an exception immediately if any prior {@link #send(ProducerRecord)} calls failed with a
     * {@link org.apache.kafka.common.errors.ProducerFencedException} or an
     * instance of {@link org.apache.kafka.common.errors.AuthorizationException}.
     *
     * <p> DatabusClientRuntimeException If method fails. The original cause could be any of these exceptions:
     * <p> IllegalStateException if no transactional.id has been configured or no transaction has been started
     * <p> ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * <p> org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * <p> org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * <p> KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    public void abortTransaction() {
        try {
            producer.abortTransaction();
        } catch (Exception e) {
            throw new DatabusClientRuntimeException("abortTransaction cannot be performed: "
                    + e.getMessage(), e, Producer.class);
        }

    }

    /**
     * The total number of records sent per clientId.
     *
     * @return ProducerMetric instance.
     */
    public ProducerMetric recordSendTotalMetric() {
        return getMetricPerClientId(ProducerMetricEnum.RECORD_SEND_TOTAL);
    }

    /**
     * The average number of records sent per second per clientId.
     *
     * @return ProducerMetric instance.
     */
    public ProducerMetric recordSendRateMetric() {
        return getMetricPerClientId(ProducerMetricEnum.RECORD_SEND_RATE);
    }

    /**
     * The average record size per clientId.
     *
     * @return ProducerMetric instance.
     */
    public ProducerMetric recordSizeAvgMetric() {
        return getMetricPerClientId(ProducerMetricEnum.RECORD_SIZE_AVG);
    }

    /**
     * The maximum record size per clientId.
     *
     * @return ProducerMetric instance.
     */
    public ProducerMetric recordSizeMaxMetric() {
        return getMetricPerClientId(ProducerMetricEnum.RECORD_SIZE_MAX);
    }

    /**
     * The total number of record sends that resulted in errors per clientId.
     *
     * @return ProducerMetric instance.
     */
    public ProducerMetric recordErrorTotalMetric() {
        return getMetricPerClientId(ProducerMetricEnum.RECORD_ERROR_TOTAL);
    }

    /**
     * The average per-second number of record sends that resulted in errors per clientId.
     *
     * @return ProducerMetric instance.
     */
    public ProducerMetric recordErrorRateMetric() {
        return getMetricPerClientId(ProducerMetricEnum.RECORD_ERROR_RATE);
    }

    /**
     * The maximum size of the batches processed by the connector per clientId.
     *
     * @return ProducerMetric instance.
     */
    public ProducerMetric recordBatchSizeMaxMetric() {
        return getMetricPerClientId(ProducerMetricEnum.RECORD_BATCH_SIZE_MAX);
    }

    /**
     * The maximum size of the batches processed by the connector per clientId.
     *
     * @return ProducerMetric instance.
     */
    public ProducerMetric recordBatchSizeAvgMetric() {
        return getMetricPerClientId(ProducerMetricEnum.RECORD_BATCH_SIZE_AVG);
    }

    /**
     * The total number of records sent for a topic.
     *
     * @param topic The topic name.
     * @return ProducerMetric instance.
     */
    public ProducerMetric recordSendTotalPerTopicMetric(final String topic) {
        return getMetricPerClientIdAndTopic(topic, ProducerMetricEnum.RECORD_SEND_TOTAL_PER_TOPIC);
    }

    /**
     * The average number of records sent per second for a topic.
     *
     * @param topic The topic name.
     * @return ProducerMetric instance.
     */
    public ProducerMetric recordSendRatePerTopicMetric(final String topic) {
        return getMetricPerClientIdAndTopic(topic, ProducerMetricEnum.RECORD_SEND_RATE_PER_TOPIC);
    }

    /**
     * The total number of record sends that resulted in errors for a topic.
     *
     * @param topic The topic name.
     * @return ProducerMetric instance.
     */
    public ProducerMetric recordErrorTotalPerTopicMetric(final String topic) {
        return getMetricPerClientIdAndTopic(topic, ProducerMetricEnum.RECORD_ERROR_TOTAL_PER_TOPIC);
    }

    /**
     * The average per-second number of record sends that resulted in errors for a topic.
     *
     * @param topic The topic name.
     * @return ProducerMetric instance.
     */
    public ProducerMetric recordErrorRatePerTopicMetric(final String topic) {
        return getMetricPerClientIdAndTopic(topic, ProducerMetricEnum.RECORD_ERROR_RATE_PER_TOPIC);
    }

    /**
     * The total number of bytes sent for a topic.
     *
     * @param topic The topic name.
     * @return ProducerMetric instance.
     */
    public ProducerMetric recordByteTotalPerTopicMetric(final String topic) {
        return getMetricPerClientIdAndTopic(topic, ProducerMetricEnum.RECORD_BYTE_TOTAL_PER_TOPIC);
    }

    /**
     * The average number of bytes sent per second for a topic.
     *
     * @param topic The topic name.
     * @return ProducerMetric instance.
     */
    public ProducerMetric recordByteRatePerTopicMetric(final String topic) {
        return getMetricPerClientIdAndTopic(topic, ProducerMetricEnum.RECORD_BYTE_RATE_PER_TOPIC);
    }

    /**
     * Gets a {@link ProducerMetric} given a {@link ProducerMetricEnum}.
     *
     * @param producerMetricEnum The {@link ProducerMetricEnum} to get the metric.
     * @return a {@link ProducerMetric} instance.
     */
    private ProducerMetric getMetricPerClientId(final ProducerMetricEnum producerMetricEnum) {

        try {
            return ProducerMetricBuilder.buildClientIdMetric(metrics(),
                    producerMetricEnum.getName(),
                    clientId);
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            final String msg = producerMetricEnum.getError() + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Producer.class);
        }
    }

    /**
     * Gets a {@link ProducerMetric} given a Topic name and a {@link ProducerMetricEnum}.
     *
     * @param topic The topic name.
     * @param producerMetricEnum The {@link ProducerMetricEnum} to get the metric.
     * @return a {@link ProducerMetric} instance.
     */
    private ProducerMetric getMetricPerClientIdAndTopic(final String topic, final ProducerMetricEnum
            producerMetricEnum) {
        if (StringUtils.isBlank(topic)) {
            throw new DatabusClientRuntimeException("topic cannot be empty or null", Producer.class);
        }

        TopicPartition topicPartition = new TopicPartition(topic);

        try {
            return ProducerMetricBuilder.buildClientIdTopicMetric(metrics(), producerMetricEnum.getName(),
                    clientId, topicPartition);
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            final String msg = producerMetricEnum.getError() + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Producer.class);
        }
    }
}

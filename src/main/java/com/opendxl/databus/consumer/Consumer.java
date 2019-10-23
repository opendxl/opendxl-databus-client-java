/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

import com.opendxl.databus.common.MetricName;
import com.opendxl.databus.common.OffsetAndTimestamp;
import com.opendxl.databus.common.PartitionInfo;
import com.opendxl.databus.common.TopicPartition;
import com.opendxl.databus.common.internal.adapter.ConsumerRecordsAdapter;
import com.opendxl.databus.common.internal.adapter.MetricNameMapAdapter;
import com.opendxl.databus.common.internal.adapter.PartitionInfoListAdapter;
import com.opendxl.databus.common.internal.adapter.TopicPartitionInfoListAdapter;
import com.opendxl.databus.common.internal.builder.TopicNameBuilder;
import com.opendxl.databus.consumer.metric.ConsumerMetricPerClientId;
import com.opendxl.databus.consumer.metric.ConsumerMetricPerClientIdAndTopicPartitions;
import com.opendxl.databus.consumer.metric.ConsumerMetricPerClientIdAndTopics;
import com.opendxl.databus.consumer.metric.ConsumerMetricsBuilder;
import com.opendxl.databus.entities.internal.DatabusMessage;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.serialization.internal.DatabusKeyDeserializer;
import com.opendxl.databus.serialization.internal.MessageDeserializer;
import org.apache.commons.lang.NullArgumentException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;


/**
 * A abstract consumer, responsible for handling Databus incoming messages. *
 *
 * @param <P> payload's type
 */
public abstract class Consumer<P> {

    /**
     * The DatabusKeyDeserializer to deserialize incoming messages.
     */
    private DatabusKeyDeserializer keyDeserializer = null;

    /**
     * The MessageDeserializer to deserialize a DatabusMessage.
     */
    private MessageDeserializer valueDeserializer = null;

    /**
     * The ConsumerRecordsAdapter to deserialize a DatabusMessage.
     */
    private ConsumerRecordsAdapter<P> consumerRecordsAdapter;

    /**
     * The client id.
     */
    private String clientId;

    /**
     * The topic partition list.
     */
    private List<TopicPartition> topicPartitions;

    /**
     * The logger object.
     */
    private static final Logger LOG = Logger.getLogger(Consumer.class);

    /**
     * The Kafka associated consumer.
     */
    private org.apache.kafka.clients.consumer.Consumer<String, DatabusMessage> consumer = null;

    /**
     * Subscribe to the given list of tenantGroups and topics to get dynamically
     * assigned topicPartitions. <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> Note that it is not possible to combine topic subscription with
     * group management
     * with manual partition assignment through {@link #assign(List)}.
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that belong to a particular
     * group and will trigger a rebalance operation if one of the following events trigger l-
     * <ul>
     * <li>Number of topicPartitions change for any of the subscribed list of topics
     * <li>Topic is created or deleted
     * <li>An existing member of the consumer group dies
     * <li>A new member is added to an existing consumer group via the join API
     * </ul>
     * <p>
     * When any of these events are triggered, the provided listener will be invoked first to indicate that
     * the consumer's assignment has been revoked, and then again when the new assignment has been received.
     * Note that this listener will immediately override any listener set in a previous call to subscribe.
     * It is guaranteed, however, that the topicPartitions revoked/assigned through this interface are from topics
     * subscribed in this call. See {@link ConsumerRebalanceListener} for more details.
     *
     * @param groupTopics The list of topics to subscribe to
     * @throws NullArgumentException         if any argument is null
     * @throws DatabusClientRuntimeException if subscription fails.
     */
    public void subscribe(final Map<String, List<String>> groupTopics) {
        subscribe(groupTopics, new NoOpConsumerRebalanceListener());
    }

    /**
     * Subscribe to the given list of tenantGroups and topics to get dynamically
     * assigned topicPartitions. <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> Note that it is not possible to combine topic subscription with group
     * management
     * with manual partition assignment through {@link #assign(List)}.
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that belong to a particular
     * group and will trigger a rebalance operation if one of the following events trigger l-
     * <ul>
     * <li>Number of topicPartitions change for any of the subscribed list of topics
     * <li>Topic is created or deleted
     * <li>An existing member of the consumer group dies
     * <li>A new member is added to an existing consumer group via the join API
     * </ul>
     * <p>
     * When any of these events are triggered, the provided listener will be invoked first to indicate that
     * the consumer's assignment has been revoked, and then again when the new assignment has been received.
     * Note that this listener will immediately override any listener set in a previous call to subscribe.
     * It is guaranteed, however, that the topicPartitions revoked/assigned through this interface are from topics
     * subscribed in this call. See {@link ConsumerRebalanceListener} for more details.
     *
     * @param groupTopics               The list of topics to subscribe to
     * @param consumerRebalanceListener Non-null listener getInstance to get notifications on partition
     *                                  assignment/revocation for the
     *                                  subscribed topics
     * @throws NullArgumentException         if any argument is null
     * @throws DatabusClientRuntimeException if subscription fails.
     */
    public void subscribe(final Map<String, List<String>> groupTopics,
                          final ConsumerRebalanceListener consumerRebalanceListener) {

        if (groupTopics == null) {
            throw new NullArgumentException("groupTopics");
        }

        if (consumerRebalanceListener == null) {
            throw new NullArgumentException("consumerRebalanceListener");
        }

        final List<String> groupTopicList = new ArrayList<String>();
        for (Map.Entry<String, List<String>> groups : groupTopics.entrySet()) {
            final String group = groups.getKey();
            for (String topic : groups.getValue()) {
                groupTopicList.add(TopicNameBuilder.getTopicName(topic, group));
            }
        }

        ConsumerRebalanceListenerAdapter adaptedListener =
                new ConsumerRebalanceListenerAdapter(consumerRebalanceListener, this);

        try {
            consumer.subscribe(groupTopicList, adaptedListener);
        } catch (Exception e) {
            final String msg = "There was an error when subscribe: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Subscribe to the given list of topics to get dynamically
     * assigned topicPartitions. <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> Note that it is not possible to combine topic subscription with group
     * management
     * with manual partition assignment through {@link #assign(List)}.
     * <p>
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that belong to a particular
     * group and will trigger a rebalance operation if one of the following events trigger -
     * <ul>
     * <li>Number of topicPartitions change for any of the subscribed list of topics
     * <li>Topic is created or deleted
     * <li>An existing member of the consumer group dies
     * <li>A new member is added to an existing consumer group via the join API
     * </ul>
     * <p>
     * When any of these events are triggered, the provided listener will be invoked first to indicate that
     * the consumer's assignment has been revoked, and then again when the new assignment has been received.
     * Note that this listener will immediately override any listener set in a previous call to subscribe.
     * It is guaranteed, however, that the topicPartitions revoked/assigned through this interface are from topics
     * subscribed in this call. See {@link ConsumerRebalanceListener} for more details.
     *
     * @param topics                    The non-null list of topics to subscribe to
     * @param consumerRebalanceListener Non-null listener getInstance to get notifications on partition
     *                                  assignment/revocation for the
     *                                  subscribed topics
     * @throws NullArgumentException         if any argument is null
     * @throws DatabusClientRuntimeException if subscription fails.
     */
    public void subscribe(final List<String> topics,
                          final ConsumerRebalanceListener consumerRebalanceListener) {

        if (topics == null) {
            throw new NullArgumentException("topics");
        }

        if (consumerRebalanceListener == null) {
            throw new NullArgumentException("consumerRebalanceListener");
        }

        ConsumerRebalanceListenerAdapter adaptedListener =
                new ConsumerRebalanceListenerAdapter(consumerRebalanceListener, this);

        try {
            consumer.subscribe(topics, adaptedListener);
        } catch (Exception e) {
            final String msg = "There was an error when subscribe: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Subscribe to the given list of topics to get dynamically
     * assigned topicPartitions. <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> Note that it is not possible to combine topic subscription with group
     * management
     * with manual partition assignment through {@link #assign(List)}.
     * <p>
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that belong to a particular
     * group and will trigger a rebalance operation if one of the following events trigger -
     * <ul>
     * <li>Number of topicPartitions change for any of the subscribed list of topics
     * <li>Topic is created or deleted
     * <li>An existing member of the consumer group dies
     * <li>A new member is added to an existing consumer group via the join API
     * </ul>
     * <p>
     * When any of these events are triggered, the provided listener will be invoked first to indicate that
     * the consumer's assignment has been revoked, and then again when the new assignment has been received.
     * Note that this listener will immediately override any listener set in a previous call to subscribe.
     * It is guaranteed, however, that the topicPartitions revoked/assigned through this interface are from topics
     * subscribed in this call. See {@link ConsumerRebalanceListener} for more details.
     *
     * @param topics The list of topics to subscribe to
     * @throws NullArgumentException         if any argument is null
     * @throws DatabusClientRuntimeException if subscription fails.
     */
    public void subscribe(final List<String> topics) {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    /**
     * Get the set of topicPartitions currently assigned to this consumer. If subscription happened by directly
     * assigning
     * topicPartitions using {@link #assign(List)} then this will simply return the same topicPartitions that
     * were assigned. If topic subscription was used, then this will give the set of topic topicPartitions currently
     * assigned
     * to the consumer (which may be none if the assignment hasn't happened yet, or the topicPartitions are in the
     * process of getting reassigned).
     *
     * @return The set of topicPartitions currently assigned to this consumer
     * @throws DatabusClientRuntimeException if it fails.
     */
    public Set<TopicPartition> assignment() {
        try {
            Set<TopicPartition> topicPartitions = new HashSet<>();
            consumer.assignment().forEach(e -> {
                topicPartitions.add(new TopicPartition(e.topic(), e.partition()));
            });
            return topicPartitions;
        } catch (Exception e) {
            final String msg = "A assignment cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Get the current subscription. Will return the same topics used in the most recent call to
     * {@link #subscribe(List, ConsumerRebalanceListener)}, or an empty set if no such call has been made.
     *
     * @return The set of topics currently subscribed to
     * @throws DatabusClientRuntimeException if it fails.
     */
    public Set<String> subscription() {
        try {

            return consumer.subscription();
        } catch (Exception e) {
            final String msg = "A subscription cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Manually assign a list of partition to this consumer. This interface does not allow for incremental assignment
     * and will replace the previous assignment (if there is one).
     * <p>
     * Manual topic assignment through this method does not use the consumer's group management
     * functionality. As such, there will be no rebalance operation triggered when group membership or cluster and topic
     * metadata change. Note that it is not possible to use both manual partition assignment with {@link #assign(List)}
     * and group assignment with {@link #subscribe(List, ConsumerRebalanceListener)}.
     * </p>
     *
     * @param partitions The list of topicPartitions to assign this consumer
     * @throws DatabusClientRuntimeException if it fails.
     */
    public void assign(final List<TopicPartition> partitions) {
        try {

            List<org.apache.kafka.common.TopicPartition> topicPartitions = new ArrayList();
            partitions.forEach(e -> {
                topicPartitions.add(new org.apache.kafka.common.TopicPartition(e.topic(), e.partition()));
            });
            consumer.assign(topicPartitions);
        } catch (Exception e) {
            final String msg = "A assign cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned topicPartitions.
     * The pattern matching will be done
     * periodically against topics
     * existing at the time of check.
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that
     * belong to a particular group and will trigger a rebalance operation if one of the
     * following events trigger -
     * <ul>
     * <li>Number of topicPartitions change for any of the subscribed list of topics
     * <li>Topic is created or deleted
     * <li>An existing member of the consumer group dies
     * <li>A new member is added to an existing consumer group via the join API
     * </ul>
     *
     * @param pattern                   Pattern to subscribe to
     * @param consumerRebalanceListener consumer listener for rebalancing databus operations
     * @throws DatabusClientRuntimeException if it fails.
     */
    public void subscribe(final Pattern pattern,
                          final ConsumerRebalanceListener consumerRebalanceListener) {
        try {

            ConsumerRebalanceListenerAdapter adaptedListener =
                    new ConsumerRebalanceListenerAdapter(consumerRebalanceListener, this);

            consumer.subscribe(pattern, adaptedListener);
        } catch (Exception e) {
            final String msg = "A subscribe cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Unsubscribe from topics currently subscribed with {@link #subscribe(List)}. This
     * also clears any topicPartitions directly assigned through {@link #assign(List)}.
     *
     * @throws DatabusClientRuntimeException if it fails.
     */
    public void unsubscribe() {
        try {
            consumer.unsubscribe();
        } catch (Exception e) {
            final String msg = "A unsubscribe cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * @throws UnsupportedOperationException because it is not supported
     */
    /*
    @Override
    @Deprecated
    public org.apache.kafka.clients.consumer.ConsumerRecords<String, DatabusMessage> poll(long timeout) {
        throw new UnsupportedOperationException("Use: public ConsumerRecords poll(int timeout)");
    }
    */

    /**
     * Fetch data for the topics or topicPartitions specified using one of the subscribe/assign APIs.
     * It is an error to not have
     * subscribed to any topics or topicPartitions before polling for data.
     * <p>
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially.
     * The last
     * consumed offset can be manually set through {@link #seek(TopicPartition, long)} or automatically set as the
     * last committed
     * offset for the subscribed list of topicPartitions
     *
     * @param timeout The time, in milliseconds, spent waiting in poll if data is not available. If 0, returns
     *                immediately with any records that are available now. Must not be negative.
     * @return map of topic to records since the last fetch for the subscribed list of topics and topicPartitions
     * @throws DatabusClientRuntimeException if poll fails.The original cause could be any of these exceptions:
     *                                       <ul>
     *                                       <li> org.apache.kafka.clients.consumer.InvalidOffsetException if the
     *                                       offset for a partition or
     *                                       set of
     *                                       topicPartitions is undefined or out of range and no offset reset policy
     *                                       has
     *                                       been configured
     *                                       <li> org.apache.kafka.common.errors.WakeupException if {@link #wakeup()}
     *                                       is called before or
     *                                       while this
     *                                       function is called
     *                                       <li> org.apache.kafka.common.errors.AuthorizationException if caller does
     *                                       Read access to any
     *                                       of the subscribed
     *                                       topics or to the configured groupId
     *                                       <li> org.apache.kafka.common.KafkaException for any other unrecoverable
     *                                       errors (e.g. invalid
     *                                       groupId or
     *                                       session timeout, errors deserializing key/value pairs, or any new error
     *                                       cases in future
     *                                       versions)
     *                                       </ul>
     */
    public ConsumerRecords poll(final long timeout) {
        try {
            final org.apache.kafka.clients.consumer.ConsumerRecords<String, DatabusMessage>
                    sourceConsumerRecords = consumer.poll(timeout);

            return consumerRecordsAdapter.adapt(sourceConsumerRecords);

        } catch (Exception e) {
            final String msg = "there was an error when poll: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }


    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It is an error to not
     * have
     * subscribed to any topics or partitions before polling for data.
     * <p>
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially.
     * The last
     * consumed offset can be manually set through {@link #seek(TopicPartition, long)} or automatically set as the last
     * committed
     * offset for the subscribed list of partitions
     *
     * <p>
     * This method returns immediately if there are records available. Otherwise, it will await the passed timeout.
     * If the timeout expires, an empty record set will be returned. Note that this method may block beyond the
     * timeout in order to execute custom  {@link ConsumerRebalanceListener} callbacks.
     *
     * @param timeout The time, in milliseconds, spent waiting in poll if data is not available. If 0, returns
     *                immediately with any records that are available now. Must not be negative.
     *
     * @return map of topic to records since the last fetch for the subscribed list of topics and topicPartitions
     * @throws DatabusClientRuntimeException if poll fails.The original cause could be any of these exceptions:
     * <ul>
     * <li> org.apache.kafka.clients.consumer.InvalidOffsetException if the offset for a partition or set of
     *             partitions is undefined or out of range and no offset reset policy has been configured
     * <li> org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * <li> org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * <li> org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more
     * details
     * <li> org.apache.kafka.common.errors.AuthorizationException if caller lacks Read access to any of the subscribed
     *             topics or to the configured groupId. See the exception for more details
     * <li> org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. invalid groupId or
     *             session timeout, errors deserializing key/value pairs, or any new error cases in future versions)
     * <li> java.lang.IllegalArgumentException if the timeout value is negative
     * <li> java.lang.IllegalStateException if the consumer is not subscribed to any topics or manually assigned any
     *             partitions to consume from
     * <li> java.lang.ArithmeticException if the timeout is greater than {@link Long#MAX_VALUE} milliseconds.
     * <li> org.apache.kafka.common.errors.InvalidTopicException if the current subscription contains any invalid
     *             topic (per {@link org.apache.kafka.common.internals.Topic#validate(String)})
     * <li> org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     * </ul>
     */
    public ConsumerRecords poll(final Duration timeout) {
        try {
            final org.apache.kafka.clients.consumer.ConsumerRecords<String, DatabusMessage>
                    sourceConsumerRecords = consumer.poll(timeout);

            return consumerRecordsAdapter.adapt(sourceConsumerRecords);

        } catch (Exception e) {
            final String msg = "there was an error when poll: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Commit offsets returned on the last {@link #poll(long) poll()} for all the subscribed list of topics
     * and topicPartitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is a synchronous commits and will block until either the commit succeeds or an unrecoverable error is
     * encountered (in which case it is thrown to the caller).
     *
     * @throws DatabusClientRuntimeException if a commitSync fails. The original cause could be any of these exceptions:
     *                                       <ul>
     *                                       <li>org.apache.kafka.clients.consumer.CommitFailedException if the commit
     *                                       failed and cannot be
     *                                       retried.
     *                                       This can only occur if you are using automatic group management with
     *                                       {@link #subscribe(List)},
     *                                       or if there is an active group with the same groupId which is using group
     *                                       management.
     *                                       <li> org.apache.kafka.common.errors.WakeupException if {@link #wakeup()}
     *                                       is called before or
     *                                       while this  function is called
     *                                       <li> org.apache.kafka.common.errors.AuthorizationException if not
     *                                       authorized to the topic or
     *                                       to the
     *                                       configured groupId
     *                                       <li> org.apache.kafka.common.KafkaException for any other unrecoverable
     *                                       errors (e.g. if offset
     *                                       metadata
     *                                       is too large or if the committed offset is invalid).
     *                                       </ul>
     */
    public void commitSync() {
        try {
            consumer.commitSync();
        } catch (Exception e) {
            final String msg = "A commitSync cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Commit the specified offsets for the specified list of topics and topicPartitions.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is a synchronous commits and will block until either the commit succeeds or an unrecoverable error is
     * encountered (in which case it is thrown to the caller).
     *
     * @param offsets A map of offsets by partition with associated metadata
     * @throws DatabusClientRuntimeException if commitSync fails. The original cause could be any of these exceptions:
     *                                       <ul>
     *                                       <li> org.apache.kafka.clients.consumer.CommitFailedException if the commit
     *                                       failed and cannot
     *                                       be retried.
     *                                       This can only occur if you are using automatic group management with
     *                                       {@link #subscribe(List)},
     *                                       or if there is an active group with the same groupId which is using group
     *                                       management.
     *                                       <li> org.apache.kafka.common.errors.WakeupException if {@link #wakeup()}
     *                                       is called before or
     *                                       while this
     *                                       function is called
     *                                       <li> org.apache.kafka.common.errors.AuthorizationException if not
     *                                       authorized to the topic or
     *                                       to the
     *                                       configured groupId
     *                                       <li> org.apache.kafka.common.KafkaException for any other unrecoverable
     *                                       errors (e.g. if offset
     *                                       metadata
     *                                       is too large or if the committed offset is invalid).
     *                                       </ul>
     */
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
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

            consumer.commitSync(adaptedOffsets);

        } catch (Exception e) {
            final String msg = "A commitSync with offsets cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Commit offsets returned on the last {@link #poll(long) poll()} for all the subscribed list of topics and
     * partition.
     * Same as {@link #commitAsync(OffsetCommitCallback) commitAsync(null)}
     *
     * @throws DatabusClientRuntimeException if a commitAsync fails
     */
    public void commitAsync() {
        try {
            consumer.commitAsync();
        } catch (Exception e) {
            final String msg = "commitAsync cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Commit offsets returned on the last {@link #poll(long) poll()} for the subscribed list of topics and
     * topicPartitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     *
     * @param offsetCommitCallback Callback to invoke when the commit completes
     * @throws DatabusClientRuntimeException if a commitAsync fails
     */
    public void commitAsync(final OffsetCommitCallback offsetCommitCallback) {
        try {
            OffsetCommitCallbackAdapter callbackAdapter =
                    new OffsetCommitCallbackAdapter(offsetCommitCallback);

            consumer.commitAsync(callbackAdapter);
        } catch (Exception e) {
            final String msg = "commitAsync(OffsetCommitCallback) cannot be permormed" + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(long) poll(timeout)}. If this API
     * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets
     *
     * @param partition partition to seek
     * @param offset    offset to seek
     * @throws DatabusClientRuntimeException if a seek fails
     */
    public void seek(final TopicPartition partition,
                     final long offset) {
        try {
            org.apache.kafka.common.TopicPartition topicPartition =
                    new org.apache.kafka.common.TopicPartition(partition.topic(), partition.partition());
            consumer.seek(topicPartition, offset);
        } catch (Exception e) {
            final String msg = "seek cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Seek to the first offset for each of the given topicPartitions. This function evaluates lazily, seeking to the
     * final offset in all topicPartitions only when {@link #poll(long)} or {@link #position(TopicPartition)}
     * are called.
     *
     * @param topicPartitions topicPartitions to seek
     * @throws DatabusClientRuntimeException if it fails
     */
    public void seekToBeginning(final TopicPartition... topicPartitions) {
        try {
            List<org.apache.kafka.common.TopicPartition> adaptedTopicPartition =
                    new ArrayList<>(topicPartitions.length);

            for (int i = 0; i < topicPartitions.length; ++i) {
                adaptedTopicPartition.add(
                        new org.apache.kafka.common.TopicPartition(topicPartitions[i].topic(),
                                topicPartitions[i].partition()));
            }
            consumer.seekToBeginning(adaptedTopicPartition);
        } catch (Exception e) {
            final String msg = "seekToBeginning cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Seek to the last offset for each of the given topicPartitions. This function evaluates lazily, seeking to the
     * final offset in all topicPartitions only when {@link #poll(long)} or {@link #position(TopicPartition)} are
     * called.
     *
     * @param topicPartitions topicPartitions to seek
     * @throws DatabusClientRuntimeException if it fails
     */
    public void seekToEnd(final TopicPartition... topicPartitions) {
        try {
            List<org.apache.kafka.common.TopicPartition> adaptedTopicPartition =
                    new ArrayList<>(topicPartitions.length);

            for (int i = 0; i < topicPartitions.length; ++i) {
                adaptedTopicPartition.add(
                        new org.apache.kafka.common.TopicPartition(topicPartitions[i].topic(),
                                topicPartitions[i].partition()));
            }
            consumer.seekToEnd(adaptedTopicPartition);
        } catch (Exception e) {
            final String msg = "seekToEnd cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Get the offset of the <i>next record</i> that will be fetched (if a record with that offset exists).
     *
     * @param partition The partition to get the position for
     * @return The offset
     * @throws DatabusClientRuntimeException if position fails. The original cause could be any of these exceptions:
     *                                       <ul>
     *                                       <li> org.apache.kafka.clients.consumer.InvalidOffsetException if no
     *                                       offset is currently
     *                                       defined for
     *                                       the partition
     *                                       <li> org.apache.kafka.common.errors.WakeupException if {@link #wakeup()}
     *                                       is called before or
     *                                       while this
     *                                       function is called
     *                                       <li> org.apache.kafka.common.errors.AuthorizationException if not
     *                                       authorized to the topic or
     *                                       to the
     *                                       configured groupId
     *                                       <li> org.apache.kafka.common.KafkaException for any other unrecoverable
     *                                       errors
     *                                       </ul>
     */
    public long position(final TopicPartition partition) {
        try {
            org.apache.kafka.common.TopicPartition topicPartition =
                    new org.apache.kafka.common.TopicPartition(partition.topic(), partition.partition());
            return consumer.position(topicPartition);
        } catch (Exception e) {
            final String msg = "position cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Get the last committed offset for the given partition (whether the commit happened by this process or
     * another). This offset will be used as the position for the consumer in the event of a failure.
     * <p>
     * This call may block to do a remote call if the partition in question isn't assigned to this consumer or if the
     * consumer hasn't yet initialized its cache of committed offsets.
     *
     * @param partition The partition to check
     * @return The last committed offset and metadata or null if there was no prior commit
     * @throws DatabusClientRuntimeException if committed fails. The original cause could be any of these exceptions:
     *                                       <ul>
     *                                       <li> org.apache.kafka.common.errors.WakeupException if {@link #wakeup()}
     *                                       is called before or
     *                                       while this
     *                                       function is called
     *                                       <li> org.apache.kafka.common.errors.AuthorizationException if not
     *                                       authorized to the topic or
     *                                       to the
     *                                       configured groupId
     *                                       <li> org.apache.kafka.common.KafkaException for any other unrecoverable
     *                                       errors
     *                                       </ul>
     */
    public OffsetAndMetadata committed(final TopicPartition partition) {
        try {
            org.apache.kafka.common.TopicPartition topicPartition =
                    new org.apache.kafka.common.TopicPartition(partition.topic(),
                            partition.partition());

            final org.apache.kafka.clients.consumer.OffsetAndMetadata committed =
                    consumer.committed(topicPartition);

            return new OffsetAndMetadata(committed.offset(), committed.metadata());
        } catch (Exception e) {
            final String msg = "committed cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Get the metrics kept by the consumer.
     *
     * @return a map of metrics
     * @throws DatabusClientRuntimeException if metrics fails.
     */
    public Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics() {
        try {
            Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric>
                    metrics = consumer.metrics();

            return new MetricNameMapAdapter().adapt(metrics);
        } catch (Exception e) {
            final String msg = "metrics cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Average number of records consumed per seconds for each consumer and its topics.
     *
     * @return ConsumerMetricPerClientIdAndTopics instance.
     */
    public ConsumerMetricPerClientIdAndTopics recordsPerSecondAvgMetric() {

        try {
            return ConsumerMetricsBuilder.buildClientTopicMetric(metrics(),
                    "records-consumed-rate",
                    clientId,
                    topicPartitions);
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            final String msg = "recordsPerSecondAvgMetric cannot be preformed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }

    }

    /**
     * Total number of records consumed per consumer and its topics.
     *
     * @return ConsumerMetricPerClientIdAndTopics instance.
     */
    public ConsumerMetricPerClientIdAndTopics recordsTotalMetric() {

        try {
            return ConsumerMetricsBuilder.buildClientTopicMetric(metrics(),
                    "records-consumed-total",
                    clientId,
                    topicPartitions);
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            final String msg = "recordsTotalMetric cannot be preformed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }

    }

    /**
     * Average bytes consumed per second for each consumer and its topics.
     *
     * @return ConsumerMetricPerClientIdAndTopics instance.
     */
    public ConsumerMetricPerClientIdAndTopics bytesPerSecondAvgMetric() {

        try {
            return ConsumerMetricsBuilder.buildClientTopicMetric(metrics(),
                    "bytes-consumed-rate",
                    clientId,
                    topicPartitions);
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            final String msg = "bytesPerSecondAvgMetric cannot be preformed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }

    }

    /**
     * Total bytes consumed per consumer and its topics.
     *
     * @return ConsumerMetricPerClientIdAndTopics instance.
     */
    public ConsumerMetricPerClientIdAndTopics bytesTotalMetric() {

        try {
            return ConsumerMetricsBuilder.buildClientTopicMetric(metrics(),
                    "bytes-consumed-total",
                    clientId,
                    topicPartitions);
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            final String msg = "bytesTotalMetric cannot be preformed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }

    }

    /**
     * Average number of records gotten per fetch request for each consumer and its topics.
     *
     * @return ConsumerMetricPerClientIdAndTopics instance.
     */
    public ConsumerMetricPerClientIdAndTopics recordsPerRequestAvgMetric() {

        try {
            return ConsumerMetricsBuilder.buildClientTopicMetric(metrics(),
                    "records-per-request-avg",
                    clientId,
                    topicPartitions);
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            final String msg = "recordsPerRequestAvgMetric cannot be preformed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg + e.getMessage(), e, Consumer.class);
        }

    }

    /**
     * Total number of fetch request for each consumer.
     * A fetch is not equals poll(). A Kafka fetch is governed by a set of consumer configuration prooperties like
     * fetch.min.bytes, fetch.max.wait.ms, max.topicPartitions.fetch.bytes, etc.
     * There might be more poll than fetch request.
     *
     * @return ConsumerMetricPerClientId instance.
     */
    public ConsumerMetricPerClientId totalFetchRequestMetric() {

        try {
            return ConsumerMetricsBuilder.buildClientMetric(metrics(),
                    "fetch-total",
                    clientId);
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            final String msg = "totalFetchRequestMetric cannot be preformed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg + e.getMessage(), e, Consumer.class);
        }

    }

    /**
     * The number of fetch requests per second.
     *
     * @return ConsumerMetricPerClientId instance
     */
    public ConsumerMetricPerClientId fetchRequestAvgMetric() {

        try {
            return ConsumerMetricsBuilder.buildClientMetric(metrics(),
                    "fetch-rate",
                    clientId);
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            final String msg = "fetchRequestAvgMetric cannot be preformed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }

    }

    /**
     * The maximum lag in terms of number of records for any partition in this window.
     *
     * @return ConsumerMetricPerClientId instance.
     */
    public ConsumerMetricPerClientId recordsLagMaxMetric() {

        try {
            return ConsumerMetricsBuilder.buildClientMetric(metrics(),
                    "records-lag-max",
                    clientId);
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            final String msg = "recordsLagMaxMetric cannot be preformed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }

    }

    /**
     * Average bytes per fetch request for each consumer and its topics.
     *
     * @return ConsumerMetricPerClientIdAndTopics instance.
     */
    public ConsumerMetricPerClientIdAndTopics bytesFetchRequestSizeAvgMetric() {

        try {
            return ConsumerMetricsBuilder.buildClientTopicMetric(metrics(),
                    "fetch-size-avg",
                    clientId,
                    topicPartitions);
        } catch (DatabusClientRuntimeException e) {
            throw e;
        } catch (Exception e) {
            final String msg = "bytesFetchRequestSizeAvgMetric cannot be preformed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * The latest lag of the partition.
     *
     * @return ConsumerMetricPerClientIdAndTopicPartitions instance.
     */
    public ConsumerMetricPerClientIdAndTopicPartitions recordsLagPerTopicPartition() {
        try {
            return ConsumerMetricsBuilder.buildClientTopicPartitionMetric(metrics(),
                    "records-lag",
                    clientId,
                    topicPartitions);

        } catch (Exception e) {
            final String msg = "recordsLagPerTopicPartition cannot be preformed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException("recordsLagPerTopicPartition cannot be preformed"
                    + e.getMessage(), e, Consumer.class);
        }
    }

    /**
     * The average lag of the partition.
     *
     * @return ConsumerMetricPerClientIdAndTopicPartitions instance.
     */
    public ConsumerMetricPerClientIdAndTopicPartitions recordsLagAvgPerTopicPartition() {
        try {
            return ConsumerMetricsBuilder.buildClientTopicPartitionMetric(metrics(),
                    "records-lag-avg",
                    clientId,
                    topicPartitions);

        } catch (Exception e) {
            final String msg = "recordsLagAvgPerTopicPartition cannot be preformed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * The max lag of the partition.
     *
     * @return ConsumerMetricPerClientIdAndTopicPartitions instance.
     */
    public ConsumerMetricPerClientIdAndTopicPartitions recordsLagMaxPerTopicPartition() {
        try {
            return ConsumerMetricsBuilder.buildClientTopicPartitionMetric(metrics(),
                    "records-lag-max",
                    clientId,
                    topicPartitions);

        } catch (Exception e) {
            final String msg = "recordsLagMaxPerTopicPartition cannot be preformed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Get metadata about the topicPartitions for a given topic. This method will issue a remote call to the server
     * if it does not already have any metadata about the given topic.
     *
     * @param topic The topic to get partition metadata for
     * @return The list of topicPartitions
     * @throws DatabusClientRuntimeException if partitionsFor fails. The original cause could be any of these
     *                                       exceptions:
     *                                       <ul>
     *                                       <li> org.apache.kafka.common.errors.WakeupException if {@link #wakeup()}
     *                                       is called before or
     *                                       while this
     *                                       function is called
     *                                       <li> org.apache.kafka.common.errors.AuthorizationException if not
     *                                       authorized to the specified
     *                                       topic
     *                                       <li> org.apache.kafka.common.errors.TimeoutException if the topic metadata
     *                                       could not be
     *                                       fetched before
     *                                       expiration of the configured request timeout
     *                                       <li> org.apache.kafka.common.KafkaException for any other unrecoverable
     *                                       errors
     *                                       </ul>
     */
    public List<PartitionInfo> partitionsFor(final String topic) {

        try {
            List<org.apache.kafka.common.PartitionInfo> partitions = consumer.partitionsFor(topic);
            return new PartitionInfoListAdapter().adapt(partitions);
        } catch (Exception e) {
            final String msg = "partitionsFor cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Get metadata about topicPartitions for all topics that the user is authorized to view. This method will issue a
     * remote call to the server.
     *
     * @return The map of topics and its topicPartitions
     * @throws DatabusClientRuntimeException if listTopics fails. The original cause could be any of these exceptions:
     *                                       <ul>
     *                                       <li> org.apache.kafka.common.errors.WakeupException if {@link #wakeup()}
     *                                       is called before or
     *                                       while this
     *                                       function is called
     *                                       <li> org.apache.kafka.common.errors.TimeoutException if the topic
     *                                       metadata could not be
     *                                       fetched before
     *                                       expiration of the configured request timeout
     *                                       <li> org.apache.kafka.common.KafkaException for any other unrecoverable
     *                                       errors
     *                                       </ul>
     */
    public Map<String, List<PartitionInfo>> listTopics() {
        try {

            final Map<String, List<org.apache.kafka.common.PartitionInfo>> topicPartitionInfoList
                    = consumer.listTopics();

            return new TopicPartitionInfoListAdapter().adapt(topicPartitionInfoList);

        } catch (Exception e) {
            final String msg = "listTopics cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Suspend fetching from the requested topicPartitions. Future calls to {@link #poll(long)} will not return
     * any records from these topicPartitions until they have been resumed using {@link #resume(Collection)}.
     * Note that this method does not affect partition subscription. In particular, it does not cause a group
     * rebalance when automatic assignment is used.
     *
     * @param partitions The topicPartitions which should be paused
     * @throws DatabusClientRuntimeException if it fails.
     */
    public void pause(final Collection<TopicPartition> partitions) {
        try {
            List<org.apache.kafka.common.TopicPartition> adaptedTopicPartition =
                    new ArrayList<>(partitions.size());

            for (TopicPartition topicPartition : partitions) {
                adaptedTopicPartition.add(
                        new org.apache.kafka.common.TopicPartition(topicPartition.topic(), topicPartition.partition())
                );
            }
            consumer.pause(adaptedTopicPartition);
        } catch (Exception e) {
            final String msg = "pause cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg + e.getMessage(), e, Consumer.class);
        }
    }

    /**
     * Resume specified topicPartitions which have been paused with {@link #pause(Collection)}. New calls to
     * {@link #poll(long)} will return records from these topicPartitions if there are any to be fetched.
     * If the topicPartitions were not previously paused, this method is a no-op.
     *
     * @param partitions The topicPartitions which should be resumed
     * @throws DatabusClientRuntimeException if it fails.
     */
    public void resume(final Collection<TopicPartition> partitions) {
        try {
            List<org.apache.kafka.common.TopicPartition> adaptedTopicPartition =
                    new ArrayList<>(partitions.size());

            for (TopicPartition topicPartition : partitions) {
                adaptedTopicPartition.add(
                        new org.apache.kafka.common.TopicPartition(topicPartition.topic(), topicPartition.partition())
                );
            }

            consumer.resume(adaptedTopicPartition);
        } catch (Exception e) {
            final String msg = "resume cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Close the consumer, waiting for up to the default timeout of 30 seconds for any needed cleanup.
     * If auto-commit is enabled, this will commit the current offsets if possible within the default
     * timeout. See {@link #close(Duration)} for details. Note that {@link #wakeup()}
     * cannot be used to interrupt close.
     *
     * @throws DatabusClientRuntimeException if it fails.
     * @throws IOException NA
     */
    public void close() throws IOException {
        try {
            consumer.close();
        } catch (Exception e) {
            final String msg = "close cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg + e.getMessage(), e, Consumer.class);
        }
    }

    /**
     * Tries to close the consumer cleanly within the specified timeout. This method waits up to
     * {@code timeout} for the consumer to complete pending commits and leave the group.
     * If auto-commit is enabled, this will commit the current offsets if possible within the
     * timeout. If the consumer is unable to complete offset commits and gracefully leave the group
     * before the timeout expires, the consumer is force closed. Note that {@link #wakeup()} cannot be
     * used to interrupt close.
     *
     * @param timeout The maximum time to wait for consumer to close gracefully. The value must be
     *                non-negative. Specifying a timeout of zero means do not wait for pending requests to complete.
     * @throws DatabusClientRuntimeException If there is a error
     */
    public void close(Duration timeout) {
        try {
            consumer.close(timeout);
        } catch (Exception e) {
            final String msg = "close cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg + e.getMessage(), e, Consumer.class);
        }
    }

    /**
     * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long poll.
     * The thread which is blocking in an operation will throw {@link org.apache.kafka.common.errors.WakeupException}.
     *
     * @throws DatabusClientRuntimeException if it fails.
     */
    public void wakeup() {
        try {
            consumer.wakeup();
        } catch (Exception e) {
            final String msg = "wakeup cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg + e.getMessage(), e, Consumer.class);
        }
    }

    /**
     * Commit the specified offsets for the specified list of topics and topicPartitions to Kafka.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     *
     * @param offsets              A map of offsets by partition with associate metadata.
     *                             This map will be copied internally, so it
     *                             is safe to mutate the map after returning.
     * @param offsetCommitCallback Callback to invoke when the commit completes.
     * @throws DatabusClientRuntimeException if it fails
     */
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets,
                            final OffsetCommitCallback offsetCommitCallback) {
        try {
            Map<org.apache.kafka.common.TopicPartition,
                    org.apache.kafka.clients.consumer.OffsetAndMetadata> adaptedOffsets = new HashMap(offsets.size());

            offsets.forEach((topicPartition, offsetAndMetadata) ->
                    adaptedOffsets.put(
                            new org.apache.kafka.common.TopicPartition(topicPartition.topic(),
                                    topicPartition.partition()),
                            new org.apache.kafka.clients.consumer.OffsetAndMetadata(offsetAndMetadata.offset(),
                                    offsetAndMetadata.metadata()))
            );

            OffsetCommitCallbackAdapter callbackAdapter =
                    new OffsetCommitCallbackAdapter(offsetCommitCallback);

            consumer.commitAsync(adaptedOffsets, callbackAdapter);
        } catch (Exception e) {
            final String msg = "commitAsync cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Look up the offsets for the given topicPartitions by timestamp. The returned offset for each partition is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
     * <p>
     * This is a blocking call. The consumer does not have to be assigned the topicPartitions.
     * If the message format version in a partition is before 0.10.0, i.e. the messages do not have timestamps, null
     * will be returned for that partition.
     *
     * @param timestampsToSearch the mapping from partition to the timestamp to look up.
     * @return a mapping from partition to the timestamp and offset of the first message with timestamp greater
     * than or equal to the target timestamp. {@code null} will be returned for the partition if there is no
     * such message.
     * @throws DatabusClientRuntimeException if it fails. The original cause could be any of these exceptions:
     *                                       <ul>
     *                                       <li>org.apache.kafka.common.errors.AuthenticationException if
     *                                       authentication fails. See the exception for more
     *                                       details
     *                                       org.apache.kafka.common.errors.AuthorizationException if not authorized to
     *                                       the topic(s). See the exception for
     *                                       more details
     *                                       <li>IllegalArgumentException if the target timestamp is negative
     *                                       <li>org.apache.kafka.common.errors.TimeoutException if the offset metadata
     *                                       could not be fetched before
     *                                              the amount of time allocated by {@code default.api.timeout.ms}
     *                                              expires.
     *                                       <li>org.apache.kafka.common.errors.UnsupportedVersionException if the
     *                                       broker does not support looking up
     *                                              the offsets by timestamp
     *                                       </ul>
     */
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch) {

        try {
            Map<org.apache.kafka.common.TopicPartition, Long> adaptedTopicPartitions = new HashMap();

            timestampsToSearch.forEach((topicPartition, timestamp) ->
                    adaptedTopicPartitions.put(new org.apache.kafka.common.TopicPartition(topicPartition.topic(),
                            topicPartition.partition()), timestamp)
            );

            Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp>
                    topicPartitionOffsetAndTimestampMap =
                    consumer.offsetsForTimes(adaptedTopicPartitions);

            Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();

            for (Map.Entry<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp>
                    entry : topicPartitionOffsetAndTimestampMap.entrySet()) {

                TopicPartition topicPartition = new TopicPartition(entry.getKey().topic(), entry.getKey().partition());
                OffsetAndTimestamp offsetAndTimestamp =
                        new OffsetAndTimestamp(entry.getValue().offset(), entry.getValue().timestamp(),
                                entry.getValue().leaderEpoch());

                result.put(topicPartition, offsetAndTimestamp);
            }

            return result;

        } catch (Exception e) {
            final String msg = "offsetsForTimes cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Get the first offset for the given topicPartitions.
     * <p>
     * This method does not change the current consumer position of the topicPartitions.
     * *
     *
     * @param partitions the topicPartitions to get the earliest offsets.
     * @return The earliest available offsets for the given topicPartitions
     * @throws DatabusClientRuntimeException if it fails. The original cause could be any of these exceptions:
     *                                       <ul>
     *                                        <li>org.apache.kafka.common.errors.AuthenticationException if
     *                                        authentication fails. See the exception for more
     *                                        details
     *                                        <li>org.apache.kafka.common.errors.AuthorizationException if not
     *                                        authorized to the topic(s). See the exception
     *                                        for more details
     *                                        <li>org.apache.kafka.common.errors.TimeoutException if the offset
     *                                        metadata could not be fetched before
     *                                               expiration of the configured {@code default.api.timeout.ms}
     *                                        </ul>
     */
    public Map<TopicPartition, Long> beginningOffsets(final List<TopicPartition> partitions) {
        try {
            List<org.apache.kafka.common.TopicPartition> kafkaTopicPartition = new ArrayList<>(partitions.size());

            for (TopicPartition partition : partitions) {
                kafkaTopicPartition.add(new org.apache.kafka.common.TopicPartition(partition.topic(),
                        partition.partition()));
            }
            Map<org.apache.kafka.common.TopicPartition, Long> topicPartitionLongMap =
                    consumer.beginningOffsets(kafkaTopicPartition);

            Map<TopicPartition, Long> result = new HashMap<>();
            for (Map.Entry<org.apache.kafka.common.TopicPartition, Long> entry : topicPartitionLongMap.entrySet()) {
                result.put(new TopicPartition(entry.getKey().topic(), entry.getKey().partition()), entry.getValue());
            }

            return result;

        } catch (Exception e) {
            final String msg = "beginningOffsets cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Get the end offsets for the given topicPartitions. In the default {@code read_uncommitted} isolation level, the
     * end
     * offset is the high watermark (that is, the offset of the last successfully replicated message plus one). For
     * {@code read_committed} consumers, the end offset is the last stable offset (LSO), which is the minimum of
     * the high watermark and the smallest offset of any open transaction. Finally, if the partition has never been
     * written to, the end offset is 0.
     *
     * <p>
     * This method does not change the current consumer position of the topicPartitions.
     *
     * @param partitions the topicPartitions to get the end offsets.
     * @return The end offsets for the given topicPartitions.
     * @throws DatabusClientRuntimeException if it fails. The original cause could be any of these exceptions:
     *                                       <ul>
     *                                        <li>org.apache.kafka.common.errors.AuthenticationException if
     *                                        authentication fails. See the exception for more
     *                                        details
     *                                        <li>org.apache.kafka.common.errors.AuthorizationException if not
     *                                        authorized to the topic(s). See the exception
     *                                        for more details
     *                                        <li>org.apache.kafka.common.errors.TimeoutException if the offset
     *                                        metadata could not be fetched before
     *                                               the amount of time allocated by {@code request.timeout.ms} expires
     *                                        </ul>
     */
    public Map<TopicPartition, Long> endOffsets(final List<TopicPartition> partitions) {
        try {
            List<org.apache.kafka.common.TopicPartition> kafkaTopicPartition = new ArrayList<>(partitions.size());

            for (TopicPartition partition : partitions) {
                kafkaTopicPartition.add(new org.apache.kafka.common.TopicPartition(partition.topic(),
                        partition.partition()));
            }
            Map<org.apache.kafka.common.TopicPartition, Long> topicPartitionLongMap =
                    consumer.endOffsets(kafkaTopicPartition);

            Map<TopicPartition, Long> result = new HashMap<>();
            for (Map.Entry<org.apache.kafka.common.TopicPartition, Long> entry : topicPartitionLongMap.entrySet()) {
                result.put(new TopicPartition(entry.getKey().topic(), entry.getKey().partition()), entry.getValue());
            }

            return result;

        } catch (Exception e) {
            final String msg = "endOffsets cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Get the set of topicPartitions that were previously paused by a call to {@link #pause(Collection)}.
     *
     * @return The set of paused topicPartitions
     */
    public Set<TopicPartition> paused() {
        try {
            Set<org.apache.kafka.common.TopicPartition> kafkaPaused = consumer.paused();

            Set<TopicPartition> result = new HashSet();
            for (org.apache.kafka.common.TopicPartition kafkaTopicPartition : kafkaPaused) {
                result.add(new TopicPartition(kafkaTopicPartition.topic(), kafkaTopicPartition.partition()));
            }
            return result;
        } catch (Exception e) {
            final String msg = "paused cannot be performed: " + e.getMessage();
            LOG.error(msg, e);
            throw new DatabusClientRuntimeException(msg, e, Consumer.class);
        }
    }

    /**
     * Sets the Kafka consumer
     *
     * @param consumer - The Kafka consumer to set.
     */
    protected void setConsumer(final org.apache.kafka.clients.consumer.Consumer<String, DatabusMessage> consumer) {
        this.consumer = consumer;
    }

    /**
     * Sets the Key Deserializer
     *
     * @param keyDeserializer - The DatabusKeyDeserializer to set.
     */
    protected void setKeyDeserializer(final DatabusKeyDeserializer keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    /**
     * Sets the MessageDeserializer
     *
     * @param valueDeserializer - The MessageDeserializer to set.
     */
    protected void setValueDeserializer(final MessageDeserializer valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }

    /**
     * Sets the ConsumerRecordsAdapter
     *
     * @param consumerRecordsAdapter - The ConsumerRecordsAdapter to set.
     */
    protected void setConsumerRecordsAdapter(final ConsumerRecordsAdapter<P> consumerRecordsAdapter) {
        this.consumerRecordsAdapter = consumerRecordsAdapter;
    }

    /**
     * Sets the clientId
     *
     * @param clientId - The clientId to set.
     */
    protected void setClientId(final String clientId) {
        this.clientId = clientId;

    }

    /**
     * Sets the clientId
     *
     * @param clientId - The clientId to set.
     */
    protected String getClientId() {
        return this.clientId;

    }

    /**
     * Gets the Key Deserializer
     *
     * @return A DatabusKeyDeserializer.
     */
    protected DatabusKeyDeserializer getKeyDeserializer() {
        return keyDeserializer;
    }

    /**
     * Gets the MessageDeserializer to get the message value.
     *
     * @return A MessageDeserializer.
     */
    protected MessageDeserializer getValueDeserializer() {
        return valueDeserializer;
    }

    /**
     * ConsumerRebalanceListener Adapter
     */
    private static class ConsumerRebalanceListenerAdapter<P>
            implements org.apache.kafka.clients.consumer.ConsumerRebalanceListener {

        /**
         * The listener for consumer rebalance
         */
        private final ConsumerRebalanceListener listener;

        /**
         * The consumer to rebalance itself
         */
        private Consumer consumer;

        /**
         * @param listener The listener for consumer rebalance
         * @param consumer The consumer to rebalance itself
         */
        ConsumerRebalanceListenerAdapter(final ConsumerRebalanceListener listener,
                                         final Consumer<P> consumer) {
            this.listener = listener;
            this.consumer = consumer;
        }

        /**
         * A callback method the user can implement to provide handling of offset commits to a customized store
         * on the start of a rebalance operation. This method will be called before a rebalance operation starts and
         * after the consumer stops fetching data. It is recommended that offsets should be committed in this callback
         * to either Kafka or a custom offset store to prevent duplicate data.
         *
         * @param partitions The list of partitions
         */
        @Override
        public void onPartitionsRevoked(final Collection<org.apache.kafka.common.TopicPartition> partitions) {
            ArrayList<TopicPartition> adaptedPartitions = new ArrayList<>();
            partitions.forEach(e -> adaptedPartitions.add(new TopicPartition(e.topic(), e.partition())));
            listener.onPartitionsRevoked(adaptedPartitions);
        }

        /**
         * A callback method the user can implement to provide handling of customized offsets on completion of a
         * successful partition re-assignment. This method will be called after an offset re-assignment
         * completes and before the consumer starts fetching data.
         *
         * @param partitions The list of partitions
         */
        @Override
        public void onPartitionsAssigned(final Collection<org.apache.kafka.common.TopicPartition> partitions) {
            ArrayList<TopicPartition> adaptedPartitions = new ArrayList<>();
            partitions.forEach(e -> adaptedPartitions.add(new TopicPartition(e.topic(), e.partition())));
            consumer.setPartitions(adaptedPartitions);
            listener.onPartitionsAssigned(adaptedPartitions);
        }
    }

    /**
     * Sets the consumer topics-partitions.
     *
     * @param partitions The topic partitions list.
     */
    private synchronized void setPartitions(final ArrayList<TopicPartition> partitions) {
        this.topicPartitions = partitions;
    }

    /**
     * OffsetCommitCallback Adapter
     */
    private static class OffsetCommitCallbackAdapter
            implements org.apache.kafka.clients.consumer.OffsetCommitCallback {

        /**
         * The OffsetCommitCallbackAdapter instance.
         */
        private final OffsetCommitCallback offsetCommitCallback;

        /**
         * The constructor of the OffsetCommitCallbackAdapter.
         *
         * @param offsetCommitCallback original listener.
         */
        OffsetCommitCallbackAdapter(final OffsetCommitCallback offsetCommitCallback) {
            this.offsetCommitCallback = offsetCommitCallback;
        }

        /**
         * A callback method the user can implement to provide asynchronous handling of commit request completion.
         * This method will be called when the commit request sent to the server has been acknowledged.
         *
         * @param offsets   A map of the offsets and associated metadata that this callback applies to.
         * @param exception The exception thrown during processing of the request, or null if the commit
         *                  completed successfully.
         */
        @Override
        public void onComplete(
                final Map<org.apache.kafka.common.TopicPartition,
                        org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets, final Exception exception) {

            final Map<TopicPartition, OffsetAndMetadata> adaptedOffsets =
                    new HashMap<>(offsets.size());

            offsets.forEach((topicPartition, offsetAndMetadata) ->
                    adaptedOffsets.put(
                            new TopicPartition(topicPartition.topic(), topicPartition.partition()),
                            new OffsetAndMetadata(offsetAndMetadata.offset(), offsetAndMetadata.metadata()))
            );

            DatabusClientRuntimeException databusClientException = null;
            if (exception != null) {
                databusClientException = new DatabusClientRuntimeException(exception.getMessage(),
                        exception, OffsetCommitCallbackAdapter.class);
            }
            offsetCommitCallback.onComplete(adaptedOffsets, databusClientException);
        }
    }

}


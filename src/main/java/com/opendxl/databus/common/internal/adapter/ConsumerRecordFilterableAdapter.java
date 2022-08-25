package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.internal.util.HeaderInternalField;
import com.opendxl.databus.consumer.ConsumerRecord;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.entities.internal.DatabusMessage;
import com.opendxl.databus.serialization.Deserializer;
import com.opendxl.databus.serialization.internal.MessageDeserializer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public final class ConsumerRecordFilterableAdapter<P> implements
Adapter<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>,
        ConsumerRecord<P>> {
    /**
     * Logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerRecordFilterableAdapter.class);

    /**
     * The message deserializer.
     */
    private final Deserializer<P> messageDeserializer;

    /**
     * The message deserializer.
     */
    private final MessageDeserializer databusMessageDeserializer;

    /**
     * The header filter to filter records.
     */
    protected Map<String, Object> headerFilter;

    /** Constructor
     * @param messageDeserializer a {@link Deserializer} getInstance getInstance used for deserialize the payload.
     */
    public ConsumerRecordFilterableAdapter(final Deserializer<P> messageDeserializer,
            final MessageDeserializer databusMessageDeserializer) {
        this.messageDeserializer = messageDeserializer;
        this.databusMessageDeserializer = databusMessageDeserializer;
    }

    public void setHeaderFilter(final Map<String, Object> filter) {
        this.headerFilter = filter;
    }

    /**
     * Adapter pattern implementation for ConsumerRecord.
     * Adapts a DatabusMessage object to a ConsumerRecord.
     *
     * @param sourceConsumerRecord the ConsumerRecord to be adapted.
     * @return A Databus {@link ConsumerRecord}
     */
    @Override
    public ConsumerRecord<P>
    adapt(final org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]> sourceConsumerRecord) {
        final Headers headers = sourceConsumerRecord.headers();
        boolean matchingRecord = isMatchingRecord(headers, headerFilter);
        if (matchingRecord) {
            final byte[] value = sourceConsumerRecord.value();
            final DatabusMessage databusMessage = getDatabusMessage(sourceConsumerRecord.topic(), headers, value);
            MessagePayload<P> payload =
            new DatabusMessageAdapter<P>(messageDeserializer).adapt(databusMessage);
            String topic = getTopic(databusMessage, sourceConsumerRecord);
            String tenantGroup = getTenantGroup(databusMessage);
            final ConsumerRecord<P> targetConsumerRecord = new ConsumerRecord<P>(sourceConsumerRecord.key(),
            databusMessage.getHeaders(),
            payload,
            topic,
            tenantGroup,
            sourceConsumerRecord.partition(),
            sourceConsumerRecord.offset(),
            sourceConsumerRecord.timestamp());
            return targetConsumerRecord;
        } else {
            return null;
        }
    }

    private DatabusMessage getDatabusMessage(final String topic, final Headers headers, final byte[] value) {
        final DatabusMessage databusMessage = value == null ? null
            : this.databusMessageDeserializer.deserialize(topic, headers, value);
        return databusMessage;
    }

    /**
     * Get the topic name from headers if it exists. Otherwise, it is taken from source Consumer Record.
     * <p>
     * If the user produce a message by using tenantGroup the the topic name comes into headers.
     * @param sourceConsumerRecord
     *
     * @param sourceConsumerRecord source Consumer Record.
     * @return The topic name.
     */
    private String
    getTopic(final DatabusMessage databusMessage,
             final org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]> sourceConsumerRecord) {
        if (!StringUtils.isBlank(databusMessage.getHeaders().get(HeaderInternalField.TOPIC_NAME_KEY))) {
            final String topic = databusMessage.getHeaders().get(HeaderInternalField.TOPIC_NAME_KEY);
            databusMessage.removeHeader(HeaderInternalField.TOPIC_NAME_KEY);
            return topic;
        } else {
            return sourceConsumerRecord.topic();
        }
    }

    /**
     * Get the tenantGroup name from headers if it exists. Otherwise, it is empty.
     *
     * @param databusMessage source Consumer Record.
     * @return The tenantGroup name from headers if it exists. Otherwise, it return an empty String.
     */
    private String
    getTenantGroup(final DatabusMessage databusMessage) {
        if (!StringUtils
                .isBlank(databusMessage.getHeaders().get(HeaderInternalField.TENANT_GROUP_NAME_KEY))) {
            final String tenantGroup =
                    databusMessage.getHeaders().get(HeaderInternalField.TENANT_GROUP_NAME_KEY);
            databusMessage.removeHeader(HeaderInternalField.TENANT_GROUP_NAME_KEY);
            return tenantGroup;
        } else {
            return "";
        }
    }

    private boolean isMatchingRecord(final Headers headers, final Map<String, Object> headerFilter) {
        Iterator<Header> iterator = headers.iterator();
        Map<String, Object> kafkaHeaderData = new HashMap<>();
        while (iterator.hasNext()) {
            Header header = iterator.next();
            kafkaHeaderData.put(header.key(), header.value().toString());
        }

        if (kafkaHeaderData.size() == 0) {
            LOG.debug("Empty Kafka Headers");
            return true;
        }

        return filterRecordMessage(kafkaHeaderData, headerFilter);
    }

    private boolean filterRecordMessage(final Map<String, Object> messageMap,
            final Map<String, Object> filterCondition) {
        boolean found = true;
        for (final String key : filterCondition.keySet()) {
            if (messageMap.containsKey(key)) {
                String headerValue = String.valueOf(messageMap.get(key));
                Object objectValue = filterCondition.get(key);
                if (objectValue instanceof Set) {
                    Set<String> filterSet = (Set<String>) objectValue;
                    found = found && filterSet.contains(headerValue);
                    if (!found) {
                        LOG.debug("Record skipped because filter " + key + ": " + filterSet + " not found in record");
                        break;
                    }
                } else {
                    String filterValue = String.valueOf(filterCondition.get(key));
                    found = found && filterValue.equals(headerValue);
                    if (!found) {
                        LOG.debug("Record skipped because filter " + key + ": " + filterValue + " not found in record");
                        break;
                    }
                }
            } else {
                found = false;
                LOG.debug("Record skipped because filtering key " + key + " not found in record");
                break;
            }
        }
        return found;
    }
}

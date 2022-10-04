/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/


package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.internal.util.HeaderInternalField;
import com.opendxl.databus.consumer.ConsumerRecord;
import com.opendxl.databus.entities.MessagePayload;
import java.util.Iterator;
import org.apache.kafka.common.header.Header;

/**
 * Adapter for ConsumerRecord.
 */
public final class ConsumerJSONRecordAdapter<P> implements
        Adapter<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>,
                ConsumerRecord<P>> {


    private com.opendxl.databus.entities.Headers targetRecordHeaders;
    private String targetRecordTopic;
    private String targetRecordTenantGroup;

    /** Constructor
     */
    public ConsumerJSONRecordAdapter() {
        targetRecordHeaders = new com.opendxl.databus.entities.Headers();
        targetRecordTopic = "";
        targetRecordTenantGroup = "";
    }

    private void extractTargetHeaderInfo(final org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>
        sourceConsumerRecord) {
        targetRecordTopic = sourceConsumerRecord.topic() != null ? sourceConsumerRecord.topic() : "";
        Iterator<Header> iterator = sourceConsumerRecord.headers().iterator();
        while (iterator.hasNext()) {
            Header header = iterator.next();
            switch (header.key()) {
                case HeaderInternalField.TOPIC_NAME_KEY:
                    targetRecordTopic = new String(header.value());
                    break;
                case HeaderInternalField.TENANT_GROUP_NAME_KEY:
                    targetRecordTenantGroup = new String(header.value());
                    break;
                default:
                    targetRecordHeaders.put(header.key(), new String(header.value()));
                    break;
            }
        }
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

        if (null == sourceConsumerRecord) {
            return null;
        }
        extractTargetHeaderInfo(sourceConsumerRecord);
        final byte[] value = sourceConsumerRecord.value();
        MessagePayload<P> payload = new MessagePayload<P>((P) value);

        final StringBuilder headers = new StringBuilder().append("[");
                targetRecordHeaders.getAll().forEach((k, v) -> headers.append("[" + k + ":" + v + "]"));
                headers.append("]");
        final ConsumerRecord<P> targetConsumerRecord = new ConsumerRecord<P>(sourceConsumerRecord.key(),
        targetRecordHeaders,
        payload,
        targetRecordTopic,
        targetRecordTenantGroup,
        sourceConsumerRecord.partition(),
        sourceConsumerRecord.offset(),
        sourceConsumerRecord.timestamp());
        return targetConsumerRecord;
    }
}

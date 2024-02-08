/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
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
        Adapter<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>, ConsumerRecord<P>> {

    class TargetRecord {
        private com.opendxl.databus.entities.Headers targetRecordHeaders;
        private String targetRecordTopic;
        private String targetRecordTenantGroup;

        TargetRecord() {
            targetRecordHeaders = new com.opendxl.databus.entities.Headers();
            targetRecordTopic = "";
            targetRecordTenantGroup = "";
        }

        public com.opendxl.databus.entities.Headers getTargetRecordHeaders() {
            return targetRecordHeaders;
        }

        public void putTargetRecordHeader(final String headerKey, final String value) {
            targetRecordHeaders.put(headerKey, value);
        }

        public String getTargetRecordTopic() {
            return targetRecordTopic;
        }

        public void setTargetRecordTopic(String targetRecordTopic) {
            this.targetRecordTopic = targetRecordTopic;
        }

        public String getTargetRecordTenantGroup() {
            return targetRecordTenantGroup;
        }

        public void setTargetRecordTenantGroup(String targetRecordTenantGroup) {
            this.targetRecordTenantGroup = targetRecordTenantGroup;
        }

    }

    /**
     * Constructor
     */
    public ConsumerJSONRecordAdapter() {
    }

    private TargetRecord extractTargetHeaderInfo(
            final org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]> sourceConsumerRecord) {
        TargetRecord targetRecord = new TargetRecord();
        targetRecord.setTargetRecordTopic(null != sourceConsumerRecord.topic() ? sourceConsumerRecord.topic() : "");
        Header header = null;
        if (null != (header = sourceConsumerRecord.headers().lastHeader(HeaderInternalField.TOPIC_NAME_KEY))) {
            targetRecord.setTargetRecordTopic(new String(header.value()));
        }
        if (null != (header = sourceConsumerRecord.headers().lastHeader(HeaderInternalField.TENANT_GROUP_NAME_KEY))) {
            targetRecord.setTargetRecordTenantGroup(new String(header.value()));
        }
        sourceConsumerRecord.headers().remove(HeaderInternalField.TOPIC_NAME_KEY);
        sourceConsumerRecord.headers().remove(HeaderInternalField.TENANT_GROUP_NAME_KEY);
        sourceConsumerRecord.headers().remove(HeaderInternalField.MESSAGE_FORMAT_KEY);
        Iterator<Header> iterator = sourceConsumerRecord.headers().iterator();
        while (iterator.hasNext()) {
            header = iterator.next();
            targetRecord.putTargetRecordHeader(header.key(), new String(header.value()));
        }
        return targetRecord;
    }

    /**
     * Adapter pattern implementation for ConsumerRecord.
     * Adapts a DatabusMessage object to a ConsumerRecord.
     *
     * @param sourceConsumerRecord the ConsumerRecord from Kafka that needs be
     *                             adapted to JSON format.
     * @return A Databus {@link ConsumerRecord}
     */
    @Override
    public ConsumerRecord<P> adapt(
            final org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]> sourceConsumerRecord) {
        TargetRecord targetRecord = extractTargetHeaderInfo(sourceConsumerRecord);
        final byte[] value = sourceConsumerRecord.value();
        MessagePayload<P> payload = new MessagePayload<P>((P) value);
        final ConsumerRecord<P> targetConsumerRecord = new ConsumerRecord<P>(sourceConsumerRecord.key(),
                targetRecord.getTargetRecordHeaders(),
                payload,
                targetRecord.getTargetRecordTopic(),
                targetRecord.getTargetRecordTenantGroup(),
                sourceConsumerRecord.partition(),
                sourceConsumerRecord.offset(),
                sourceConsumerRecord.timestamp());
        return targetConsumerRecord;
    }
}

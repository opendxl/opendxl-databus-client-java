/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer;

import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.entities.RoutingData;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class ProducerRecordTest {

    private static final String TENANT_GROUP = "default";

    @Test(expected = DatabusClientRuntimeException.class)
    public void throwExceptionWhenRoutingDataObjectIsNull() {
        Headers headers = new Headers();
        MessagePayload<byte[]> messagePayload = new MessagePayload<>(new String().getBytes());
        new ProducerRecord<>(null, headers, messagePayload);
    }

    @Test
    public void shouldGetAnEmptyHeaderWhenHeadersObjectIsNull() {
        try {
            String topic = "topic1";
            String shardingKey = "key1";
            RoutingData rData = new RoutingData(topic, shardingKey, TENANT_GROUP);
            MessagePayload<byte[]> messagePayload = new MessagePayload<>(new String().getBytes());
            ProducerRecord record1 = new ProducerRecord(rData, null, messagePayload);
            Assert.assertTrue(record1.getHeaders().getAll().isEmpty());

        } catch(Exception e) {
            Assert.fail();
        }
    }

    @Test(expected = DatabusClientRuntimeException.class)
    public void throwExceptionWhenPayloadObjectIsNull() {
        Headers headers = new Headers();
        headers.put("tenantId", "23452145-23452435-3245432");
        headers.put("sourceId", "578-790-870-363265");
        String topic = "topic1";
        String shardingKey = "key1";
        RoutingData rData = new RoutingData(topic, shardingKey, TENANT_GROUP);
        new ProducerRecord(rData, headers, null);
    }

    @Test
    public void AssignOneProducerRecordObjToAnotherToValidateAndCompareEachField() {
        String topic = "topic1";
        String shardingKey = "key1";

        RoutingData rData = new RoutingData(topic, shardingKey, TENANT_GROUP);

        Headers headers = new Headers();
        headers.put("tenantId", "23452145-23452435-3245432");
        headers.put("sourceId", "578-790-870-363265");

        String message = new String("Hello World");
        byte[] payload = message.getBytes();

        MessagePayload<byte[]> messagePayload = new MessagePayload<>(payload);

        ProducerRecord<byte[]> record1 = new ProducerRecord(rData, headers, messagePayload);
        Assert.assertTrue(record1 != null);

        ProducerRecord<byte[]> record2 = new ProducerRecord(rData, headers, messagePayload);
        Assert.assertTrue(record2 != null);

        Assert.assertEquals(record1.getRoutingData().getTopic(), record2.getRoutingData().getTopic());
        Assert.assertEquals(record1.getRoutingData().getShardingKey(), record2.getRoutingData().getShardingKey());
        Assert.assertEquals(record1.getRoutingData().getTenantGroup(), record2.getRoutingData().getTenantGroup());
        Assert.assertEquals(record1.getRoutingData().getPartition(), record2.getRoutingData().getPartition());
        Assert.assertTrue(Arrays.equals(record1.payload().getPayload(),record2.payload().getPayload()));

        Assert.assertTrue(record1.equals(record1));
    }
}

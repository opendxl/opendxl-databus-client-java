/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.internal.DatabusMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MessageSerializationTest {

    @Test
    public void shouldSerializeAndDeserializeADatabusMessage() {
        final String topic = "MyTopic";
        try {
            AvroMessageSerializer ser = new AvroMessageSerializer(AvroV1MessageSchema.getSchema());
            AvroMessageDeserializer des = new AvroMessageDeserializer(AvroV1MessageSchema.getSchema());

            Map<String,String> map = new HashMap();
            map.put("key1","value1");
            String message = new String("Hello World!");
            byte[] payload = message.getBytes();
            Headers headers = new Headers(map);
            DatabusMessage actualMessage = new DatabusMessage(new Headers(map),payload);

            byte[] serializedMessage = ser.serialize(actualMessage);
            DatabusMessage deserializedMessage = des.deserialize(topic, serializedMessage);

            Assert.assertTrue(Arrays.equals(deserializedMessage.getPayload(),payload));
            Assert.assertTrue(deserializedMessage.getHeaders().getAll().equals(headers.getAll()));
            Assert.assertTrue(new String(deserializedMessage.getPayload()).equals(message));

        } catch (Exception e ) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void shouldSupportNullValueForHeaders() {
        try {
            String topic = "topic1";
            MessageSerializer serializer = new MessageSerializer();
            Map<String, String> map = new HashMap<>();
            map.put("field1",null);
            Headers headers = new Headers(map);
            byte[] payload = new byte[]{0};
            DatabusMessage expectedMessage = new DatabusMessage(headers,payload);
            byte[] serializedMessage = serializer.serialize(topic,expectedMessage);

            MessageDeserializer deserializer = new MessageDeserializer();
            DatabusMessage actualMessage = deserializer.deserialize(topic,serializedMessage);

            Assert.assertTrue(actualMessage.equals(expectedMessage));
        } catch (Exception e) {
            Assert.fail("Exception is not expected");
        }
    }
}

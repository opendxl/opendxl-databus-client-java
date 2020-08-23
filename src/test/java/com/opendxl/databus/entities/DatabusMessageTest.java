/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package com.opendxl.databus.entities;



import com.opendxl.databus.common.HeadersField;
import com.opendxl.databus.entities.internal.DatabusMessage;
import org.apache.commons.io.Charsets;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class DatabusMessageTest {

    private static final String SOURCE_ID_VALUE = "1ECB1B90-0820-41DB-BCF0-9FC06D8F4427";
    private static final String TENANT_ID_VALUE = "BE7C0BE1-D2A2-4547-A567-3C5E745E7A04";

    @Test
    public void shouldGetEmptyValues() {
        try {
            DatabusMessage databusMessage = new DatabusMessage(null, null);
            byte[] bytes = new byte[0];
            Assert.assertTrue(Arrays.equals(databusMessage.getPayload(),bytes));
            Assert.assertTrue(databusMessage.getHeaders().getAll().isEmpty());
        } catch (Exception e) {
            Assert.fail("An exception is not expected");
        }
    }

    @Test
    public void shouldGetValidNotEmptyValues() {
        try {

            String message = "Hello World!";
            byte[] payload = message.getBytes();

            Map<String, String> headersMap = new HashMap<>();
            headersMap.put(HeadersField.SOURCE_ID,SOURCE_ID_VALUE );
            headersMap.put(HeadersField.TENANT_ID,TENANT_ID_VALUE );
            Headers headers = new Headers(headersMap);

            DatabusMessage databusMessage = new DatabusMessage(headers,payload);
            Assert.assertTrue(databusMessage.getPayload() != new byte[0]);
            Assert.assertTrue(!databusMessage.getHeaders().getAll().isEmpty());

            Assert.assertTrue(Arrays.equals(databusMessage.getPayload(),payload));
            Assert.assertTrue(databusMessage.getHeaders().getAll().get(HeadersField.SOURCE_ID).equals(SOURCE_ID_VALUE));
            Assert.assertTrue(databusMessage.getHeaders().getAll().get(HeadersField.TENANT_ID).equals(TENANT_ID_VALUE));

        } catch (Exception e) {
            Assert.fail("An exception is not expected");
        }
    }

    @Test
    public void shouldGetHashCode() {
        try {
            String message = "Hello World!";
            byte[] payload = message.getBytes();

            Map<String, String> headersMap = new HashMap<>();
            headersMap.put(HeadersField.SOURCE_ID,SOURCE_ID_VALUE );
            headersMap.put(HeadersField.TENANT_ID,TENANT_ID_VALUE );
            Headers headers = new Headers(headersMap);

            DatabusMessage dm1 = new DatabusMessage(headers,payload);
            DatabusMessage dm2 = new DatabusMessage(headers,payload);
            DatabusMessage dm3 = new DatabusMessage(headers,"Hello".getBytes());
            DatabusMessage dm4 = new DatabusMessage(null,payload);

            Assert.assertTrue(dm1.equals(dm2));
            Assert.assertTrue(dm1.hashCode() == dm2.hashCode());

            Assert.assertTrue(!dm1.equals(dm3));
            Assert.assertTrue(dm1.hashCode() != dm3.hashCode());

            Assert.assertTrue(!dm1.equals(dm4));
            Assert.assertTrue(dm1.hashCode() != dm4.hashCode());

        } catch (Exception e) {
            Assert.fail("An exception is not expected "+ e.getMessage() );
        }
    }

    @Test
    public void equals_should_return_true_when_comparing_the_same_databusMessage() {
        String message = "Hello World!";
        byte[] payload = message.getBytes();

        Map<String, String> headersMap = new HashMap<>();
        headersMap.put(HeadersField.SOURCE_ID,SOURCE_ID_VALUE );
        headersMap.put(HeadersField.TENANT_ID,TENANT_ID_VALUE );
        Headers headers = new Headers(headersMap);

        DatabusMessage databusMessage = new DatabusMessage(headers,payload);

        assertThat("The same databusMessage must be equals to itself", databusMessage.equals(databusMessage), is(true));
    }

    @Test
    public void equals_should_return_true_when_comparing_the_same_databusMessage_when_headers_are_null() {
        String message = "Hello World!";
        byte[] payload = message.getBytes();
        DatabusMessage databusMessage1 = new DatabusMessage(null,payload);
        DatabusMessage databusMessage2 = new DatabusMessage(null,payload);

        assertThat("The same databusMessage must be equals when the payload is equals and the headers are null",
                databusMessage1.equals(databusMessage2), is(true));
    }

    @Test
    public void equals_should_return_true_when_comparing_the_same_databusMessage_when_payload_is_null() {
        Map<String, String> headersMap = new HashMap<>();
        headersMap.put(HeadersField.SOURCE_ID,SOURCE_ID_VALUE );
        headersMap.put(HeadersField.TENANT_ID,TENANT_ID_VALUE );
        Headers headers = new Headers(headersMap);
        DatabusMessage databusMessage1 = new DatabusMessage(headers,null);
        DatabusMessage databusMessage2 = new DatabusMessage(headers,null);

        assertThat("The same databusMessage must be equals when the payload is null and the headers are equals"
                , databusMessage1.equals(databusMessage2), is(true));
    }

    @Test
    public void equals_should_return_false_when_comparing_to_null() {
        DatabusMessage databusMessage = new DatabusMessage(null,null);
        assertThat("The same databusMessage must be not equals to null", databusMessage.equals(null), is(false));
    }

    @Test
    public void equals_should_return_false_when_comparing_to_a_different_object_type() {
        DatabusMessage databusMessage = new DatabusMessage(null,null);
        assertThat("The same databusMessage must be not equals to a different object type", databusMessage.equals(""), is(false));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void databus_message_should_be_serializable() throws Exception {
        Map<String, String> headersMap = new HashMap<>();
        headersMap.put(HeadersField.SOURCE_ID,SOURCE_ID_VALUE );
        headersMap.put(HeadersField.TENANT_ID,TENANT_ID_VALUE );
        Headers headers = new Headers(headersMap);
        DatabusMessage databusMessage = new DatabusMessage(headers, "test".getBytes(Charsets.UTF_8));
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream);
            outputStream.writeObject(databusMessage);
            outputStream.close();
            ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
            DatabusMessage actual = (DatabusMessage)inputStream.readObject();
            inputStream.close();
            assertThat("The same databusMessage must be equals to itself after serialization", actual.equals(databusMessage), is(true));
            assertThat("The same databusMessage must implements Serializable", actual instanceof Serializable, is(true));
        } catch (Exception e) {
            Assert.fail("An exception is not expected: " + e.getMessage() );
        }
    }
}
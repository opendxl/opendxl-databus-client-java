/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.entities.internal;


import com.opendxl.databus.consumer.ConsumerRecord;
import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.producer.DatabusProducer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

/**
 * It represents the message sent thru Databus. It contains a {@link Headers}
 * and payload as byte[]
 * <p>
 * This class is for internal use of Databus-SDK. It is created and kept as a member
 * of {@link ConsumerRecord} and it is used when the
 * Databus-SDK's user invoke {@link DatabusProducer}.send() method
 * </p>
 */
public class DatabusMessage implements Serializable {

    /**
     * The map of Headers contained in the Databus Message
     */
    private  Map<String, String> headers;

    /**
     * The byte payload which is the data itself for the DatabusMessage
     */
    private  byte[] payload;

    private DatabusMessage() {
    }

    /**
     * A DatabusMessage is created by providing a {@link Headers} and a payload as byte[].
     * If they are not provided, empty instances of them will be created.
     *
     * @param headers key-value map that contains message headers
     * @param payload payload as byte[]
     */
    public DatabusMessage(final Headers headers, final byte[] payload) {
        this.headers = Optional.ofNullable(headers).orElse(new Headers()).getAll();
        this.payload = Optional.ofNullable(payload).orElse(new byte[0]);
    }

    /**
     * Gets the headers of a DatabusMessage instance
     *
     * @return {@link Headers} of the message
     */
    public Headers getHeaders() {
        Headers clonedHeaders = new Headers();
        headers.forEach((k, v) -> clonedHeaders.put(k, v));
        return clonedHeaders;
    }

    /**
     * Delete an specific header passing a header name as a parameter
     *
     * @param key the key to be removed from headers
     */
    public void removeHeader(final String key) {
        headers.remove(key);
    }

    /**
     * Gets the payload data
     *
     * @return A payload as array of byte
     */
    public byte[] getPayload() {
        return payload;
    }


    /**
     * Overrides equals method for DatabusMessage
     *
     * @param obj getInstance to be compared to
     * @return true if both objects are equals
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (!DatabusMessage.class.isAssignableFrom(obj.getClass())) {
            return false;
        }
        final DatabusMessage other = (DatabusMessage) obj;
        if ((this.headers == null) ? (other.headers != null) : !this.headers.equals(other.headers)) {
            return false;
        }

        if ((this.payload == null) ? (other.payload != null) : !Arrays.equals(this.payload, other.payload)) {
            return false;
        }
        return true;
    }

    /**
     * Overrides equals method for DatabusMessage
     *
     * @return HashCode as a product of payload hashcode and headers hashcode
     */
    @Override
    public int hashCode() {
        return payload.hashCode() * headers.hashCode();
    }


}

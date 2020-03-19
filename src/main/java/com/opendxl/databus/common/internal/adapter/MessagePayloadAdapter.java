/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/


package com.opendxl.databus.common.internal.adapter;


import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.entities.internal.DatabusMessage;
import com.opendxl.databus.serialization.Serializer;

/**
 * Adapter for Message Payload
 *
 * @param <P> payload's type
 */
public final class MessagePayloadAdapter<P> {

    /**
     * The message deserializer.
     */
    private final Serializer<P> userSerializer;

    /**
     * Constructor
     *
     * @param userSerializer A {@link Serializer} instance
     * or creating a {@link DatabusMessage}.
     */
    public MessagePayloadAdapter(final Serializer<P> userSerializer) {

        this.userSerializer = userSerializer;
    }

    /**
     * Adapter pattern implementation for DatabusMessage instance.
     * Adapts a MessagePayload to a DatabusMessage instance.
     *
     * @param messagePayload a {@link MessagePayload} instance to be adapted.
     * @return a {@link DatabusMessage} instance.
     * @param headers headers
     */
    public DatabusMessage adapt(final MessagePayload<P> messagePayload, final Headers headers) {
        final byte[] payload = userSerializer.serialize(messagePayload.getPayload());
        return new DatabusMessage(headers, payload);
    }

}

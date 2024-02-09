/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
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
public final class MessagePayloadAdapter<P> implements Adapter<MessagePayload<P>, DatabusMessage> {

    /**
     * The message deserializer.
     */
    private final Serializer<P> messageSerializer;

    /**
     * The headers map.
     */
    private final Headers headers;

    /**
     * Constructor
     *
     * @param messageSerializer A {@link Serializer} instance
     * or creating a {@link DatabusMessage}.
     * @param headers Headers map.
     */
    public MessagePayloadAdapter(final Serializer<P> messageSerializer,
                                 final Headers headers) {

        this.messageSerializer = messageSerializer;
        this.headers = headers;
    }

    /**
     * Adapter pattern implementation for DatabusMessage instance.
     * Adapts a MessagePayload to a DatabusMessage instance.
     *
     * @param messagePayload a {@link MessagePayload} instance to be adapted.
     * @return a {@link DatabusMessage} instance.
     */
    @Override
    public DatabusMessage adapt(final MessagePayload<P> messagePayload) {
        final byte[] payload = messageSerializer.serialize(messagePayload.getPayload());
        return new DatabusMessage(headers, payload);
    }

}

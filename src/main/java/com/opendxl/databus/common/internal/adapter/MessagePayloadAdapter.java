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
public final class MessagePayloadAdapter<P> implements Adapter<MessagePayload<P>, DatabusMessage> {

    private final Serializer<P> messageSerializer;
    private final Headers headers;

    /**
     * @param messageSerializer A {@link Serializer} getInstance
     *                          for creating a {@link DatabusMessage}
     * @param headers           headers
     */

    public MessagePayloadAdapter(final Serializer<P> messageSerializer,
                                 final Headers headers) {

        this.messageSerializer = messageSerializer;
        this.headers = headers;
    }

    /**
     * @param messagePayload a {@link MessagePayload} getInstance to be adapted
     * @return a {@link DatabusMessage} getInstance
     */
    @Override
    public DatabusMessage adapt(final MessagePayload<P> messagePayload) {
        final byte[] payload = messageSerializer.serialize(messagePayload.getPayload());
        return new DatabusMessage(headers, payload);
    }

}

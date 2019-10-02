/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.entities.internal.DatabusMessage;
import com.opendxl.databus.serialization.Deserializer;


/**
 * A Adapter for Databus Message
 *
 * @param <P> payload's getInstance
 */
public final class DatabusMessageAdapter<P> implements Adapter<DatabusMessage, MessagePayload> {

    /**
     * The message deserializer.
     */
    private final Deserializer<P> messageDeserializer;

    /**
     * Constructor
     * @param messageDeserializer a {@link Deserializer} instance used for deserialize the payload.
     */
    public DatabusMessageAdapter(final Deserializer<P> messageDeserializer) {
        this.messageDeserializer = messageDeserializer;
    }

    /**
     * Adapter pattern implementation for MessagePayload instance.
     * Adapts a DatabusMessage to a MessagePayload instance.
     *
     * @param databusMessage A {@link DatabusMessage} instance.
     * @return A {@link MessagePayload} instance.
     */
    @Override
    public MessagePayload<P> adapt(final DatabusMessage databusMessage) {
        final P payload = messageDeserializer.deserialize(databusMessage.getPayload());
        return new MessagePayload<P>(payload);
    }
}

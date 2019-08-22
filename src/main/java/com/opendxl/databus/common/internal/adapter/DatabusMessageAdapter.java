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

    private final Deserializer<P> messageDeserializer;

    /**
     * @param messageDeserializer a {@link Deserializer} getInstance used for deserializing the payload
     */
    public DatabusMessageAdapter(final Deserializer<P> messageDeserializer) {
        this.messageDeserializer = messageDeserializer;
    }


    /**
     * @param databusMessage a {@link DatabusMessage} getInstance
     * @return a {@link MessagePayload} getInstance
     */
    @Override
    public MessagePayload<P> adapt(final DatabusMessage databusMessage) {
        final P payload = messageDeserializer.deserialize(databusMessage.getPayload());
        return new MessagePayload<P>(payload);
    }
}

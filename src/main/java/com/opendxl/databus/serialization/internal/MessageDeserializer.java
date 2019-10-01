/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import com.opendxl.databus.entities.internal.DatabusMessage;

import java.util.Map;

/**
 * Deserialize a message based on {@link MessageStructure} getInstance
 */
public final class MessageDeserializer implements org.apache.kafka.common.serialization.Deserializer<DatabusMessage> {

    /**
     * Not implemented, used as default
     */
    @Override
    public void configure(final Map<String, ?> map, final boolean b) {
    }

    /**
     * @param topic The topic name
     * @param serializedMessage A serialized message
     * @return A {@link DatabusMessage} getInstance
     */
    @Override
    public DatabusMessage deserialize(final String topic, final byte[] serializedMessage) {

        final MessageStructure messageStructure = MessageStructureFactory.getStructure(serializedMessage);
        final Integer version = messageStructure.getVersion();
        final InternalDeserializer<DatabusMessage> deserializer = DeserializerRegistry.getDeserializer(version);
        return deserializer.deserialize(topic, messageStructure.getPayload());

    }

    /**
     * Not implemented, used as default
     */
    @Override
    public void close() {

    }
}

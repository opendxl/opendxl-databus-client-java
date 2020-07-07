/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import com.opendxl.databus.entities.TierStorage;
import com.opendxl.databus.entities.internal.DatabusMessage;

import java.util.Map;

/**
 * Deserialize a message based on {@link MessageStructure} getInstance
 */
public final class MessageDeserializer implements org.apache.kafka.common.serialization.Deserializer<DatabusMessage> {


    /**
     * Tier Storage
     */
    private TierStorage tierStorage;

    /**
     * Constructor
     *
     * @param tierStorage If null it will be ignored and payload won't be read
     */
    public MessageDeserializer(final TierStorage tierStorage) {
        this.tierStorage = tierStorage;
    }

    /**
     * Constructor
     */
    public MessageDeserializer() {
        this(null);
    }


    /**
     * Not implemented.
     */
    @Override
    public void configure(final Map<String, ?> map, final boolean b) {
    }

    /**
     * Deserialize a message to a {@link DatabusMessage}
     * If tierStorage is not null will be used to read the payload from the underlying Tier Storage.
     *
     * @param topic The topic name.
     * @param serializedMessage A serialized message.
     * @return A {@link DatabusMessage} instance.
     */
    @Override
    public DatabusMessage deserialize(final String topic, final byte[] serializedMessage) {

        final MessageStructure messageStructure = MessageStructureFactory.getStructure(serializedMessage);
        final Integer version = messageStructure.getVersion();
        final InternalDeserializer<DatabusMessage> deserializer = DeserializerRegistry.getDeserializer(version);
        return deserializer.deserialize(topic, messageStructure.getPayload(), tierStorage);

    }

    /**
     * Not implemented.
     */
    @Override
    public void close() {

    }
}

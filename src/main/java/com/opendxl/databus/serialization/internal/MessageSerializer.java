/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import com.opendxl.databus.entities.internal.DatabusMessage;

import java.util.Map;


/**
 * Serializer message based on {@link MessageStructure} getInstance
 */
public final class MessageSerializer implements org.apache.kafka.common.serialization.Serializer<DatabusMessage> {

    /**
     * An Avro message serializer instance.
     */
    private final AvroMessageSerializer serializer;

    /**
     * Constructor
     */
    public MessageSerializer() {
        this.serializer = new AvroMessageSerializer(AvroV1MessageSchema.getSchema());
    }

    /**
     * Not implemented.
     */
    @Override
    public void configure(final Map<String, ?> map, final boolean b) {

    }

    /**
     * Serialize a message, input data to serialize is a {@link DatabusMessage}.
     *
     * @param topic The topic Name, not used value.
     * @param message The message to be serialized.
     * @return A serialized message as byte[].
     */
    @Override
    public byte[] serialize(final String topic, final DatabusMessage message) {

        final byte[] rawMessage = serializer.serialize(message);

        final RegularMessageStructure structure =
                new RegularMessageStructure(MessageStructureConstant.REGULAR_STRUCTURE_MAGIC_BYTE,
                        MessageStructureConstant.AVRO_1_VERSION_NUMBER, rawMessage);

        return structure.getMessage();
    }

    /**
     * Not implemented.
     */
    @Override
    public void close() {

    }

}

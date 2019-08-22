/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import com.opendxl.databus.entities.internal.DatabusMessage;

import java.util.Map;


/**
 * Serializer message based on {@link MessageStructure} getInstance
 */
public final class MessageSerializer implements org.apache.kafka.common.serialization.Serializer<DatabusMessage> {

    private final AvroMessageSerializer serializer;

    public MessageSerializer() {
        this.serializer = new AvroMessageSerializer(AvroV1MessageSchema.getSchema());
    }

    @Override
    public void configure(final Map<String, ?> map, final boolean b) {

    }

    /**
     * @param topic   Not Used
     * @param message to be serialized
     * @return a serialized message as byte[]
     */
    @Override
    public byte[] serialize(final String topic, final DatabusMessage message) {

        final byte[] rawMessage = serializer.serialize(message);

        final RegularMessageStructure structure =
                new RegularMessageStructure(MessageStructureConstant.REGULAR_STRUCTURE_MAGIC_BYTE,
                        MessageStructureConstant.AVRO_1_VERSION_NUMBER, rawMessage);

        return structure.getMessage();
    }

    @Override
    public void close() {

    }

}

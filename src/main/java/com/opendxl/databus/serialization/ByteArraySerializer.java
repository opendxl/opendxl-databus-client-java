/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization;

/**
 * Byte Array Serializer.
 */
public final class ByteArraySerializer implements Serializer<byte[]> {

    /**
     * Serialize a message.
     *
     * @param message Message to be serialized.
     * @return Message as byte array.
     */
    @Override
    public byte[] serialize(final byte[] message) {
        return message;
    }
}

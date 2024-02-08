/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization;

/**
 * Byte Array Deserializer.
 */
public final class ByteArrayDeserializer implements Deserializer<byte[]> {

    /**
     * Deserialize a message.
     *
     * @param message Message to be deserialized.
     * @return Message as byte array.
     */
    @Override
    public byte[] deserialize(final byte[] message) {
        return message;
    }
}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization;

/**
 * Byte Array Serializer
 */
public final class ByteArraySerializer implements Serializer<byte[]> {

    /**
     * Serialize a message
     * @param message message to be serialized
     * @return message as byte array
     */
    @Override
    public byte[] serialize(final byte[] message) {
        return message;
    }
}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization;

/**
 * Byte Array Deserializer
 */
public final class ByteArrayDeserializer implements Deserializer<byte[]> {

    /**
     *
     * @param message message to be deserialized
     * @return message as byte array
     */
    @Override
    public byte[] deserialize(final byte[] message) {
        return message;
    }
}

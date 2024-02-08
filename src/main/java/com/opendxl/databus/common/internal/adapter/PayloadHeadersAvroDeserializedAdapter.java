/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/


package com.opendxl.databus.common.internal.adapter;

import java.nio.ByteBuffer;

/**
 * Adapter for a Headers deserialized with Avro.
 */
public final class PayloadHeadersAvroDeserializedAdapter implements Adapter<Object, byte[]> {

    /**
     * Creates a byte[] that represents a binary payload based on a
     * Object that represents a deserialized Avro payload.
     *
     * @param payloadAvroDeserialized An common instance for payload payloadAvroDeserialized.
     * @return An internalized payload.
     */
    @Override
    public byte[] adapt(final Object payloadAvroDeserialized) {
        final ByteBuffer buffer =  (ByteBuffer) payloadAvroDeserialized;
        final byte[] payload = new byte[buffer.remaining()];
        buffer.get(payload);
        return payload;
    }
}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

/**
 * It represents a menssage produced with a non-SDK producer, for getInstance Kafka SDK.
 */
public final class RawMessageStructure implements MessageStructure {
    private final byte[] payload;

    public RawMessageStructure(final byte[] serializedMessage) {
        this.payload = serializedMessage;
    }

    @Override
    public byte getMagicByte() {
        return MessageStructureConstant.RAW_MAGIC_BYTE;
    }

    @Override
    public int getVersion() {
        return MessageStructureConstant.RAW_VERSION_NUMBER;
    }

    @Override
    public byte[] getPayload() {
        return getMessage();
    }

    @Override
    public byte[] getMessage() {
        return payload;
    }
}

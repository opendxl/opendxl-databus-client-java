/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

/**
 * Represents a message produced with a non-SDK producer, for getInstance Kafka SDK.
 */
public final class RawMessageStructure implements MessageStructure {

    /**
     * The payload.
     */
    private final byte[] payload;

    /**
     * The Constructor.
     *
     * @param serializedMessage A serialized message.
     */
    public RawMessageStructure(final byte[] serializedMessage) {
        this.payload = serializedMessage;
    }

    /**
     * Gets the magic byte.
     *
     * @return The magic byte.
     */
    @Override
    public byte getMagicByte() {
        return MessageStructureConstant.RAW_MAGIC_BYTE;
    }

    /**
     * Gets the version.
     *
     * @return The version.
     */
    @Override
    public int getVersion() {
        return MessageStructureConstant.RAW_VERSION_NUMBER;
    }

    /**
     * Gets the payload.
     *
     * @return The payload.
     */
    @Override
    public byte[] getPayload() {
        return getMessage();
    }

    /**
     * Gets the message.
     *
     * @return The message.
     */
    @Override
    public byte[] getMessage() {
        return payload;
    }
}

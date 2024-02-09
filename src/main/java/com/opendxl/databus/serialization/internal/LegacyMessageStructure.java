/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

/**
 * Represent a Legacy Message Structura based on JSON
 * <p>
 * For example: {"headers":{"sourceId":"abc","tenantId":"TENANT10"},
 * "payloadBase64String":"wqFIb2xhISBIdWdvIGFxdcOtLg=="}
 */
public class LegacyMessageStructure implements MessageStructure {

    private final byte[] payload;

    /**
     * Constructor.
     *
     * @param serializedMessage A serialized message.
     */
    public LegacyMessageStructure(final byte[] serializedMessage) {
        this.payload = serializedMessage;
    }

    /**
     * Gets the magic byte.
     *
     * @return A magic byte.
     */
    @Override
    public byte getMagicByte() {
        return MessageStructureConstant.LEGACY_STRUCTURE_MAGIC_BYTE;
    }

    /**
     * Gets the message structure version.
     *
     * @return The message structure version.
     */
    @Override
    public int getVersion() {
        return MessageStructureConstant.LEGACY_VERSION_NUMBER;
    }

    /**
     * Gets the message.
     *
     * @return The message itself.
     */
    @Override
    public byte[] getMessage() {
        return getPayload();
    }


    /**
     * Gets the full payload
     *
     * @return The payload.
     */
    @Override
    public byte[] getPayload() {
        return payload;
    }
}

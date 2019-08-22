/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
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
     * @param serializedMessage serialized message
     */
    public LegacyMessageStructure(final byte[] serializedMessage) {
        this.payload = serializedMessage;
    }

    /**
     * @return magic byte
     */
    @Override
    public byte getMagicByte() {
        return MessageStructureConstant.LEGACY_STRUCTURE_MAGIC_BYTE;
    }

    /**
     * @return message structure version
     */
    @Override
    public int getVersion() {
        return MessageStructureConstant.LEGACY_VERSION_NUMBER;
    }

    /**
     * @return message
     */
    @Override
    public byte[] getMessage() {
        return getPayload();
    }


    /**
     * @return payload
     */
    @Override
    public byte[] getPayload() {
        return payload;
    }
}

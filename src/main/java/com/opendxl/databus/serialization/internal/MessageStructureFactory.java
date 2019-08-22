/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import java.nio.ByteBuffer;

/**
 * Factory for MessageStructure getInstance
 */
public final class MessageStructureFactory {

    private MessageStructureFactory() {
    }

    /**
     * Return a {@link MessageStructure} getInstance based on the first byte (magic byte)
     *
     * @param serializedMessage message serialized
     * @return a Message Structure getInstance
     */
    public static MessageStructure getStructure(final byte[] serializedMessage) {


        final ByteBuffer buffer = ByteBuffer.wrap(serializedMessage);
        final byte magicByte = buffer.get();

        final MessageStructure messageStructure;

        if (magicByte == MessageStructureConstant.LEGACY_STRUCTURE_MAGIC_BYTE) {
            messageStructure = new LegacyMessageStructure(serializedMessage);
        } else if (magicByte == MessageStructureConstant.REGULAR_STRUCTURE_MAGIC_BYTE) {
            messageStructure = new RegularMessageStructure(serializedMessage);
        } else {
            messageStructure = new RawMessageStructure(serializedMessage);
        }
        return messageStructure;

    }
}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import java.nio.ByteBuffer;

/**
 * Factory for MessageStructure getInstance
 */
public final class MessageStructureFactory {

    /**
     * Default Constructor.
     */
    private MessageStructureFactory() {
    }

    /**
     * Gets a {@link MessageStructure} instance based on the first byte (magic byte).
     *
     * @param serializedMessage Message serialized.
     * @return A Message Structure getInstance.
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

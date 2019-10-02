/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import com.opendxl.databus.entities.internal.DatabusMessage;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents a {@link DatabusMessage} structure.
 */
public final class RegularMessageStructure implements MessageStructure {

    /**
     * The payload position in the byte array message.
     */
    private static final int PAYLOAD_START_POSITION = 5;

    /**
     * The size in bytes of the version.
     */
    private static final int VERSION_SIZE = 4;

    /**
     * The size in bytes of the magic byte.
     */
    private static final int MAGIC_BYTE_SIZE = 1;

    /**
     * The version.
     */
    private final int version;

    /**
     * The payload as byte array.
     */
    private final byte[] payload;

    /**
     * The magic byte.
     */
    private final byte magicByte;

    /**
     * Constructor. Creates a Regular Message Structure based on a serialized message.
     *
     * A regular structure is a byte[] with the following format:
     * |0|0001|payload|
     *
     * Magic Byte: size:1 position:0-0 value: byte 0x0
     * Version   : size:4 position:1-4 value: int 1
     * Payload   : size n position:5-n value: byte[] serialized by the user
     *
     * @param serializedMessage Serialized message as byte array.
     */
    public RegularMessageStructure(final byte[] serializedMessage) {
        final ByteBuffer buffer = ByteBuffer.wrap(serializedMessage);
        this.magicByte = buffer.get();
        this.version = buffer.getInt();
        this.payload = Arrays.copyOfRange(buffer.array(), PAYLOAD_START_POSITION, serializedMessage.length);
    }

    /**
     * Constructor. It creates a Regular Message Structure.
     *
     * @param magicByte The message indicator. See {@link RegularMessageStructure}.
     * @param version The message version.
     * @param payload The message payload.
     */
    public RegularMessageStructure(final byte magicByte, final int version, final byte[] payload) {
        this.magicByte = magicByte;
        this.version = version;
        this.payload = payload;
    }

    /**
     * Gets the magic byte.
     *
     * @return The magic byte.
     */
    @Override
    public byte getMagicByte() {
        return magicByte;
    }

    /**
     * Gets the version.
     *
     * @return The version.
     */
    @Override
    public int getVersion() {
        return version;
    }

    /**
     * Gets the payload.
     *
     * @return The payload.
     */
    @Override
    public byte[] getPayload() {
        return payload;
    }

    /**
     * Gets the message.
     *
     * @return The message.
     */
    @Override
    public byte[] getMessage() {
        return ByteBuffer
                .allocate(MAGIC_BYTE_SIZE + VERSION_SIZE + getPayload().length)
                .put(getMagicByte())
                .putInt(getVersion())
                .put(getPayload()).array();
    }
}

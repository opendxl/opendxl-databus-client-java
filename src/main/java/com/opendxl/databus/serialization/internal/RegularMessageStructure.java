/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represent a Databus message structure.
 */
public final class RegularMessageStructure implements MessageStructure {

    private static final int PAYLOAD_START_POSITION = 5;
    private static final int VERSION_SIZE = 1;
    private static final int MAGIC_BYTE_SIZE = 4;
    private final int version;
    private final byte[] payload;
    private final byte magicByte;

    /**
     * It creates a Regular Message Structure based on a serialized message.
     *
     * A regular structure is a byte[] with the following format:
     * |0|0001|payload|
     *
     * Magic Byte: size:1 position:0-0 value: byte 0x0
     * Version   : size:4 position:1-4 value: int 1
     * Payload   : size n position:5-n value: byte[] serialized by the user
     *
     * @param serializedMessage serialized message
     */
    public RegularMessageStructure(final byte[] serializedMessage) {
        final ByteBuffer buffer = ByteBuffer.wrap(serializedMessage);
        this.magicByte = buffer.get();
        this.version = buffer.getInt();
        this.payload = Arrays.copyOfRange(buffer.array(), PAYLOAD_START_POSITION, serializedMessage.length);
    }

    /**
     * It create a It creates a Regular Message Structure
     *
     * @param magicByte message indicator. See {@link RegularMessageStructure}
     * @param version   message version
     * @param payload   message payload
     */
    public RegularMessageStructure(final byte magicByte, final int version, final byte[] payload) {
        this.magicByte = magicByte;
        this.version = version;
        this.payload = payload;
    }


    /**
     * @return magic byte
     */
    @Override
    public byte getMagicByte() {
        return magicByte;
    }

    /**
     * @return message structure version
     */
    @Override
    public int getVersion() {
        return version;
    }

    /**
     * @return message
     */
    @Override
    public byte[] getPayload() {
        return payload;
    }

    /**
     * @return the complete message as byte[]
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

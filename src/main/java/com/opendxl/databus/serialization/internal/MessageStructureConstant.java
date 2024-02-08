/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

/**
 * Contains a set of constants for Message Structure
 */
public final class MessageStructureConstant {

    private MessageStructureConstant() {
    }

    /**
     * The legacy version number.
     */
    public static final int LEGACY_VERSION_NUMBER = 0;

    /**
     * The Avro version number.
     */
    public static final int AVRO_1_VERSION_NUMBER = 1;

    /**
     * The raw version number.
     */
    public static final int RAW_VERSION_NUMBER = 99;

    /**
     * The structure of the magic byte.
     */
    public static final byte REGULAR_STRUCTURE_MAGIC_BYTE = 0x0;

    /**
     * The legacy structure of the magic byte.
     */
    public static final byte LEGACY_STRUCTURE_MAGIC_BYTE = (byte) '{';

    /**
     * The raw magic byte.
     */
    public static final byte RAW_MAGIC_BYTE = (byte) '1';
}

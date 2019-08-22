/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

/**
 * Contains a set of constants for Message Structure
 */
public final class MessageStructureConstant {

    private MessageStructureConstant() {
    }

    public static final int LEGACY_VERSION_NUMBER = 0;
    public static final int AVRO_1_VERSION_NUMBER = 1;
    public static final int RAW_VERSION_NUMBER = 99;
    public static final byte REGULAR_STRUCTURE_MAGIC_BYTE = 0x0;
    public static final byte LEGACY_STRUCTURE_MAGIC_BYTE = (byte) '{';
    public static final byte RAW_MAGIC_BYTE = (byte) '1';
}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

/**
 * A common interface for the message structure.
 */
public interface MessageStructure {

    /**
     * Gets the magic byte.
     *
     * @return The magic byte.
     */
    byte getMagicByte();

    /**
     * Gets the version.
     * @return The version.
     */
    int getVersion();

    /**
     * Gets the payload.
     *
     * @return The payload.
     */
    byte[] getPayload();

    /**
     * Gets the message.
     *
     * @return The message.
     */
    byte[] getMessage();
}

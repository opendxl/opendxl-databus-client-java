/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

public interface MessageStructure {

    byte getMagicByte();

    int getVersion();

    byte[] getPayload();

    byte[] getMessage();
}

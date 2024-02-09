/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

/**
 * A Internal Message Serializer Interface,
 * Used by SDK to serialize an object ot type P
 *
 * @param <P> The message's type
 */
public interface InternalSerializer<P> {

    /**
     * Serialize data.
     *
     * @param message data to be serialized
     * @return data as byte array
     */
    byte[] serialize(P message);

}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization;

/**
 * A Message Serializer Interface,
 * SDK's users have to implement this in order to be able to produce a Databus message.
 *
 * @param <P> The message's type.
 */
public interface Serializer<P> {

    /**
     * Serialize a message.
     *
     * @param message Data to be serialized.
     * @return Data as byte array.
     */
    byte[] serialize(P message);

}

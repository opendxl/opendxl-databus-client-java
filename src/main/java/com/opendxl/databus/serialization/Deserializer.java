/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization;

/**
 * Deserializer
 * SDK's users have to implement this, in order to be able to consume a Databus message.
 *
 * @param <P> the data's type.
 */
public interface Deserializer<P> {

    /**
     * Deserialize a message.
     *
     * @param data Data to be deserialized.
     * @return Data of type P.
     */
    P deserialize(byte[] data);
}

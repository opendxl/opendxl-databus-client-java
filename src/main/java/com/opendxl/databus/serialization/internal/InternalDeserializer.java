/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

/**
 * Internal Deserializer
 * Used by SDK to deserialize an object of P type,
 *
 * @param <P> the data's type
 */

public interface InternalDeserializer<P> {

    /**
     * Deserialize data.
     *
     * @param topic the topic where the message comes from
     * @param data data to be deserialized
     * @return data of type P
     */
    P deserialize(String topic, byte[] data);

}

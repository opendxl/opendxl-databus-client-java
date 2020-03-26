/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import com.opendxl.databus.entities.TierStorage;

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

    /**
     *
     * @param topic the topic where the message comes from
     * @param data data to be deserialized
     * @param tierStorage tier storage where the payload should be read
     * @return data of type P
     */
    P deserialize(String topic, byte[] data, TierStorage tierStorage);


}

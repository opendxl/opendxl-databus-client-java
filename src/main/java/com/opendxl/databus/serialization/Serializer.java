/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization;

/**
 * A Message Serializer Interface,
 * SDK's users have to implement this in order to be able to produce a Dataus message
 *
 * @param <P> The message's type
 */
public interface Serializer<P> {

    /**
     *
     * @param message data to be serialized
     * @return data as byte array
     */
    byte[] serialize(P message);

}

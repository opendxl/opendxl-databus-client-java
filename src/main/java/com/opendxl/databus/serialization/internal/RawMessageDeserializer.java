/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import com.opendxl.databus.entities.internal.DatabusMessage;

/**
 * Deserialize messages produced by Kafka Client
 */
public final class RawMessageDeserializer implements InternalDeserializer<DatabusMessage> {

    /**
     * It takes binary data from Databus and create a {@link DatabusMessage} with
     * this binary data and empty headers.
     *
     * @param topic Not used
     * @param data data to be deserialized
     * @return a DatabusMessage witw binary data as is.
     */
    @Override
    public DatabusMessage deserialize(final String topic, final byte[] data) {
        return new DatabusMessage(null, data);
    }
}

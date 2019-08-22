/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/


package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.entities.Headers;
import org.apache.avro.util.Utf8;

import java.util.HashMap;
import java.util.Map;

/**
 * Adapter for a Headers deserialized with Avro
 */
public final class HeadersAvroDeserializedAdapter implements Adapter<Object, Headers> {

    /**
     * Create a {@link Headers} based on a Object that represents a map deserialized with Avro
     *
     * @param sourceDeserailizedHeaders deserializedHeaders map
     * @return Databus {@link Headers}
     */
    @Override
    public Headers adapt(final Object sourceDeserailizedHeaders) {

        final Map<String, String> headersMap = new HashMap<>();
        ((Map<Utf8, Utf8>) sourceDeserailizedHeaders).forEach((k, v) -> {
            headersMap.put(new String(k.getBytes()), new String(v.getBytes()));
        });
        return new Headers(headersMap);
    }
}

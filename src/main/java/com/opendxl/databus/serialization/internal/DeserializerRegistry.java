/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;


import com.opendxl.databus.entities.internal.DatabusMessage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * It contains a key-value Collection of message version and Deserializer
 */
public final class DeserializerRegistry {

    private static final Map<Integer, InternalDeserializer<DatabusMessage>> REGISTRY;

    static {
        final Map<Integer, InternalDeserializer<DatabusMessage>> aMap = new HashMap<>();
        aMap.put(MessageStructureConstant.LEGACY_VERSION_NUMBER, new LegacyMessageDeserializer());
        aMap.put(MessageStructureConstant.AVRO_1_VERSION_NUMBER,
                new AvroMessageDeserializer(AvroV1MessageSchema.getSchema()));
        aMap.put(MessageStructureConstant.RAW_VERSION_NUMBER, new RawMessageDeserializer());
        REGISTRY = Collections.unmodifiableMap(aMap);
    }

    private DeserializerRegistry() {
    }


    /**
     * @param version message version
     * @return a {@link InternalDeserializer} getInstance
     */
    public static InternalDeserializer<DatabusMessage> getDeserializer(final Integer version) {
        return REGISTRY.get(version);
    }
}

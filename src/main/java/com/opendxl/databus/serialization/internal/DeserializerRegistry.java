/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;


import com.opendxl.databus.entities.internal.DatabusMessage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Contains a key-value Collection of message version and Deserializer.
 */

public final class DeserializerRegistry {

    /**
     * A registry of the Avro message version.
     */
    private static final Map<Integer, InternalDeserializer<DatabusMessage>> REGISTRY;

    static {
        final Map<Integer, InternalDeserializer<DatabusMessage>> aMap = new HashMap<>();
        aMap.put(MessageStructureConstant.LEGACY_VERSION_NUMBER, new LegacyMessageDeserializer());
        aMap.put(MessageStructureConstant.AVRO_1_VERSION_NUMBER,
                new AvroMessageDeserializer(AvroV1MessageSchema.getSchema()));
        aMap.put(MessageStructureConstant.RAW_VERSION_NUMBER, new RawMessageDeserializer());
        REGISTRY = Collections.unmodifiableMap(aMap);
    }

    /**
     * Default Constructor.
     */
    private DeserializerRegistry() {
    }


    /**
     * Gets the Internal deserializer.
     *
     * @param version message version.
     * @return An {@link InternalDeserializer} instance.
     */
    public static InternalDeserializer<DatabusMessage> getDeserializer(final Integer version) {
        return REGISTRY.get(version);
    }
}

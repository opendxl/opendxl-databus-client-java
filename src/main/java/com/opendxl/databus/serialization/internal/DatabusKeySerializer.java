/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

/**
 * Serializer for Databus key
 */
public final class DatabusKeySerializer implements Serializer<String> {

    /**
     * The serializer.
     */
    private StringSerializer serializer = new StringSerializer();

    /**
     * Add configuration.
     * @param configs The configuration
     * @param isKey To check if configuration is a key
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        serializer.configure(configs, isKey);
    }

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic Topic associated with data.
     * @param data Typed data.
     * @return Serialized bytes.
     */
    @Override
    public byte[] serialize(final String topic, final String data) {
        return serializer.serialize(topic, data);
    }

    /**
     * Close this deserializer.
     */
    @Override
    public void close() {
        serializer.close();
    }
}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;

/**
 * Deserializer for Databus key
 */
public final class DatabusKeyDeserializer implements Deserializer<String> {

    /**
     * The deserializer.
     */
    private StringDeserializer deserializer = new StringDeserializer();

    /**
     * Add configuration.
     * @param configs The configuration
     * @param isKey To check if configuration is a key
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        deserializer.configure(configs, isKey);

    }

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic Topic associated with the data.
     * @param data Serialized bytes.
     * @return A String deserialized data.
     */
    @Override
    public String deserialize(final String topic, final byte[] data) {
        return deserializer.deserialize(topic, data);
    }

    /**
     * Close this deserializer.
     */
    @Override
    public void close() {
        deserializer.close();
    }
}

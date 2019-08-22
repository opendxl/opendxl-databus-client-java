/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;

/**
 * Deserializer for Databus key
 */
public final class DatabusKeyDeserializer implements Deserializer<String> {

    private StringDeserializer deserializer = new StringDeserializer();

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        deserializer.configure(configs, isKey);

    }

    @Override
    public String deserialize(final String topic, final byte[] data) {
        return deserializer.deserialize(topic, data);
    }

    @Override
    public void close() {
        deserializer.close();
    }
}

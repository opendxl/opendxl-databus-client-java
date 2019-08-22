/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

/**
 * Serializer for Databus key
 */
public final class DatabusKeySerializer implements Serializer<String> {

    private StringSerializer serializer = new StringSerializer();

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        serializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(final String topic, final String data) {
        return serializer.serialize(topic, data);
    }

    @Override
    public void close() {
        serializer.close();
    }
}

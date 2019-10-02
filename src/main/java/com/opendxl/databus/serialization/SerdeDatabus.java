/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization;

import com.opendxl.databus.entities.internal.DatabusMessage;
import com.opendxl.databus.serialization.internal.MessageDeserializer;
import com.opendxl.databus.serialization.internal.MessageSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

/**
 * Serializer / Deserializer for Databus messages
 */
public class SerdeDatabus implements Serde<DatabusMessage> {

    /**
     * Not implemented.
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {

    }

    /**
     * Not implemented.
     */
    @Override
    public void close() {

    }

    /**
     * Creates the serializer.
     *
     * @return A DatabusMessage serializer instance.
     */
    @Override
    public Serializer<DatabusMessage> serializer() {
        return new MessageSerializer();
    }

    /**
     * Creates the deserializer.
     *
     * @return A DatabusMessage deserializer instance.
     */
    @Override
    public Deserializer<DatabusMessage> deserializer() {
        return new MessageDeserializer();
    }
}

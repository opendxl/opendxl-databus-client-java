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

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<DatabusMessage> serializer() {
        return new MessageSerializer();
    }

    @Override
    public Deserializer<DatabusMessage> deserializer() {
        return new MessageDeserializer();
    }
}

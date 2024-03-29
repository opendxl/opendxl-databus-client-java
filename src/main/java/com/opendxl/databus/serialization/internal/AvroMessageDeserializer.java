/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;


import com.opendxl.databus.consumer.ConsumerRecord;
import com.opendxl.databus.consumer.DatabusConsumer;
import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.common.internal.adapter.HeadersAvroDeserializedAdapter;
import com.opendxl.databus.common.internal.adapter.PayloadHeadersAvroDeserializedAdapter;
import com.opendxl.databus.entities.internal.DatabusMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

/**
 * Avro Message Deserializer
 * It used internally by {@link DatabusConsumer#poll(long)}  }
 * after reading a {@link ConsumerRecord}
 */
public final class AvroMessageDeserializer implements InternalDeserializer<DatabusMessage> {

    /**
     * The schema to define the message.
     */
    private final Schema schema;

    /**
     * The schema reader.
     */
    private final DatumReader<GenericRecord> reader;

    /**
     * Constructor
     * @param schema Avro schema.
     */
    public AvroMessageDeserializer(final Schema schema) {
        this.schema = schema;
        this.reader = new GenericDatumReader(schema);
    }

    /**
     * Deserialize a message
     *
     * @param data The data to serialize.
     * @return A {@link DatabusMessage} instance.
     */
    @Override
    public DatabusMessage deserialize(final String topic, final byte[] data) {
        try {

            final GenericRecord avroRecord = reader.read(null, DecoderFactory.get().binaryDecoder(data, null));

            final Headers headers =
                    new HeadersAvroDeserializedAdapter()
                            .adapt(avroRecord.get("headers"));

            final byte[] payload =
                    new PayloadHeadersAvroDeserializedAdapter()
                            .adapt(avroRecord.get("payload"));

            final DatabusMessage message = new DatabusMessage(headers, payload);
            return message;

        } catch (Exception e) {
            throw new DatabusClientRuntimeException("Error deserializing Avro schema:" + schema.toString(true),
                    e, AvroMessageDeserializer.class);
        }
    }
}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.entities.internal.DatabusMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 * Avro Message Serializer
 * It used internally by
 * {@link
 * com.opendxl.databus.producer.DatabusProducer
 * #send(com.opendxl.streaming.nativeclient.producer.ProducerRecord,
 * com.opendxl.streaming.nativeclient.producer.Callback)} )}  }
 * before sending  a {@link com.opendxl.databus.producer.ProducerRecord}
 */
public final class AvroMessageSerializer implements InternalSerializer<DatabusMessage> {

    protected static final String HEADERS_FIELD_NAME = "headers";
    protected static final String PAYLOAD_FIELD_NAME = "payload";
    private final GenericData.Record databusValue;
    private final DatumWriter<GenericRecord> writer;

    /**
     * @param schema Avro schema
     */
    public AvroMessageSerializer(final Schema schema) {
        this.databusValue = new GenericData.Record(schema);
        this.writer = new GenericDatumWriter<>(schema);
    }


    /**
     * @param data data to be serialized
     * @return a serialized avro message as byte[]
     */
    @Override
    public byte[] serialize(final DatabusMessage data) {

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            databusValue.put(HEADERS_FIELD_NAME, data.getHeaders().getAll());
            databusValue.put(PAYLOAD_FIELD_NAME, ByteBuffer.wrap(data.getPayload()));

            final BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);

            writer.write(databusValue, encoder);
            encoder.flush();

            final byte[] bytes = out.toByteArray();

            return bytes;

        } catch (Exception e) {
            throw new DatabusClientRuntimeException("Error serializing Avro message"
                    + e.getMessage(), e, AvroMessageSerializer.class);
        }
    }

}

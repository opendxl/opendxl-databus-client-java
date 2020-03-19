/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;


import com.opendxl.databus.common.internal.util.HeaderInternalField;
import com.opendxl.databus.consumer.ConsumerRecord;
import com.opendxl.databus.consumer.DatabusConsumer;
import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.TierStorage;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.common.internal.adapter.HeadersAvroDeserializedAdapter;
import com.opendxl.databus.common.internal.adapter.PayloadHeadersAvroDeserializedAdapter;
import com.opendxl.databus.entities.internal.DatabusMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Avro Message Deserializer
 * It used internally by {@link DatabusConsumer#poll(long)}  }
 * after reading a {@link ConsumerRecord}
 */
public final class AvroMessageDeserializer implements InternalDeserializer<DatabusMessage> {

    /**
     * The logger object.
     */
    private static final Logger LOG = LoggerFactory.getLogger(AvroMessageDeserializer.class);

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
        return this.deserialize(topic, data, null);
    }

    @Override
    public DatabusMessage deserialize(String topic, byte[] data, TierStorage tierStorage) {
        try {

            GenericRecord avroRecord = reader.read(null, DecoderFactory.get().binaryDecoder(data, null));

            Headers headers =
                    new HeadersAvroDeserializedAdapter()
                            .adapt(avroRecord.get("headers"));

            byte[] payload =
                    new PayloadHeadersAvroDeserializedAdapter()
                            .adapt(avroRecord.get("payload"));


            // Tier Storage Section
            if (tierStorage != null) {
                final String bucketName = headers.get(HeaderInternalField.TIER_STORAGE_BUCKET_NAME_KEY);
                final String objectName = headers.get(HeaderInternalField.TIER_STORAGE_OBJECT_NAME_KEY);

                if (bucketName != null && objectName != null && !bucketName.isEmpty() && !objectName.isEmpty()) {
                    byte[] tierStorageObjectContent = null;
                    try {
                        tierStorageObjectContent =  tierStorage.get(bucketName, objectName);
                    } catch (Exception e) {
                        LOG.error("Error when reading message from Tier Storage. Bucket Name: "
                                + bucketName  + "Object Name: "
                                + objectName, e);
                    }

                    if (tierStorageObjectContent != null && tierStorageObjectContent.length > 0) {
                        MessageStructure messageStructure =
                                MessageStructureFactory.getStructure(tierStorageObjectContent);
                        avroRecord = reader
                                .read(null, DecoderFactory.get().binaryDecoder(messageStructure.getPayload(), null));
                        headers = new HeadersAvroDeserializedAdapter().adapt(avroRecord.get("headers"));
                        payload = new PayloadHeadersAvroDeserializedAdapter().adapt(avroRecord.get("payload"));
                    } else {
                        LOG.warn("Object content reading from Tier Storage is null or empty. Bucket: " + bucketName
                                + " Object: " + objectName);
                    }
                }
            }

            final DatabusMessage message = new DatabusMessage(headers, payload);
            return message;

        } catch (Exception e) {
            final String errMsg = "Error deserializing Avro schema:" + schema.toString(true);
            LOG.error(errMsg, e);
            throw new DatabusClientRuntimeException(errMsg, e, AvroMessageDeserializer.class);
        }
    }
}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import org.apache.avro.Schema;

/**
 * Avro Schema Registry
 *
 *
 *   Avro Schema V1
 *   {
 *       "namespace": "com.intel.databus.client",
 *       "type": "record",
 *       "name": "DatabusMessage",
 *       "fields": [{
 *           "name": "headers",
 *           "type": {
 *               "type": "map"
 *               "values": "string"
 *           }
 *       }, {
 *           "name": "payload"
 *           "type": "bytes"
 *       }]
 *   }
 */
public final class AvroV1MessageSchema {

    private static final String HEADERS_FIELD_NAME = "headers";
    private static final String PAYLOAD_FIELD_NAME = "payload";

    private static final Schema SCHEMA;

    static {
        Schema.Parser parser = new Schema.Parser();
        String rawSchema = "{"
                + "    \"namespace\":\"com.intel.databus.client\","
                + "    \"type\": \"record\","
                + "    \"name\": \"DatabusMessage\","
                + "    \"fields\": ["
                + "        {"
                + "            \"name\": \"" + HEADERS_FIELD_NAME + "\","
                + "            \"type\": {"
                + "                \"type\": \"map\","
                + "                \"values\": \"string\""
                + "            }"
                + "        },"
                + "        {"
                + "            \"name\":\"" + PAYLOAD_FIELD_NAME + "\","
                + "            \"type\":\"bytes\""
                + "        }"
                + "    ]"
                + "}";
        SCHEMA = parser.parse(rawSchema);
    }

    private AvroV1MessageSchema() {
    }


    /**
     * Get  Avro version 1 Schema
     *
     * @return a Avro version 1 Schema
     */
    public static Schema getSchema() {
        return SCHEMA;
    }

}

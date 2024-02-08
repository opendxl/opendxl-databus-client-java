/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;


import com.google.gson.Gson;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.common.internal.builder.TopicNameBuilder;
import com.opendxl.databus.common.internal.util.HeaderInternalField;
import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.internal.DatabusMessage;
import org.apache.commons.lang.StringUtils;

import java.util.Base64;
import java.util.Map;

//import org.apache.commons.lang.StringUtils

/**
 * Deserilize Legacy JSON messages
 * <p>
 * For example: {"headers":{"sourceId":"abc","tenantId":"TENANT10"},
 * "payloadBase64String":"wqFIb2xhISBIdWdvIGFxdcOtLg=="}*
 */
public final class LegacyMessageDeserializer implements InternalDeserializer<DatabusMessage> {

    /**
     * Deserialize data to a {@link DatabusMessage}.
     *
     * @param data Data to be deserialized.
     * @return A {@link DatabusMessage} instance.
     */
    @Override
    public DatabusMessage deserialize(final String topic, final byte[] data) {

        try {
            final String value = new String(data);
            final Gson gson = new Gson();
            final DatabusMessageAdapter result = gson.fromJson(value, DatabusMessageAdapter.class);
            return result.getDatabusMessage(topic);
        } catch (Exception e) {
            throw new DatabusClientRuntimeException("Error when deserializing byte[] to string:" + e.getMessage(), e,
                    LegacyMessageDeserializer.class);
        }

    }

    /**
     * This class is filled by GSON framework, based on a JSON as byte[],
     * then, it can create a {@link DatabusMessage} getInstance
     */
    private final class DatabusMessageAdapter {
        private Map<String, String> headers;
        private String payloadBase64String;

        /**
         * Gets a DatabusMessage. <br/>
         * Adds internal headers with Tenant Group and Topic Name to let ConsumerRecord adapter knows
         * what the topic and tenant group are.
         *
         * @return A {@link DatabusMessage} instance.

         *
         */
        public DatabusMessage getDatabusMessage(final String topic) {
            final byte[] payload = Base64.getDecoder().decode(payloadBase64String);
            final Headers hdrs = new Headers(headers);

            // Add internal headers for topic and tenant group
            if (StringUtils.isNotBlank(TopicNameBuilder.getTenantGroupTopicFromComposedTopicName(topic))) {
                hdrs.getAll().put(HeaderInternalField.TOPIC_NAME_KEY,
                        TopicNameBuilder.getTopicFromComposedTopicName(topic));

                hdrs.getAll().put(HeaderInternalField.TENANT_GROUP_NAME_KEY,
                        TopicNameBuilder.getTenantGroupTopicFromComposedTopicName(topic));
            }

            return new DatabusMessage(hdrs, payload);
        }

    }
}



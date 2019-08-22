/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer;

import com.opendxl.databus.common.HeadersField;
import com.opendxl.databus.consumer.DatabusConsumer;
import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.RoutingData;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.common.internal.util.HeaderInternalField;
import com.opendxl.databus.entities.MessagePayload;

import java.util.HashMap;
import java.util.Optional;

/**
 * Represents a record that {@link DatabusProducer}
 * sends to {@link DatabusConsumer}
 *
 * @param <P> message type
 */
public final class ProducerRecord<P> {

    /**
     * A {@link RoutingData} instance associated to the producer record.
     */
    private final RoutingData routingData;

    /**
     * A {@link Headers} instance associated to the producer record.
     */
    private final Headers headers;

    /**
     * A {@link MessagePayload} instance associated to the producer record.
     */
    private MessagePayload<P> payload;

    /**
     * Parameters cannot be null.
     *
     * @param routingData the address where the record must to be sent.
     *                          If it's null, throws {@link DatabusClientRuntimeException} exception
     * @param headers  key-value map with additional information. Some fields are well-know. See
     *                          {@link HeadersField}
     * @param payload message itself
     */
    public ProducerRecord(final RoutingData routingData,
                          final Headers headers,
                          final MessagePayload<P> payload) {

        if (routingData == null || payload == null) {
            throw new DatabusClientRuntimeException("Arguments cannot be null", this.getClass());
        }
        this.routingData = routingData;

        this.headers = Optional.ofNullable(headers).orElse(new Headers(new HashMap<>()));

        this.payload = payload;

        this.headers.getAll().forEach((k, v) -> validateHeaderKey(k));
    }

    /**
     * The address where the record must to be sent.
     *
     * @return Routing Data
     */
    public RoutingData getRoutingData() {
        return routingData;
    }

    /**
     * A Map with additional information. Some fields are well-know. See
     * {@link HeadersField}
     * @return Headers
     */
    public Headers getHeaders() {
        return headers;
    }

    /**
     *The message which will be sent
     *
     * @return Payload
     */
    public MessagePayload<P> payload() {
        return payload;
    }

    /**
     * The Databus exception
     *
     * @param key key to be validated
     * @throws DatabusClientRuntimeException in case key has an invalid name
     */
    private void validateHeaderKey(final String key) throws DatabusClientRuntimeException {
        if (key.startsWith(HeaderInternalField.INTERNAL_HEADER_IDENTIFIER)
                && key.endsWith(HeaderInternalField.INTERNAL_HEADER_IDENTIFIER)) {
            throw new DatabusClientRuntimeException("Name not allowed for a Key header", Headers.class);
        }
    }

}

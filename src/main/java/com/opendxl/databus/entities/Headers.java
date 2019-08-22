/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.entities;


import com.opendxl.databus.common.HeadersField;
import com.opendxl.databus.producer.DatabusProducer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * It represents a key-value pair map where the user can put elements for the consumer's interest
 * Some out-of-the-box elements names are located at {@link HeadersField}
 * <p>
 * See how to use in {@link DatabusProducer} example
 * </p>
 */
public class Headers implements Cloneable {


    private Map<String, String> headers;

    /**
     *
     * @param headers a key-value pair Map with header to be transported. A null value is replcaed by a empty String
     */
    public Headers(final Map<String, String> headers) {

        // replace a null value for an empty value in case it exists
        this.headers = headers
                .entrySet()
                .stream()
                .collect(Collectors
                        .toMap(Map.Entry::getKey, e -> Optional.ofNullable(e.getValue()).orElse("")));

    }

    public Headers() {
        this.headers = new HashMap<>();
    }

    public String put(final String headerKey, final String value) {
        return this.headers.put(headerKey, Optional.ofNullable(value).orElse(""));
    }

    public String get(final String key) {
        return headers.get(key);
    }

    public Map<String, String> getAll() {
        return headers;
    }

    @Override
    public Headers clone() {
        Map<String, String> clonedHeaders = new HashMap<>(headers);
        return new Headers(clonedHeaders);
    }

}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer.internal;


import com.opendxl.databus.producer.DatabusProducer;
import com.opendxl.databus.producer.Producer;
import com.opendxl.databus.producer.ProducerConfig;

import java.util.Properties;

/**
 * This class contains a set of config properties values that will be added
 * to {@link DatabusProducer}
 * unless they are provided by the SDK user
 */
public final class ProducerDefaultConfiguration {

    /**
     * This configuration properties controls how long {@link Producer#send}
     * and {@link Producer#partitionsFor} will block.
     * These methods can be blocked either because the buffer is full or metadata
     * unavailable. Blocking in the user-supplied serializers or partitioner will not be counted against this timeout.
     */
    public static final String MAX_BLOCK_MS_CONFIG_DEFAULT_VALUE = "5000";
    public static final String MAX_BLOCK_MS_CONFIG_KEY = ProducerConfig.MAX_BLOCK_MS_CONFIG;
    private static Properties configuration = new Properties();

    static {
        configuration.setProperty(MAX_BLOCK_MS_CONFIG_KEY, MAX_BLOCK_MS_CONFIG_DEFAULT_VALUE);
    }

    private ProducerDefaultConfiguration() {
    }

    /**
     * @return all configuration properties
     */
    public static Properties getAll() {
        return configuration;
    }

    /**
     * @param key the property key to look for
     * @return the String value mapped to the key. Returns null if it is not mapped. If a string is returned,
     * {@link DatabusProducer} will add it unless it being provided
     * by the SDK user.
     * If null is returned,{@link DatabusProducer}  will take
     * Kafka default configuration for the key.
     */
    public static String get(final String key) {
        return configuration.getProperty(key);
    }
}

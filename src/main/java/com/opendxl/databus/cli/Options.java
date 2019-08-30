/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli;

/**
 * Each enumerated represents a command line option. Each option is made up by --optionName value
 */
public enum Options {
    TO_TOPIC("to-topic"),
    OPERATION("operation"),
    BROKER_LIST("brokers"),
    MESSAGE("msg"),
    CONFIG("config"),
    TENANT_GROUP("tenant-group"),
    SHARDING_KEY("sharding-key"),
    HEADERS("headers"),
    PARTITION("partition");

    private final String optionName;

    /**
     *
     * @param optionName the option name
     */
    Options(final String optionName) {
        this.optionName = optionName;

    }

    /**
     *
     * @return option name as string
     */
    public String getOptionName() {
        return optionName;

    }
};



/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli;

/**
 * Each enumerated represents a command line option. Each option is made up by --optionName value
 */
public enum Options {

    /**
     * The option name indicating the name of the topic
     */
    TO_TOPIC("to-topic"),

    /**
     * The option name indicating the name of the operation to execute
     */
    OPERATION("operation"),

    /**
     * The option name for the broker list
     */
    BROKER_LIST("brokers"),

    /**
     * The option name for the message payload
     */
    MESSAGE("msg"),

    /**
     * The option name for extra producer configurations
     */
    CONFIG("config"),

    /**
     * The option name used to add a tenant group name
     */
    TENANT_GROUP("tenant-group"),

    /**
     * The option name used to add a sharding ket
     */
    SHARDING_KEY("sharding-key"),

    /**
     * The option name used to add Headers
     */
    HEADERS("headers"),

    /**
     * The option name indicating the name of the topic where the message will be consumed
     */
    FROM_TOPIC("from-topic"),

    /**
     * The option name indicating the timeout value for the consumer
     */
    CONSUME_TIMEOUT("consume-timeout"),

    /**
     * The option name indicating the name of the consumer group
     */
    CG("cg"),

    /**
     * The option name indicating the number of records to consume
     */
    CONSUME_RECORDS("consume-records"),

    /**
     * The option name indicating the number of the topic partition
     */
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



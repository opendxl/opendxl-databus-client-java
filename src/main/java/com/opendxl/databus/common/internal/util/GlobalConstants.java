/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.util;

/**
 * Databus SDK Global Constants
 */
public final class GlobalConstants {

    private GlobalConstants() {
    }

    /**
     * The topic separator.
     */
    public static final String TOPIC_SEPARATOR = "-";

    /**
     * The tenant group position to get tenant group.
     */
    public static final int TENANT_GROUP_POSITION = 1;

    /**
     * The topic position to get the topic.
     */
    public static final int TOPIC_POSITION = 0;

    /**
     * The number of components for topic and tenant group.
     */
    public static final int NUMBER_OF_TOPIC_COMPONENTS = 2;
}

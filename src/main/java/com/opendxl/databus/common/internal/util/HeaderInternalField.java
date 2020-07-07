/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.util;

/**
 * Internal Headers Key
 * All Header field valus must start and finish with "_"
 */
public final class HeaderInternalField {


    private HeaderInternalField() {
    }

    /**
     * The internal header identifier.
     */
    public static final String INTERNAL_HEADER_IDENTIFIER = "_";

    /**
     * The tenant group key name.
     */
    public static final String TENANT_GROUP_NAME_KEY = INTERNAL_HEADER_IDENTIFIER + "TGN" + INTERNAL_HEADER_IDENTIFIER;

    /**
     * The topic name key name.
     */
    public static final String TOPIC_NAME_KEY = INTERNAL_HEADER_IDENTIFIER + "TN" + INTERNAL_HEADER_IDENTIFIER;

    public static final String TIER_STORAGE_BUCKET_NAME_KEY = INTERNAL_HEADER_IDENTIFIER + "BN"
            + INTERNAL_HEADER_IDENTIFIER;;

    public static final String TIER_STORAGE_OBJECT_NAME_KEY = INTERNAL_HEADER_IDENTIFIER + "OB"
            + INTERNAL_HEADER_IDENTIFIER;;

}

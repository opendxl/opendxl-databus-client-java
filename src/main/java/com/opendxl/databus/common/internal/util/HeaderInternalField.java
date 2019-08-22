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

    public static final String INTERNAL_HEADER_IDENTIFIER = "_";
    public static final String TENANT_GROUP_NAME_KEY = INTERNAL_HEADER_IDENTIFIER + "TGN" + INTERNAL_HEADER_IDENTIFIER;
    public static final String TOPIC_NAME_KEY = INTERNAL_HEADER_IDENTIFIER + "TN" + INTERNAL_HEADER_IDENTIFIER;
}

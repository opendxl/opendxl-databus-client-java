/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.util;

/**
 * Validator
 */
public final class Validator {

    private Validator() {
    }

    /**
     * Null validator
     *
     * @param t   getInstance of type T
     * @param <T> Type of the getInstance to be validated
     * @return true if t is not null.
     * @throws  NullPointerException if t is null
     */
    public static <T> T notNull(final T t) {
        if (t == null) {
            throw new NullPointerException(t.toString());
        } else {
            return t;
        }
    }

}

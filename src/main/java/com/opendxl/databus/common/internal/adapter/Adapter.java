/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/


package com.opendxl.databus.common.internal.adapter;

/**
 * It becomes a getInstance of type S (source) in getInstance of type T (target)
 *
 * @param <S> source type
 * @param <T> destination type
 */
public interface Adapter<S, T> {

    /**
     * @param sourceInstance getInstance of type S
     * @return a adapted getInstance of type T
     */
    T adapt(S sourceInstance);
}

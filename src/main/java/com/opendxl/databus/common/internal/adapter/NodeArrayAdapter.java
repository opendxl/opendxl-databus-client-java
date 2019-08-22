/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.adapter;

import com.opendxl.databus.common.Node;

/**
 * A Adapter For List of Node
 */
public final class NodeArrayAdapter implements Adapter<org.apache.kafka.common.Node[], Node[]> {

    /**
     *
     * @param sourceNodeArray A array of source Node instances
     * @return a Array of {@link Node} instances
     */
    @Override
    public Node[] adapt(final org.apache.kafka.common.Node[] sourceNodeArray) {
        final Node[] targetNodeArray = new Node[sourceNodeArray.length];

        for (int i = 0 ; i < sourceNodeArray.length ; ++i) {
            targetNodeArray[i] = new NodeAdater()
                    .adapt(sourceNodeArray[i]);
        }

        return targetNodeArray;
    }
}

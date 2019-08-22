/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.adapter;


import com.opendxl.databus.common.Node;

/**
 * A Adapter for Node
 */
public final class NodeAdater implements Adapter<org.apache.kafka.common.Node, Node> {

    /**
     *
     * @param sourceNode a source Node
     * @return a getInstance of {@link Node}
     */
    @Override
    public Node adapt(final org.apache.kafka.common.Node sourceNode) {

        final Node node = new Node(sourceNode.id(),
                sourceNode.host(),
                sourceNode.port());

        return node;
    }
}

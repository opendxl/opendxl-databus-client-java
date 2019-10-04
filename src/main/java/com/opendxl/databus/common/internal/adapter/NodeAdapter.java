/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.adapter;


import com.opendxl.databus.common.Node;

/**
 * A Adapter for Node
 */
public final class NodeAdapter implements Adapter<org.apache.kafka.common.Node, Node> {

    /**
     * Adapter pattern implementation for Node instance.
     * Adapts a {@code org.apache.kafka.common.Node} to a {@code com.opendxl.databus.common.Node} instance.
     *
     * @param sourceNode a source Node.
     * @return An instance of {@link Node}.
     */
    @Override
    public Node adapt(final org.apache.kafka.common.Node sourceNode) {

        final Node node = new Node(sourceNode.id(),
                sourceNode.host(),
                sourceNode.port());

        return node;
    }
}

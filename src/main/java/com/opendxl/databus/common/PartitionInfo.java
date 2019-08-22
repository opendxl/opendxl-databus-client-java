/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common;


import com.opendxl.databus.producer.Producer;

/**
 * Information about a topic-partition.
 * An object of this class is built only for Databus as a result
 * of a {@link Producer#partitionsFor(String)}  method
 *
 */
public final class PartitionInfo {

    private final String topic;
    private final int partition;
    private final Node leader;
    private final Node[] replicas;
    private final Node[] inSyncReplicas;

    public PartitionInfo(final String topic,
                         final int partition,
                         final Node leader,
                         final Node[] replicas,
                         final Node[] inSyncReplicas) {

        this.topic = topic;
        this.partition = partition;
        this.leader = leader;
        this.replicas = replicas;
        this.inSyncReplicas = inSyncReplicas;
    }

    /**
     *
     * @return topic name
     */
    public String topic() {
        return topic;
    }

    /**
     *
     * @return partition number
     */
    public int partition() {
        return partition;
    }

    /**
     *
     * @return leader node
     */
    public Node leader() {
        return leader;
    }

    /**
     *
     * @return replica nodes
     */
    public Node[] replicas() {
        return replicas;
    }

    /**
     *
     * @return replica nodes
     */
    public Node[] inSyncReplicas() {
        return inSyncReplicas;
    }

    /**
     *
     *
     * @return this getInstance as String
     */
    public String toString() {
        return String.format("Partition(topic = %s, partition = %d, leader = %s, replicas = %s, isr = %s)",
                new Object[]{topic(), Integer.valueOf(partition()),
                        leader() == null ? "none" : Integer.valueOf(leader().id()),
                        this.fmtNodeIds(replicas()), this.fmtNodeIds(inSyncReplicas())});
    }

    /**
     * Extract the node ids from each item in the array and format for display
     * @param nodes nodes
     * @return formated string
     */
    private String fmtNodeIds(final Node[] nodes) {
        StringBuilder b = new StringBuilder("[");
        for (int i = 0; i < nodes.length - 1; i++) {
            b.append(Integer.toString(nodes[i].id()));
            b.append(',');
        }
        if (nodes.length > 0) {
            b.append(Integer.toString(nodes[nodes.length - 1].id()));
            b.append(',');
        }
        b.append("]");
        return b.toString();
    }

}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common;

/**
 * Information about a Databus node
 */
public final class Node {

    private org.apache.kafka.common.Node node;
    private int id;
    private String idString;
    private String host;
    private int port;

    /**
     *
     * @param id node id
     * @param host host name
     * @param port port number
     */
    public Node(final int id, final String host, final int port) {
        this.id = id;
        this.idString = Integer.toString(id);
        this.host = host;
        this.port = port;
    }

    public static Node noNode() {
        return new Node(-1, "", -1);
    }

    /**
     *
     * @return node id
     */
    public int id() {
        return id;
    }

    /**
     *
     * @return id as String
     */
    public String idString() {
        return idString;
    }

    /**
     *
     * @return host name
     */
    public String host() {
        return host;
    }

    /**
     *
     * @return port number
     */
    public int port() {
        return port;
    }

    /**
     *
     * @return getInstance hashcode
     */
    public int hashCode() {
        return node.hashCode();
    }

    /**
     *
     * @param obj Node to be compared to
     * @return true if getInstance are the same otherwise false
     */
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (!Node.class.isAssignableFrom(obj.getClass())) {
            return false;
        }
        final Node other = (Node) obj;
        if ((this.host == null) ? (other.host != null) : !this.host.equals(other.host)) {
            return false;
        }
        if (this.id != other.id) {
            return false;
        }
        if (this.port != other.port) {
            return false;
        }

        return true;
    }

    /**
     *
     * @return this getInstance as String
     */
    public String toString() {
        return String.format("Node(id = %d, host = %s, port = %d)",
                new Object[]{Integer.valueOf(id()), host(), Integer.valueOf(port())});
    }

}

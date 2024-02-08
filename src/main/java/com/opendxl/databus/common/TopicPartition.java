/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;

/**
 * A topic name and partition number
 */
public final class TopicPartition implements Serializable {

    /**
     * An int which represents a magic number for the hashcode
     */
    private static final int MAGIC_NUMBER_31 = 31;

    /**
     * An int which represents an associated partition
     */
    private final int partition;

    /**
     * An String which represents the topic name
     */
    private final String topic;

    /**
     * An int which represents the hash
     */
    private int hash = 0;

    /**
     * A static field which represents an invalid partition
     */
    private static final int INVALID_PARTITION = -1;

    /**
     * @param topic     The topic
     * @param partition The topic partition
     */
    public TopicPartition(final String topic, final int partition) {

        if (StringUtils.isBlank(topic) || partition < 0) {
            throw new IllegalArgumentException();
        }

        this.partition = partition;
        this.topic = topic;
    }

    /**
     * @param topic The topic name
     */
    public TopicPartition(final String topic) {
        this.topic = topic;
        this.partition = INVALID_PARTITION;
    }

    /**
     * @return The partition
     */
    public int partition() {
        return partition;
    }

    /**
     * @return The topic name
     */
    public String topic() {
        return topic;
    }

    /**
     * @return The hash code
     */
    public int hashCode() {
        if (this.hash != 0) {
            return this.hash;
        } else {
            boolean prime = true;
            byte result = 1;
            int result1 = MAGIC_NUMBER_31 * result + this.partition;
            result1 = MAGIC_NUMBER_31 * result1 + (this.topic == null ? 0 : this.topic.hashCode());
            this.hash = result1;
            return result1;
        }
    }

    /**
     * @return A boolean value to check if instance is equal
     */
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (this.getClass() != obj.getClass()) {
            return false;
        } else {
            TopicPartition other = (TopicPartition) obj;
            if (this.partition != other.partition) {
                return false;
            } else {
                if (this.topic == null) {
                    if (other.topic != null) {
                        return false;
                    }
                } else if (!this.topic.equals(other.topic)) {
                    return false;
                }
                return true;
            }
        }
    }

    public String toString() {
        return this.topic + "-" + this.partition;
    }

}

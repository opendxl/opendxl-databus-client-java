/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

import java.io.Serializable;

/**
 * The Kafka offset commit API allows users to provide additional metadata (in the form of a string)
 * when an offset is committed. This can be useful (for example) to store information about which
 * node made the commit, what time the commit was made, etc.
 */
public final class OffsetAndMetadata implements Serializable {

    /**
     * An instance of Kafka {@link org.apache.kafka.clients.consumer.OffsetAndMetadata}
     */
    private final org.apache.kafka.clients.consumer.OffsetAndMetadata offsetAndMetadata;

    /**
     * Construct a new OffsetAndMetadata object for committing through {@link Consumer}.
     * @param offset The offset to be committed
     * @param metadata Non-null metadata
     */
    public OffsetAndMetadata(final long offset, final String metadata) {
        offsetAndMetadata = new org.apache.kafka.clients.consumer.OffsetAndMetadata(offset, metadata);
    }

    /**
     * Construct a new OffsetAndMetadata object for committing through {@link Consumer}. The metadata
     * associated with the commit will be empty.
     * @param offset The offset to be committed
     */
    public OffsetAndMetadata(final long offset) {
        this(offset, "");
    }

    /**
     * Returns the offset value number
     * @return The Offset value number
     */
    public long offset() {
        return offsetAndMetadata.offset();
    }

    /**
     * Returns the metadata value as String
     * @return Metadata of the consumer
     */
    public String metadata() {
        return offsetAndMetadata.metadata();
    }

    /**
     * Overriding equals method
     * @return True if {@link org.apache.kafka.clients.consumer.OffsetAndMetadata} is equals
     */
    @Override
    public boolean equals(final Object o) {
        return offsetAndMetadata.equals(o);
    }

    /**
     * Get the hashcode of the {@link org.apache.kafka.clients.consumer.OffsetAndMetadata} object
     * @return The hashcode of the OffsetAndMetadata object as int
     */
    @Override
    public int hashCode() {
        return offsetAndMetadata.hashCode();
    }

    /**
     * Get the toString method of the {@link org.apache.kafka.clients.consumer.OffsetAndMetadata} object
     * @return The toString of the OffsetAndMetadata object
     */
    @Override
    public String toString() {
        return offsetAndMetadata.toString();
    }

}

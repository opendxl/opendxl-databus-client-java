/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

import java.io.Serializable;

/**
 * The Kafka offset commit API allows users to provide additional metadata (in the form of a string)
 * when an offset is committed. This can be useful (for example) to store information about which
 * node made the commit, what time the commit was made, etc.
 */
public final class OffsetAndMetadata implements Serializable {

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
     *
     * @return offset
     */
    public long offset() {
        return offsetAndMetadata.offset();
    }

    /**
     *
     * @return metadata
     */
    public String metadata() {
        return offsetAndMetadata.metadata();
    }

    @Override
    public boolean equals(final Object o) {
        return offsetAndMetadata.equals(o);
    }

    @Override
    public int hashCode() {
        return offsetAndMetadata.hashCode();
    }

    @Override
    public String toString() {
        return offsetAndMetadata.toString();
    }

}

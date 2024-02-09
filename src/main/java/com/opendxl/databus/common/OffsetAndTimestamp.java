/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common;


import java.util.Objects;
import java.util.Optional;

/**
 * A container class for offset and timestamp.
 */
public final class OffsetAndTimestamp {
    private final long timestamp;
    private final long offset;
    private final Optional<Integer> leaderEpoch;

    public OffsetAndTimestamp(final long offset, final long timestamp) {
        this(offset, timestamp, Optional.empty());
    }

    public OffsetAndTimestamp(final long offset, final long timestamp, final Optional<Integer> leaderEpoch) {
        if (offset < 0) {
            throw new IllegalArgumentException("Invalid negative offset");
        }

        if (timestamp < 0) {
            throw new IllegalArgumentException("Invalid negative timestamp");
        }

        this.offset = offset;
        this.timestamp = timestamp;
        this.leaderEpoch = leaderEpoch;
    }

    public long timestamp() {
        return timestamp;
    }

    public long offset() {
        return offset;
    }

    /**
     * Get the leader epoch corresponding to the offset that was found (if one exists).
     * This can be provided to seek() to ensure that the log hasn't been truncated prior to fetching.
     *
     * @return The leader epoch or empty if it is not known
     */
    public Optional<Integer> leaderEpoch() {
        return leaderEpoch;
    }

    @Override
    public String toString() {
        return "(timestamp=" + timestamp
                + ", leaderEpoch=" + leaderEpoch.orElse(null)
                + ", offset=" + offset + ")";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OffsetAndTimestamp that = (OffsetAndTimestamp) o;
        return timestamp == that.timestamp
                && offset == that.offset
                && Objects.equals(leaderEpoch, that.leaderEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, offset, leaderEpoch);
    }
}

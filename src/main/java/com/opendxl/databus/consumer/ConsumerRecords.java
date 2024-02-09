/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

import com.opendxl.databus.common.TopicPartition;
import org.apache.commons.lang.NullArgumentException;
import org.apache.kafka.common.utils.AbstractIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class contains a set of {@link ConsumerRecord}. It is returned by
 * {@link Consumer#poll(long)}. Since it implements {@link Iterable} the SDK user
 * can iterate using a for structure.
 */
public class ConsumerRecords<P> implements Iterable<ConsumerRecord> {

    /**
     * The consumer record instance.
     */
    public static final ConsumerRecords EMPTY;

    static {
        EMPTY = new ConsumerRecords(Collections.EMPTY_MAP);
    }

    /**
     * A map of TopicPartition as keys with a list of consumer record as values.
     * Used to identify which list of records belongs to each topic.
     */
    private final Map<TopicPartition, List<ConsumerRecord<P>>> records;

    /**
     * Constructor for the consumer record Map.
     *
     * @param records a Map that groups a list of records for each Partition.
     * @throws NullArgumentException when records is null.
     */
    public ConsumerRecords(final Map<TopicPartition, List<ConsumerRecord<P>>> records) {

        if (records == null) {
            throw new NullArgumentException("records");
        }

        this.records = records;
    }

    /**
     * Gets an empty set of the consumer records map.
     *
     * @return And empty consumer records object.
     */
    public static ConsumerRecords empty() {
        return EMPTY;
    }

    /**
     * Gets the consumer ConsumerRecord list.
     *
     * @param partition The topic partition name to get the partition number.
     * @return A list of ConsumerRecord object.
     */
    public List<ConsumerRecord> records(final TopicPartition partition) {
        List recs = (List) this.records.get(partition);
        return recs == null ? Collections.emptyList() : Collections.unmodifiableList(recs);
    }

    /**
     * Gets an Iterable instance of ConsumerRecord list.
     *
     * @param topic The topic name.
     * @return An iterable list of ConsumerRecord.
     */
    public Iterable<ConsumerRecord> records(final String topic) {
        if (topic == null) {
            throw new IllegalArgumentException("Topic must be non-null.");
        } else {
            ArrayList recs = new ArrayList();
            Iterator iterator = this.records.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry entry = (Map.Entry) iterator.next();
                if (((TopicPartition) entry.getKey()).topic().equals(topic)) {
                    recs.add(entry.getValue());
                }
            }

            return new ConcatenatedIterable(recs);
        }
    }

    /**
     * Gets the topic partition Set object for the records Map.
     *
     * @return The TopicPartition Set.
     */
    public Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(this.records.keySet());
    }

    @Override
    public Iterator<ConsumerRecord> iterator() {
        return new ConcatenatedIterable(this.records.values()).iterator();
    }

    /**
     * Gets total of records contained in the records object instance.
     *
     * @return The total records number as int.
     */
    public int count() {
        int count = 0;

        List recs;
        for (Iterator iterator = this.records.values().iterator(); iterator.hasNext(); count += recs.size()) {
            recs = (List) iterator.next();
        }

        return count;
    }

    /**
     * Gets a boolean value that checks if the records map if empty.
     *
     * @return True if the records maps if empty.
     */
    public boolean isEmpty() {
        return this.records.isEmpty();
    }

    /**
     * This inner class contains an implementation of a iterator pattern to iterate Consumer Record objects.
     */
    private static class ConcatenatedIterable<K, V> implements Iterable<ConsumerRecord> {

        /**
         * The iterables consumer records object.
         */
        private final Iterable<? extends Iterable<ConsumerRecord>> iterables;


        /**
         * The constructor of ContatenatedIterable instance
         *
         * @param iterables An iterable list of ConsumerRecord
         */
        ConcatenatedIterable(final Iterable<? extends Iterable<ConsumerRecord>> iterables) {
            this.iterables = iterables;
        }

        /**
         * The iterator pattern at itself
         *
         * @return An iterator object of Consumer record.
         */
        public Iterator<ConsumerRecord> iterator() {
            return new AbstractIterator() {
                private final Iterator<? extends Iterable<ConsumerRecord>> iters;
                private Iterator<ConsumerRecord> current;

                {
                    this.iters = ConcatenatedIterable.this.iterables.iterator();
                }

                public ConsumerRecord makeNext() {
                    if (this.current == null || !this.current.hasNext()) {
                        if (!this.iters.hasNext()) {
                            return (ConsumerRecord) this.allDone();
                        }

                        this.current = ((Iterable) this.iters.next()).iterator();
                    }

                    return (ConsumerRecord) this.current.next();
                }
            };
        }
    }
}

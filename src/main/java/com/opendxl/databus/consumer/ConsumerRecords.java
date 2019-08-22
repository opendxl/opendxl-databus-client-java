/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
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

//import org.apache.kafka.common.TopicPartition;

/**
 * It contains a set of {@link ConsumerRecord}. It is returned by
 * {@link Consumer#poll(long)} Since it implements {@link Iterable} the SDK user
 * can iterate using a for structure
 */
public class ConsumerRecords<P> implements Iterable<ConsumerRecord> {

    public static final ConsumerRecords EMPTY;

    static {
        EMPTY = new ConsumerRecords(Collections.EMPTY_MAP);
    }

    private final Map<TopicPartition, List<ConsumerRecord<P>>> records;

    /**
     *
     * @param records a Map that groups a list of records for each Partition
     * @throws NullArgumentException when records is null
     */
    public ConsumerRecords(final Map<TopicPartition, List<ConsumerRecord<P>>> records) {

        if (records == null) {
            throw new NullArgumentException("records");
        }

        this.records = records;
    }

    public static ConsumerRecords empty() {
        return EMPTY;
    }

    public List<ConsumerRecord> records(final TopicPartition partition) {
        List recs = (List) this.records.get(partition);
        return recs == null ? Collections.emptyList() : Collections.unmodifiableList(recs);
    }

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

    public Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(this.records.keySet());
    }

    @Override
    public Iterator<ConsumerRecord> iterator() {
        return new ConcatenatedIterable(this.records.values()).iterator();
    }

    public int count() {
        int count = 0;

        List recs;
        for (Iterator iterator = this.records.values().iterator(); iterator.hasNext(); count += recs.size()) {
            recs = (List) iterator.next();
        }

        return count;
    }

    public boolean isEmpty() {
        return this.records.isEmpty();
    }

    private static class ConcatenatedIterable<K, V> implements Iterable<ConsumerRecord> {

        private final Iterable<? extends Iterable<ConsumerRecord>> iterables;

        ConcatenatedIterable(final Iterable<? extends Iterable<ConsumerRecord>> iterables) {
            this.iterables = iterables;
        }

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

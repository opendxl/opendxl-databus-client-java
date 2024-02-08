/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common;

import com.opendxl.databus.common.internal.util.Validator;

import java.util.Map;

/**
 * A Metric name
 */
public final class MetricName {

    private final String name;
    private final String group;
    private final String description;
    private final Map<String, String> tags;
    private int hash = 0;

    /**
     * Create MetricName
     *
     * @param name        The name of the metric
     * @param group       logical group name of the metrics to which this metric belongs
     * @param description A human-readable description to include in the metric
     * @param tags        additional key/value attributes of the metric
     */
    public MetricName(final String name,
                      final String group,
                      final String description,
                      final Map<String, String> tags) {

        this.name = Validator.notNull(name);
        this.group = Validator.notNull(group);
        this.description = Validator.notNull(description);
        this.tags = Validator.notNull(tags);
    }

    /**
     * @return metric name as String
     */
    public String name() {
        return name;
    }

    /**
     * @return group name
     */
    public String group() {
        return group;
    }

    /**
     * @return tags
     */
    public Map<String, String> tags() {
        return tags;
    }

    /**
     * @return metric description
     */
    public String description() {
        return description;
    }


    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        final int prime = 31;
        int result = 1;
        result = prime * result + ((group == null) ? 0 : group.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((tags == null) ? 0 : tags.hashCode());
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MetricName other = (MetricName) obj;
        if (group == null) {
            if (other.group != null) {
                return false;
            }
        } else if (!group.equals(other.group)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (tags == null) {
            if (other.tags != null) {
                return false;
            }
        } else if (!tags.equals(other.tags)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "MetricName [name=" + name + ", group=" + group + ", description="
                + description + ", tags=" + tags + "]";
    }

}

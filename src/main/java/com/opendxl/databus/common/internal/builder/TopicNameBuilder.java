/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.builder;

import com.opendxl.databus.common.internal.util.GlobalConstants;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import org.apache.commons.lang.StringUtils;

import java.util.Optional;


/**
 * It is a helper class to compose topic names.
 * A Databus topic is made up by a topic name and a tenant group name
 */
public final  class TopicNameBuilder {



    private TopicNameBuilder() {
    }

    /**
     * Compose a Kafka topic name based on group and topic
     *
     * @param topic       topic name
     * @param tenantGroup tenant group name
     * @return a composed topic name
     */
    public static String getTopicName(final String topic, final String tenantGroup) {
        if (StringUtils.isBlank(topic)) {
            throw new DatabusClientRuntimeException("Arguments cannot be null",
                    TopicNameBuilder.class);
        }

        final StringBuilder topicBuilder = new StringBuilder();
        final String[] topicValues = new String[GlobalConstants.NUMBER_OF_TOPIC_COMPONENTS];
        topicValues[GlobalConstants.TENANT_GROUP_POSITION] = Optional.ofNullable(tenantGroup).orElse("").trim();
        topicValues[GlobalConstants.TOPIC_POSITION] = topic;

        if (!StringUtils.isBlank(topicValues[0])
                && !StringUtils.isBlank(topicValues[1])) {

            topicBuilder.append(topicValues[0])
                    .append(GlobalConstants.TOPIC_SEPARATOR)
                    .append(topicValues[1]);

        } else if (StringUtils.isBlank(topicValues[0])) {
            topicBuilder.append(topicValues[1]);

        } else if (StringUtils.isBlank(topicValues[1])) {
            topicBuilder.append(topicValues[0]);
        }
        return topicBuilder.toString();
    }

    /**
     *
     * @param composedTopicName A topic name composed by topic and tenant group
     * @return topic name without a tenant group
     */
    public static String getTopicFromComposedTopicName(final String composedTopicName) {
        if (StringUtils.isBlank(composedTopicName)) {
            throw new DatabusClientRuntimeException("Arguments cannot be null",
                    TopicNameBuilder.class);
        }

        String result = composedTopicName;

        final String[] tokens = composedTopicName.split(GlobalConstants.TOPIC_SEPARATOR);

        if (tokens.length == GlobalConstants.NUMBER_OF_TOPIC_COMPONENTS
                && !StringUtils.isBlank(tokens[GlobalConstants.TOPIC_POSITION])) {

            result = tokens[GlobalConstants.TOPIC_POSITION];

        } else if (tokens.length == 1 && !StringUtils.isBlank(tokens[0])) {
            result = tokens[0];
        }
        return result;
    }

    /**
     *
     * @param composedTopicName A topic name composed by topic and tenant group
     * @return topic name without a tenant group
     */
    public static String getTenantGroupTopicFromComposedTopicName(final String composedTopicName) {
        if (StringUtils.isBlank(composedTopicName)) {
            throw new DatabusClientRuntimeException("Arguments cannot be null",
                    TopicNameBuilder.class);
        }

        String result = "";

        final String[] tokens = composedTopicName.split(GlobalConstants.TOPIC_SEPARATOR);

        if (tokens.length == GlobalConstants.NUMBER_OF_TOPIC_COMPONENTS) {
            result = tokens[GlobalConstants.TENANT_GROUP_POSITION];
        }
        return result;
    }

}

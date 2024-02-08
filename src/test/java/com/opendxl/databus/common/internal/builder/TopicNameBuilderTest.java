/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common.internal.builder;

import com.opendxl.databus.common.internal.util.GlobalConstants;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import org.junit.Assert;
import org.junit.Test;

public class TopicNameBuilderTest {

    @Test
    public void shouldGetTopicFromComposedTopic() {
        String expected = "MyTopic";
        String actual = TopicNameBuilder.getTopicFromComposedTopicName("MyTopic"+ GlobalConstants.TOPIC_SEPARATOR+"group0");
        Assert.assertTrue(actual.equals(expected));
    }

    @Test
    public void shouldGetTopicFromNonComposedTopicName() {
        String expected = "MyTopic";
        String actual = TopicNameBuilder.getTopicFromComposedTopicName("MyTopic");
        Assert.assertTrue(actual.equals(expected));
    }

    @Test(expected = DatabusClientRuntimeException.class)
    public void shouldGetAnExceptionWhenTopicIsEmpty() {
        TopicNameBuilder.getTopicFromComposedTopicName("");
    }

    @Test(expected = DatabusClientRuntimeException.class)
    public void shouldGetAnExceptionWhenTopicIsNull() {
        TopicNameBuilder.getTopicFromComposedTopicName(null);
    }

    @Test
    public void shouldGetTopicWhenTenantGroupIsEmpty () {
        String expected = "MyTopic";
        String actual = TopicNameBuilder.getTopicFromComposedTopicName("MyTopic" + GlobalConstants.TOPIC_SEPARATOR);
        Assert.assertTrue(actual.equals(expected));
    }

    @Test
    public void shouldGetTopicWhenTenantGroupIsEmpty1 () {
        String expected = "-MyTopic";
        String actual = TopicNameBuilder.getTopicFromComposedTopicName(GlobalConstants.TOPIC_SEPARATOR + "MyTopic");
        Assert.assertTrue(actual.equals(expected));
    }

    @Test
    public void shouldGetTenantGroup () {
        String expected = "group0";
        String actual = TopicNameBuilder.getTenantGroupTopicFromComposedTopicName("MyTopic"+GlobalConstants.TOPIC_SEPARATOR+"group0");
        Assert.assertTrue(actual.equals(expected));
    }

    @Test
    public void shouldNotGetTenantGroup() {
        String expected = "";
        String actual = TopicNameBuilder.getTenantGroupTopicFromComposedTopicName("MyTopic");
        Assert.assertTrue(actual.equals(expected));
    }
}

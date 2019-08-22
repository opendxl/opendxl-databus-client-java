/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

import com.opendxl.databus.common.internal.builder.TopicNameBuilder;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.serialization.ByteArrayDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.*;

import java.util.*;

public class DatabusConsumerTest {

    @Test
    public void shouldCreateInstancesUsingConstructor1() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer consumer = new DatabusConsumer(config, new ByteArrayDeserializer());
        Assert.assertTrue(consumer != null);
        Assert.assertTrue(consumer instanceof DatabusConsumer);
    }

    @Test
    public void shouldCreateInstancesUsingConstructor3() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer consumer = new DatabusConsumer(config, new ByteArrayDeserializer());
        Assert.assertTrue(consumer != null);
        Assert.assertTrue(consumer instanceof DatabusConsumer);
    }

    @Test(expected = DatabusClientRuntimeException.class)
    public void shouldThrowAnExceptionUsingConstructor1() {
        Map<String, Object> config = null;
        Consumer consumer = new DatabusConsumer(config, new ByteArrayDeserializer());
    }

    @Test(expected = DatabusClientRuntimeException.class)
    public void shouldThrowAnExceptionUsingConstructor1DeserializerNull() {
        Map<String, Object> config = new HashMap<>();
        Consumer consumer = new DatabusConsumer(config, null);
    }

    @Test(expected = DatabusClientRuntimeException.class)
    public void shouldThrowAnExceptionUsingConstructor3() {
        Properties config = null;
        Consumer consumer = new DatabusConsumer(config,new ByteArrayDeserializer());
    }

    @Test(expected = DatabusClientRuntimeException.class)
    public void shouldThrowAnExceptionUsingConstructor3DeserializerNull() {
        Properties config = new Properties();
        Consumer consumer = new DatabusConsumer(config,null);
    }


    private Consumer getConsumer() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.intel.databus.client.serialization.DatabusMessageDeserializer");
        return new DatabusConsumer<>(config,new ByteArrayDeserializer());
    }

    @Test
    public void shouldGetTheTopicSubscribedTo() {
        try {
            String topic = "topic1";
            String tenantGroup = "group0";

            Consumer<byte[]> consumer = getConsumer();
            Map<String,List<String>> tenantGroupTopicMap = new HashMap<>();
            tenantGroupTopicMap.put(tenantGroup, Collections.singletonList(topic));
            consumer.subscribe(tenantGroupTopicMap);

            Set<String> topics = consumer.subscription();
            Iterator<String> it = topics.iterator();
            String t = it.next();
            Assert.assertTrue(t.equals(TopicNameBuilder.getTopicName(topic,tenantGroup)));

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer;

import com.opendxl.databus.common.RecordMetadata;
import com.opendxl.databus.common.internal.builder.TopicNameBuilder;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.entities.RoutingData;
import com.opendxl.databus.producer.internal.ProducerDefaultConfiguration;
import com.opendxl.databus.serialization.ByteArraySerializer;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ProducerTest {

    @Test
    public void shouldReturnDefaultConfigIfDoesNotPassed() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "DemoProducer");

        Map<String, Object> configs = new HashMap(props);

        Producer<byte[]> producer1 = new DatabusProducer<>(configs,new ByteArraySerializer());
        Producer<byte[]> producer2 = new DatabusProducer<>(props,new ByteArraySerializer());

        Assert.assertTrue(
                producer1.getConfiguration().containsKey(
                        ProducerDefaultConfiguration.MAX_BLOCK_MS_CONFIG_KEY)
        );
        Assert.assertTrue(producer1.getConfiguration().get(
                ProducerDefaultConfiguration.MAX_BLOCK_MS_CONFIG_KEY) ==
                ProducerDefaultConfiguration.MAX_BLOCK_MS_CONFIG_DEFAULT_VALUE);

        Assert.assertTrue(
                producer2.getConfiguration().containsKey(
                        ProducerDefaultConfiguration.MAX_BLOCK_MS_CONFIG_KEY)
        );
        Assert.assertTrue(producer2.getConfiguration().get(
                ProducerDefaultConfiguration.MAX_BLOCK_MS_CONFIG_KEY) ==
                ProducerDefaultConfiguration.MAX_BLOCK_MS_CONFIG_DEFAULT_VALUE);
    }

    @Test
    public void shouldReturnCustomConfigIfPassed() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "DemoProducer");
        String expectedValue = "7000";
        props.put(ProducerDefaultConfiguration.MAX_BLOCK_MS_CONFIG_KEY, expectedValue);

        Map<String, Object> configs = new HashMap(props);

        Producer<byte[]> producer1 = new DatabusProducer<>(configs,new ByteArraySerializer());
        Producer<byte[]> producer2 = new DatabusProducer<>(props,new ByteArraySerializer());

        Assert.assertTrue(
                producer1.getConfiguration().containsKey(
                        ProducerDefaultConfiguration.MAX_BLOCK_MS_CONFIG_KEY)
        );
        Assert.assertTrue(producer1.getConfiguration().get(
                ProducerDefaultConfiguration.MAX_BLOCK_MS_CONFIG_KEY) == expectedValue);

        Assert.assertTrue(
                producer2.getConfiguration().containsKey(
                        ProducerDefaultConfiguration.MAX_BLOCK_MS_CONFIG_KEY)
        );
        Assert.assertTrue(producer2.getConfiguration().get(
                ProducerDefaultConfiguration.MAX_BLOCK_MS_CONFIG_KEY) == expectedValue);
    }

    //TODO: CHECK THIS WHY IS USING IGNORE
    @Ignore
    @Test
    public void shouldSendAMessageThruPlainTextProducer() {
        Map configs = new HashMap<>();

        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("client.id", "DemoProducer");
        String topic = "topic1";
        String group = "default";
        String expectedTopic = TopicNameBuilder.getTopicName(topic, group);

        Producer<byte[]> producer = new DatabusProducer(configs,new ByteArraySerializer());

        ProducerRecord<byte[]> producerRecord =
                new ProducerRecord<>(new RoutingData("topic1","key","default"),
                        null,new MessagePayload<>("Hello World".getBytes()));

        CallbackTest callback = new CallbackTest();
        producer.send(producerRecord, callback);

        Assert.assertTrue(expectedTopic.equals(callback.getTopic()));
        Assert.assertTrue(0 == callback.getPartition());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowAnExceptionWhenMessageIsNull()  {
        Map configs = new HashMap<String, Object>();

        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("client.id", "DemoProducer");
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<byte[]> producer = new DatabusProducer(configs,new ByteArraySerializer());

        ProducerRecord producerRecord = null;

        producer.send(producerRecord);
    }

    @Test
    public void shouldConstructProducerMockWithAllConstructors() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("client.id", "DemoProducer");

        Map configs = properties;

        Producer<byte[]> prod1 = new DatabusProducer(configs, new ByteArraySerializer());
        Producer<byte[]> prod3 = new DatabusProducer(properties, new ByteArraySerializer());

        Assert.assertTrue(prod1 != null && prod1 instanceof DatabusProducer);
        Assert.assertTrue(prod3 != null && prod3 instanceof DatabusProducer);

    }

    @Test
    public void shouldThrowAnExceptionUsingConstructorWhenConfigsIsNull() {
        Map<String, Object> config = null;
        try {
            Producer producer = new DatabusProducer(config, new ByteArraySerializer());
            Assert.fail("new DatabusProducer() should fail when configs argument is null");
        } catch (final DatabusClientRuntimeException e) {
            assertThat(e.getMessage(), is("A DatabusProducer instance cannot be created: "
                    + "config properties cannot be null"));
        }
    }

    @Test
    public void shouldThrowAnExceptionUsingConstructorWhenPropertiesIsNull() {
        Properties properties = null;
        try {
            Producer producer = new DatabusProducer(properties, new ByteArraySerializer());
            Assert.fail("new DatabusProducer() should fail when configs argument is null");
        } catch (final DatabusClientRuntimeException e) {
            assertThat(e.getMessage(), is("A DatabusProducer instance cannot be created: "
                    + "config properties cannot be null"));
        }
    }

    @Test
    public void shouldThrowAnExceptionUsingConstructorWhenSerializerIsNull() {
        Map<String, Object> config = new HashMap<>();
        try {
            Producer producer = new DatabusProducer(config, null);
            Assert.fail("new DatabusProducer() should fail when messageSerializer argument is null");
        } catch (final DatabusClientRuntimeException e) {
            assertThat(e.getMessage(), is("A DatabusProducer instance cannot be created: "
                    + "Message Serializer cannot be null"));
        }
    }

    private static class CallbackTest  implements Callback {
        private long offset;
        private int partition;
        private String topic;

        public long getOffset() {
            return offset;
        }

        public int getPartition() {
            return partition;
        }

        public String getTopic() {
            return topic;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            this.offset=  metadata.offset();
            this.partition =  metadata.partition();
            this.topic = metadata.topic();
        }
    }
}

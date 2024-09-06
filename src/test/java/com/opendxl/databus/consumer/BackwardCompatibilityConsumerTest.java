/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

import broker.ClusterHelper;

import com.opendxl.databus.util.Constants;
import com.opendxl.databus.util.ConsumerHelper;
import com.opendxl.databus.util.Topic;
import com.opendxl.databus.producer.ProducerRecord;
import com.opendxl.databus.util.ProducerHelper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Ignore
public class BackwardCompatibilityConsumerTest {


    public void startKafkaCluster(final Properties kafkaProperties) throws IOException {

        ClusterHelper.getInstance()
                .addBroker(Integer.valueOf(Constants.KAFKA_PORT))
                .zookeeperPort(Integer.valueOf(Constants.ZOOKEEPER_PORT))
                .start();
    }

    @Test
    public void ShouldConsumerSDKMessagesFormatVersion090() throws IOException {
        final int numberOfRecords = 10;
        final Properties kafkaProperties = getKafkaPropertiesForMessageFormat090();
        produceAndConsumeWithStreamingSDK(numberOfRecords, kafkaProperties);
    }

    @Test
    public void ShouldConsumerSDKMessagesFormatVersion01021() throws IOException {
        final int numberOfRecords = 10;
        final Properties kafkaProperties = getKafkaPropertiesForMessageFormat01021();
        produceAndConsumeWithStreamingSDK(numberOfRecords, kafkaProperties);
    }

    @Test
    public void ShouldConsumerSDKMessagesFormatVersion23() throws IOException {
        final int numberOfRecords = 10;
        final Properties kafkaProperties = getKafkaPropertiesForMessageFormat23();
        produceAndConsumeWithStreamingSDK(numberOfRecords, kafkaProperties);
    }

    @Test
    public void ShouldProduceKafkaRawMessagesAndConsumerSDKMessagesFormatVersion090() throws IOException {
        final int numberOfRecords = 1;
        final Properties kafkaProperties = getKafkaPropertiesForMessageFormat090();
        produceWithKafkaRawAndConsumeWithStreamingSDK(numberOfRecords,kafkaProperties);
    }

    @Test
    public void ShouldProduceKafkaRawMessagesAndConsumerSDKMessagesFormatVersion01021() throws IOException {
        final int numberOfRecords = 1;
        final Properties kafkaProperties = getKafkaPropertiesForMessageFormat01021();
        produceWithKafkaRawAndConsumeWithStreamingSDK(numberOfRecords,kafkaProperties);
    }

    @Test
    public void ShouldProduceKafkaRawMessagesAndConsumerSDKMessagesFormatVersion23() throws IOException {
        final int numberOfRecords = 1;
        final Properties kafkaProperties = getKafkaPropertiesForMessageFormat23();
        produceWithKafkaRawAndConsumeWithStreamingSDK(numberOfRecords,kafkaProperties);
    }

    @Test
    public void ShouldProduceKafkaRawMessagesAndConsumerSDKMessagesFormatLatest() throws IOException {
        final int numberOfRecords = 1;
        final Properties kafkaProperties = getKafkaPropertiesWithoutFormatMessage();
        produceWithKafkaRawAndConsumeWithStreamingSDK(numberOfRecords,kafkaProperties);
    }


    private void produceWithKafkaRawAndConsumeWithStreamingSDK(final int numberOfRecords,
                                                               final Properties kafkaProperties) throws IOException {
        // Start cluster
        startKafkaCluster(kafkaProperties);

        // create a topic
        final String topicName = createRandomTopicName();

        // produceWithStreamingSDK records
        String payload = "Hello world at: " + System.currentTimeMillis();
        produceWithKafka(topicName, payload);

        // Consume records
        List<ConsumerRecord<byte[]>> recordsConsumed = consume(topicName, numberOfRecords);

        // Check that all records were consumed successfully
        Assert.assertEquals(numberOfRecords, recordsConsumed.size());
        for (ConsumerRecord<byte[]> recordConsumed : recordsConsumed) {
            final String actualPayload = new String(recordConsumed.getMessagePayload().getPayload());
            Assert.assertEquals(payload, actualPayload);
            Assert.assertEquals(topicName, recordConsumed.getTopic());
        }

        // Stop Cluster
        stopKafkaCluster();
    }


    private void produceAndConsumeWithStreamingSDK(final int numberOfRecords,
                                                   final Properties kafkaProperties) throws IOException {
        // Start cluster
        startKafkaCluster(kafkaProperties);

        // create a topic
        final String topicName = createRandomTopicName();

        // produceWithStreamingSDK records
        Map<String, ProducerRecord<byte[]>> recordsProduced = produceWithStreamingSDK(topicName, numberOfRecords);

        // Check records were produced
        Assert.assertTrue(recordsProduced != null);
        Assert.assertEquals(numberOfRecords, recordsProduced.size());

        // Consume records
        List<ConsumerRecord<byte[]>> recordsConsumed = consume(topicName, numberOfRecords);

        // Check that all records were consumed successfully
        Assert.assertEquals(numberOfRecords, recordsConsumed.size());
        for (ConsumerRecord<byte[]> recordConsumed : recordsConsumed) {
            ProducerRecord<byte[]> producerRecord = recordsProduced.get(recordConsumed.getKey());
            Assert.assertEquals(producerRecord.getRoutingData().getTopic(), recordConsumed.getTopic());
            Assert.assertTrue(Arrays.equals(producerRecord.payload().getPayload(),
                    recordConsumed.getMessagePayload().getPayload()));
        }

        // Stop Cluster
        stopKafkaCluster();
    }

    private void stopKafkaCluster() {
        ClusterHelper.getInstance().stop();
    }



    private Properties getKafkaPropertiesForMessageFormat090() throws IOException {
        Properties properties = getKafkaDefaultProperties();
        properties.setProperty("log.message.format.version", "0.9.0");
        return properties;
    }

    private Properties getKafkaPropertiesForMessageFormat01021() throws IOException {
        Properties properties = getKafkaDefaultProperties();
        properties.setProperty("log.message.format.version", "0.10.0");
        return properties;
    }

    private Properties getKafkaPropertiesForMessageFormat23() throws IOException {
        Properties properties = getKafkaDefaultProperties();
        properties.setProperty("log.message.format.version", "2.3");
        return properties;
    }

    private Properties getKafkaPropertiesWithoutFormatMessage() throws IOException {
        // Get don't override message format in order to let kafka to use the latest
        Properties properties = getKafkaDefaultProperties();
        return properties;
    }


    private Properties getKafkaDefaultProperties() throws IOException {
        File logsDir1;

        Properties kafkaProperties;

        logsDir1 = Files.createTempDirectory(Constants.LOG_PATH_PREFIX).toFile();
        kafkaProperties = new Properties();
        kafkaProperties.setProperty("zookeeper.connect", Constants.ZOOKEEPER_HOST.concat(":").concat(Constants.ZOOKEEPER_PORT));
        kafkaProperties.setProperty("broker.id", "1");
        kafkaProperties.setProperty("host.name", Constants.KAFKA_HOST);
        kafkaProperties.setProperty("port", Constants.KAFKA_PORT);
        kafkaProperties.setProperty("log.dir", logsDir1.getAbsolutePath());
        kafkaProperties.setProperty("log.flush.interval.messages", String.valueOf(1));
        kafkaProperties.setProperty("delete.topic.enable", String.valueOf(true));
        kafkaProperties.setProperty("offsets.topic.replication.factor", String.valueOf(1));
        kafkaProperties.setProperty("auto.create.topics.enable", String.valueOf(true));

        return kafkaProperties;
    }


    public String createRandomTopicName() {
        final String topicName = createTopic()
                .partitions(1)
                .go();
        return topicName;
    }

    public Map<String, ProducerRecord<byte[]>> produceWithStreamingSDK(final String topicName,
                                                                       final int numOfRecords) {
        try {
            // produceWithStreamingSDK records
            Map<String, ProducerRecord<byte[]>> recordsProduced = ProducerHelper.produceTo(topicName)
                    .numberOfRecords(numOfRecords)
                    .produce()
                    .asMap();

            return recordsProduced;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void produceWithKafka(final String topicName, final String payload) {

        // Produce a record
        final Properties props = new Properties();

        props.put("bootstrap.servers",
                Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT));
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        org.apache.kafka.clients.producer.ProducerRecord<String, String> producerRecord =
                new org.apache.kafka.clients.producer.ProducerRecord<>(topicName,
                        0,
                        "p0", payload);

        try {
            producer.send(producerRecord).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }


    public List<ConsumerRecord<byte[]>> consume(final String topicName, final int numOfRecords) {
        try {

            // consume records
            List<ConsumerRecord<byte[]>> recordsConsumed = ConsumerHelper.consumeFrom(topicName)
                    .consumerGroup(topicName) // We use topic name for the consumer group name
                    .numberOfRecords(numOfRecords)
                    .config(new HashMap<String, Object>() {{
                        put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, false);
                        put(ConsumerConfiguration.AUTO_OFFSET_RESET_CONFIG, "earliest");
                    }})
                    .consume()
                    .close()
                    .asList();

            return recordsConsumed;


        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private Topic.Builder createTopic() {
        return new Topic.Builder();
    }

}
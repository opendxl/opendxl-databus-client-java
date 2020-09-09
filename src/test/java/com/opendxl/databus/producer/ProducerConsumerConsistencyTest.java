/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer;

import broker.ClusterHelper;
import com.opendxl.databus.consumer.*;
import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.entities.RoutingData;
import com.opendxl.databus.entities.internal.DatabusMessage;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.serialization.ByteArrayDeserializer;
import com.opendxl.databus.serialization.ByteArraySerializer;
import com.opendxl.databus.serialization.internal.*;
import com.opendxl.databus.util.Constants;
import junit.extensions.PA;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ProducerConsumerConsistencyTest {

    private static final String TENANT_GROUP = "group0";
    private Producer<byte[]> producer;
    private Consumer<byte[]> consumer;
    private ExecutorService executor;


    @BeforeClass
    public static void beforeClass() throws IOException {
        ClusterHelper.getInstance()
                .addBroker(Integer.valueOf(Constants.KAFKA_PORT))
                .zookeeperPort(Integer.valueOf(Constants.ZOOKEEPER_PORT))
                .start();

    }

    @AfterClass
    public static void afterClass() {
        ClusterHelper.getInstance().stop();
    }


    /**
     * Check that all messages produced (headers and payload) are consumed consistently
     * Check that crosstalk is prevented when multiple threads are sharing the same producer.
     */
    @Test
    public void payloadAndHeadersProducedShouldBeConsistentAcrossTopics() {

        producer = getProducer();
        consumer = getConsumer();

        try {
            produceAndConsumeMultiThreading();
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            producer.close();
            executor.shutdownNow();
        }
    }


    /**
     * It injects an Unsafe and Mock Serializer in order to force corrupted messages
     * when multiple threads share the same producer.
     */
    @Test(expected = ExecutionException.class)
    public void shouldFailsWhenMessagesWereAltered() throws ExecutionException, InterruptedException {
        try {
            producer = getProducerWithMockSerializer();
            consumer = getConsumer();
            produceAndConsumeMultiThreading();
        } finally {
            producer.close();
            executor.shutdownNow();
        }
    }


    /**
     * Create a Kafka Producer with a MockMessageSerializer. Then Kafka Producer is injected into Databus Producer
     */
    private Producer<byte[]> getProducerWithMockSerializer() {

        final Map config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-" + new Random().nextInt(100000));
        config.put(ProducerConfig.LINGER_MS_CONFIG, "0");

        org.apache.kafka.clients.producer.Producer<String, DatabusMessage> kafkaProducer =
                new KafkaProducer(config, new DatabusKeySerializer(), new MockMessageSerializer());

        producer = getProducer();
        PA.setValue(producer, "producer", kafkaProducer);
        return producer;
    }


    /**
     * Produce and Consume Multithreading.
     * It spawns 60 threads and produce 300000 records to 2 topics
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private void produceAndConsumeMultiThreading() throws InterruptedException, ExecutionException {
        final String tenantId1 = UUID.randomUUID().toString();
        final String tenantId2 = UUID.randomUUID().toString();

        final String topic1 = "topic1-" + tenantId1;
        final String topic2 = "topic2-" + tenantId2;

        final int numberOfProducerThreadsPerTopic = 30;
        final int numberOfRecordsToBeProducedPerProducer = 5000;


        // producer tasks holder
        List<Callable<ProducerResult>> producerTasksHolder = new ArrayList<>();

        // Add a list of producer tasks to produce to topic1 , tenantId1
        producerTasksHolder.addAll(getProducers(tenantId1,
                topic1, numberOfProducerThreadsPerTopic, numberOfRecordsToBeProducedPerProducer));

        // Add a producer tasks to produce to topic2 , tenantId2
        producerTasksHolder.addAll(getProducers(tenantId2,
                topic2, numberOfProducerThreadsPerTopic, numberOfRecordsToBeProducedPerProducer));

        // Create Consumer Task
        Callable<ConsumerResult> consumerTask =
                getConsumerTask(numberOfRecordsToBeProducedPerProducer * numberOfProducerThreadsPerTopic,
                        Arrays.asList(topic1, topic2));

        // Set executor threads to support the list of producers and a consumer
        executor = Executors.newFixedThreadPool(producerTasksHolder.size() + 1);


        // Start a consumer to read messages
        final Future<ConsumerResult> consumerFuture = executor.submit(consumerTask);

        // Start multiple threads to write messages by using the same Databus producer
        final List<Future<ProducerResult>> producerFutures = executor.invokeAll(producerTasksHolder);

        // Wait for each producer task finishes and collect producer result in a list
        List<ProducerResult> producerResults = producerFutures
                .stream()
                .map(future -> {
                    try {
                        return future.get();
                    } catch (Exception e) {
                        Assert.fail(e.getMessage());
                    }
                    return null;
                }).collect(Collectors.toList());

        // Wait for consumer task finishes
        ConsumerResult consumerResult = consumerFuture.get();

        // Collect producer results in a payloadProducedPerTopic list
        Map<String, List<String>> payloadProducedPerTopic = new HashMap<>();
        for (ProducerResult producerResult : producerResults) {
            for (Map.Entry<String, List<String>> entry : producerResult.getPayloadProduced().entrySet()) {
                if (payloadProducedPerTopic.containsKey(entry.getKey())) {
                    payloadProducedPerTopic.get(entry.getKey()).addAll(entry.getValue());
                } else {
                    payloadProducedPerTopic.put(entry.getKey(), entry.getValue());
                }
            }
        }

        // Sort payloadProducedPerTopic
        for (Map.Entry<String, List<String>> entry : payloadProducedPerTopic.entrySet()) {
            entry.getValue().sort(Comparator.naturalOrder());
        }

        // Sort payloadConsumedPerTopic
        Map<String, List<String>> payloadConsumedPerTopic = consumerResult.getPayloadConsumedPerTopic();
        for (Map.Entry<String, List<String>> entry : payloadConsumedPerTopic.entrySet()) {
            entry.getValue().sort(Comparator.naturalOrder());
        }

        // Check number of messages produced per each Producer
        producerResults.forEach(producerResult ->
                Assert.assertTrue(producerResult.getRecordsProduced()
                        == numberOfRecordsToBeProducedPerProducer)
        );

        // Check number of messages consumed
        Map<String, Integer> summary = consumerResult.getSummary();
        for (Map.Entry<String, Integer> entry : summary.entrySet()) {
            // Assert number of messages consumed
            Assert.assertTrue(entry.getValue()
                    == numberOfRecordsToBeProducedPerProducer * numberOfProducerThreadsPerTopic);
        }


        // Match messages consumed with messages produced
        Assert.assertTrue(
                payloadConsumedPerTopic.entrySet()
                        .stream()
                        .allMatch(payloadConsumedEntry ->
                                payloadConsumedEntry
                                        .getValue()
                                        .equals(payloadProducedPerTopic.get(payloadConsumedEntry.getKey()))
                        )
        );

        // Match messages produced with messages consumed
        Assert.assertTrue(
                payloadProducedPerTopic.entrySet()
                        .stream()
                        .allMatch(payloadProducedEntry ->
                                payloadProducedEntry
                                        .getValue()
                                        .equals(payloadConsumedPerTopic.get(payloadProducedEntry.getKey()))
                        )
        );
    }


    /**
     * @param numberOfRecordsToProduce total number of records to be produced by this task.
     *                                 Also, It used at the end of the payload
     * @param topicName                topic name without the tenant group
     * @param tenantId                 tenant id
     * @param producerId               producer id
     * @return a Producer callable task
     */
    private Callable<ProducerResult> getProducerTask(final int numberOfRecordsToProduce,
                                                     final String topicName,
                                                     final String tenantId,
                                                     final int producerId) {
        return () -> {

            Map<String, List<String>> payloadProduced = new HashMap<>();
            payloadProduced.put(topicName + "-" + TENANT_GROUP, new ArrayList<>());
            int recordNumber = 0;
            Instant start = Instant.now();


            while (recordNumber < numberOfRecordsToProduce) {

                // Prepare a record
                final String message = topicName + "-" + TENANT_GROUP + " producerId:" + producerId
                        + " record#:" + recordNumber;

                final byte[] payload = message.getBytes(Charset.defaultCharset());
                final ProducerRecord<byte[]> producerRecord = getProducerRecord(topicName, payload, recordNumber,
                        tenantId, producerId);

                // Produce the record
                final CountDownLatch latch = new CountDownLatch(1);
                producer.send(producerRecord, (metadata, exception) -> {
                    if (exception == null) {
                        Assert.assertTrue(metadata.topic().equals(topicName + "-" + TENANT_GROUP));
                        latch.countDown();
                    }
                });
                latch.await();
                payloadProduced.get(topicName + "-" + TENANT_GROUP).add(message);

                recordNumber++;
            }

            return new ProducerResult(recordNumber, payloadProduced);
        };
    }


    /**
     * @param numberOfRecordsToConsumePerTopic total number of records to consume per topic
     * @param topicNames                       List of topic names to subscribe to
     * @return a Consumer callable task
     */
    private Callable<ConsumerResult> getConsumerTask(final int numberOfRecordsToConsumePerTopic,
                                                     final List<String> topicNames) {
        return () -> {

            Map<String, List<String>> topics = new HashMap<>();
            topics.put(TENANT_GROUP, topicNames);
            consumer.subscribe(topics);

            Map<String, Integer> summary = new HashMap();
            Map<String, List<String>> topicPayloads = new HashMap();
            for (String topicName : topicNames) {
                summary.put(topicName + "-" + TENANT_GROUP, 0);
                topicPayloads.put(topicName + "-" + TENANT_GROUP, new ArrayList<>());
            }

            boolean continuePolling = true;

            try {
                while (continuePolling) {

                    final ConsumerRecords<byte[]> records = consumer.poll(Duration.ofMillis(100));
                    if (records.count() == 0) {
                        continue;
                    }

                    for (ConsumerRecord<byte[]> record : records) {

                        // Check Payload
                        final String payload = new String(record.getMessagePayload().getPayload());
                        final String recordNumber = record.getHeaders().get("recordNumber");
                        final String producerId = record.getHeaders().get("producerId");
                        final String expectedPayload = record.getComposedTopic() + " producerId:" + producerId
                                + " record#:" + recordNumber;
                        if (!payload.equals(expectedPayload)) {
                            throw new Exception("INCONSISTENCY ERROR: Expected payload: "
                                    + expectedPayload + " Current Payload: " + payload);
                        }

                        // Check Tenant Id
                        final String tenantId = record.getHeaders().get("tenantId");
                        final String expectedTenantId = record.getTopic().substring(record.getTopic().indexOf("-") + 1);
                        if (!tenantId.equals(expectedTenantId)) {
                            throw new Exception("INCONSISTENCY ERROR: Expected tenantId: "
                                    + expectedTenantId + " Current tenantId: " + tenantId);
                        }

                        // Check Topic Name
                        final String topicName = record.getHeaders().get("topicName");
                        if (!topicName.equals(record.getComposedTopic())) {
                            throw new Exception("INCONSISTENCY ERROR: Expected topicName: "
                                    + record.getComposedTopic() + " Current topicName: " + topicName);
                        }

                        // Update Consumer Results
                        int count = summary.get(record.getComposedTopic());
                        summary.put(record.getComposedTopic(), new Integer(++count));
                        topicPayloads.get(record.getComposedTopic()).add(payload);

                    }
                    consumer.commitSync();

                    // Check if loop should continue
                    continuePolling = false;
                    for (Map.Entry<String, Integer> entry : summary.entrySet()) {
                        if (entry.getValue() < numberOfRecordsToConsumePerTopic) {
                            continuePolling = true;
                            break;
                        }
                    }

                }
            } catch (Exception e) {
                throw e;
            } finally {
                consumer.close();
            }
            return new ConsumerResult(summary, topicPayloads);

        };
    }


    /**
     * @param topic        topic to subscribe to pull from
     * @param payload      payload to be sent
     * @param recordNumber record identifier
     * @param tenantId     tenantId
     * @param producerId   producer id
     * @return record to be produced
     */
    private ProducerRecord<byte[]> getProducerRecord(final String topic,
                                                    final byte[] payload,
                                                    final int recordNumber,
                                                    final String tenantId,
                                                    final int producerId) {

        String key = String.valueOf(System.currentTimeMillis());
        RoutingData routingData = new RoutingData(topic, key, TENANT_GROUP);
        Headers headers = new Headers();
        headers.put("topicName", topic + "-" + TENANT_GROUP);
        headers.put("recordNumber", String.valueOf(recordNumber));
        headers.put("tenantId", tenantId);
        headers.put("producerId", String.valueOf(producerId));
        MessagePayload<byte[]> messagePayload = new MessagePayload<>(payload);
        return new ProducerRecord<>(routingData, headers, messagePayload);
    }


    /**
     * @return a Databus Producer
     */
    private Producer<byte[]> getProducer() {
        final Map config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-" + new Random().nextInt(100000));
        config.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        return new DatabusProducer<>(config, new ByteArraySerializer());
    }


    /**
     * @return a Databus Consumer
     */
    private Consumer<byte[]> getConsumer() {
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfiguration.GROUP_ID_CONFIG, "cg1" + new Random().nextInt(100000));
        consumerProps.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        consumerProps.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        consumerProps.put(ConsumerConfiguration.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DatabusConsumer<>(consumerProps, new ByteArrayDeserializer());
    }


    /**
     * @param tenantId                               tenant id
     * @param topic                                  topic name
     * @param numberOfProducerPerTopic               number of Producers to produce per topic
     * @param numberOfRecordsToBeProducedPerProducer number of records to produce for each Producer
     * @return a List a Producer callable tasks
     */
    private List<Callable<ProducerResult>> getProducers(final String tenantId,
                                                          final String topic,
                                                          final int numberOfProducerPerTopic,
                                                          final int numberOfRecordsToBeProducedPerProducer) {
        List<Callable<ProducerResult>> producerTasks = new ArrayList<>();
        for (int producerId = 0; producerId < numberOfProducerPerTopic; producerId++) {
            producerTasks.add(getProducerTask(numberOfRecordsToBeProducedPerProducer, topic, tenantId, producerId));
        }
        return producerTasks;
    }


    /**
     * Custom class to set Producer Result
     */
    private class ProducerResult {
        private int recordsProduced;
        private Map<String, List<String>> payloadProduced;

        public ProducerResult(int recordsProduced, Map<String, List<String>> payloadProduced) {
            this.recordsProduced = recordsProduced;
            this.payloadProduced = payloadProduced;
        }

        public int getRecordsProduced() {
            return recordsProduced;
        }

        public Map<String, List<String>> getPayloadProduced() {
            return payloadProduced;
        }

    }


    /**
     * Custome class to set Consumer Result
     */
    private class ConsumerResult {
        private final Map<String, Integer> summary;
        private final Map<String, List<String>> payloadConsumedPerTopic;

        public ConsumerResult(final Map<String, Integer> summary,
                              final Map<String, List<String>> payloadConsumedPerTopic) {
            this.summary = summary;
            this.payloadConsumedPerTopic = payloadConsumedPerTopic;
        }

        public Map<String, Integer> getSummary() {
            return summary;
        }

        public Map<String, List<String>> getPayloadConsumedPerTopic() {
            return payloadConsumedPerTopic;
        }

    }


    /**
     * Unsafe Avro Message Serializer
     */
    private class UnsafeAvroMessageSerializer implements InternalSerializer<DatabusMessage> {

        protected static final String HEADERS_FIELD_NAME = "headers";
        protected static final String PAYLOAD_FIELD_NAME = "payload";
        private final GenericData.Record databusValue;
        private final DatumWriter<GenericRecord> writer;

        public UnsafeAvroMessageSerializer(final Schema schema) {
            this.databusValue = new GenericData.Record(schema);
            this.writer = new GenericDatumWriter<>(schema);
        }

        @Override
        public byte[] serialize(final DatabusMessage data) {

            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {

                databusValue.put(HEADERS_FIELD_NAME, data.getHeaders().getAll());
                databusValue.put(PAYLOAD_FIELD_NAME, ByteBuffer.wrap(data.getPayload()));

                final BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);

                writer.write(databusValue, encoder);
                encoder.flush();

                final byte[] bytes = out.toByteArray();

                return bytes;

            } catch (Exception e) {
                throw new DatabusClientRuntimeException("Error serializing Avro message"
                        + e.getMessage(), e, com.opendxl.databus.serialization.internal.AvroMessageSerializer.class);
            }
        }
    }


    /**
     * Message Serializer Mock class
     */
    private class MockMessageSerializer implements org.apache.kafka.common.serialization.Serializer<DatabusMessage> {

        private final UnsafeAvroMessageSerializer serializer;

        public MockMessageSerializer() {
            this.serializer = new UnsafeAvroMessageSerializer(AvroV1MessageSchema.getSchema());
        }

        @Override
        public void configure(final Map<String, ?> map, final boolean b) {

        }

        @Override
        public byte[] serialize(final String topic, final DatabusMessage message) {

            final byte[] rawMessage = serializer.serialize(message);

            final RegularMessageStructure structure =
                    new RegularMessageStructure(MessageStructureConstant.REGULAR_STRUCTURE_MAGIC_BYTE,
                            MessageStructureConstant.AVRO_1_VERSION_NUMBER, rawMessage);

            return structure.getMessage();
        }

        @Override
        public void close() {

        }
    }


}

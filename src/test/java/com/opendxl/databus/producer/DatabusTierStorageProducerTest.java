package com.opendxl.databus.producer;

import broker.ClusterHelper;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.opendxl.databus.common.internal.util.HeaderInternalField;
import com.opendxl.databus.consumer.*;
import com.opendxl.databus.entities.*;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.serialization.ByteArrayDeserializer;
import com.opendxl.databus.serialization.ByteArraySerializer;
import io.findify.s3mock.S3Mock;
import junit.extensions.PA;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DatabusTierStorageProducerTest {

    private static final String AWS_SECRET_KEY = "secretKey";
    private static final String AWS_ACCESS_KEY = "accessKey";
    private static final String AWS_REGION = "us-east-1";
    private static final String BUCKET_NAME = "databus-poc-test";
    private static S3Mock api;
    private static AmazonS3Client client;
    private static S3TierStorage tierStorage;

    @BeforeClass
    public static void beforeClass() {
        // Start Kafka cluster
        ClusterHelper
                .getInstance()
                .addBroker(9092)
                .zookeeperPort(2181)
                .start();

        api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
        api.start();
        AwsClientBuilder.EndpointConfiguration endpoint =
                new AwsClientBuilder
                        .EndpointConfiguration("http://localhost:8001", "us-east-1");

        client = (AmazonS3Client) AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();

        tierStorage = new S3TierStorage(AWS_REGION, new ClientConfiguration(),
                AWS_ACCESS_KEY, AWS_SECRET_KEY);
        PA.setValue(tierStorage, "s3Client", client);

    }
    @AfterClass
    public static void afterClass() {
        ClusterHelper.getInstance().stop();
        api.shutdown(); // kills the underlying actor system. Use api.stop() to just unbind the port.
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWhenTierStorageIsNull() {
        final Map config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-id-sample");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, "150000");
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        new DatabusTierStorageProducer<byte[]>(config, new ByteArraySerializer(), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWhenTransactionIdIsNotDefined() {
        final Map config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-id-sample");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, "150000");

        // The following line is commented on purpose to show that transaction id is not configured
        //config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());

        new DatabusTierStorageProducer<byte[]>(config, new ByteArraySerializer(), tierStorage);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWhenTransactionIdIsNull() {
        final Map config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-id-sample");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, "150000");

        // The following line set TransactionId null
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, null);

        new DatabusTierStorageProducer<byte[]>(config, new ByteArraySerializer(), tierStorage);
    }

    @Test(expected = DatabusClientRuntimeException.class)
    public void shouldFailWhenTransactionIdIsEmpty() {
        final Map config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-id-sample");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, "150000");

        // The following line set TransactionId empty
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "");

        new DatabusTierStorageProducer<byte[]>(config, new ByteArraySerializer(), tierStorage);
    }


    @Test
    public void shouldProduceARecordWithCallBackAndTierStorageRecord() {
        final String topicName = UUID.randomUUID().toString();

        Producer<byte[]> producer = null;
        Consumer<byte[]> consumer = null;

        try {
            producer = getProducer();

            // Prepare a record
            final String message = "Hello World at "+ LocalDateTime.now();
            final byte[] payload = message.getBytes(Charset.defaultCharset());
            final String key = UUID.randomUUID().toString();
            final ProducerRecord<byte[]> producerRecord = getProducerRecord(topicName, payload, key);

            // Send the record and set an anonymous callback for check the result
            CountDownLatch latch = new CountDownLatch(1);
            producer.send(producerRecord, (metadata, exception) -> {
                try {
                    if(exception != null) {
                        Assert.fail(exception.getMessage());
                    }
                } finally {
                    latch.countDown();
                }
            });

            // Wait for callback being invoked by Kafka
            boolean isTimeout = latch.await(10000, TimeUnit.MILLISECONDS);
            if(!isTimeout) {
                Assert.fail("Producer take a long time to produce a record");
                return;
            }

            // Consume the record
            consumer = getConsumer();
            consumer.subscribe(Collections.singletonList(topicName));
            boolean closed = false;
            while(!closed) {
                final ConsumerRecords<byte[]> records = consumer.poll(1000);
                for(ConsumerRecord<byte[]> record : records) {
                    if(record.getKey().equals(key)) {
                        final String actualMessage = new String(record.getMessagePayload().getPayload());
                        Assert.assertTrue(actualMessage.equals(message));
                        closed = true;
                        break;
                    }
                }
            }
            Assert.assertTrue(tierStorage.doesObjectExist(BUCKET_NAME, topicName + key));

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (consumer != null) {
                consumer.close();
            }

        }
    }

    @Test
    public void shouldProduceARecordWithoutCallBackAndTierStorageRecord() {
        final String topicName = UUID.randomUUID().toString();

        Producer<byte[]> producer = null;
        Consumer<byte[]> consumer = null;

        try {
            producer = getProducer();

            // Prepare a record
            final String message = "Hello World at "+ LocalDateTime.now();
            final byte[] payload = message.getBytes(Charset.defaultCharset());
            final String key = UUID.randomUUID().toString();
            final ProducerRecord<byte[]> producerRecord = getProducerRecord(topicName, payload, key);

            // Send the record
            producer.send(producerRecord);

            // Consume the record
            consumer = getConsumer();
            consumer.subscribe(Collections.singletonList(topicName));
            boolean closed = false;
            while(!closed) {
                final ConsumerRecords<byte[]> records = consumer.poll(1000);
                for(ConsumerRecord<byte[]> record : records) {
                    if(record.getKey().equals(key)) {
                        final String actualMessage = new String(record.getMessagePayload().getPayload());
                        Assert.assertTrue(actualMessage.equals(message));
                        closed = true;
                        break;
                    }
                }
            }
            Assert.assertTrue(tierStorage.doesObjectExist(BUCKET_NAME, topicName + key));

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (consumer != null) {
                consumer.close();
            }

        }
    }


    @Test
    public void shouldConsumeWithoutTierStorage() {
        final String topicName = UUID.randomUUID().toString();

        Producer<byte[]> producer = null;
        Consumer<byte[]> consumer = null;

        try {
            producer = getProducer();

            // Prepare a record
            final String message = "Hello World at "+ LocalDateTime.now();
            final byte[] payload = message.getBytes(Charset.defaultCharset());
            final String key = UUID.randomUUID().toString();
            final ProducerRecord<byte[]> producerRecord = getProducerRecord(topicName, payload, key);

            // Send the record and set an anonymous callback for check the result
            CountDownLatch latch = new CountDownLatch(1);
            producer.send(producerRecord, (metadata, exception) -> {
                try {
                    if(exception != null) {
                        Assert.fail(exception.getMessage());
                    }
                } finally {
                    latch.countDown();
                }
            });

            // Wait for callback being invoked by Kafka
            boolean isTimeout = latch.await(10000, TimeUnit.MILLISECONDS);
            if(!isTimeout) {
                Assert.fail("Producer take a long time to produce a record");
                return;
            }

            // Consume the record
            consumer = getConsumerWOTierStorage();
            consumer.subscribe(Collections.singletonList(topicName));
            boolean closed = false;
            while(!closed) {
                final ConsumerRecords<byte[]> records = consumer.poll(500);
                for (ConsumerRecord<byte[]> record : records) {
                    final Headers headers = record.getHeaders();
                    final String bucketName = headers.get(HeaderInternalField.TIER_STORAGE_BUCKET_NAME_KEY);
                    final String objectName = headers.get(HeaderInternalField.TIER_STORAGE_OBJECT_NAME_KEY);
                    Assert.assertTrue(bucketName.equals(BUCKET_NAME));
                    Assert.assertTrue(objectName.equals(topicName + key));
                    closed = true;
                    break;
                }
            }

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (consumer != null) {
                consumer.close();
            }
        }
    }


    @Test
    public void shouldConsumeWithoutCallbackAndTierStorage() {
        final String topicName = UUID.randomUUID().toString();

        Producer<byte[]> producer = null;
        Consumer<byte[]> consumer = null;

        try {
            producer = getProducer();

            // Prepare a record
            final String message = "Hello World at "+ LocalDateTime.now();
            final byte[] payload = message.getBytes(Charset.defaultCharset());
            final String key = UUID.randomUUID().toString();
            final ProducerRecord<byte[]> producerRecord = getProducerRecord(topicName, payload, key);

            // Send the record
            producer.send(producerRecord);

            // Consume the record
            consumer = getConsumerWOTierStorage();
            consumer.subscribe(Collections.singletonList(topicName));
            boolean closed = false;
            while(!closed) {
                final ConsumerRecords<byte[]> records = consumer.poll(500);
                for (ConsumerRecord<byte[]> record : records) {
                    final Headers headers = record.getHeaders();
                    final String bucketName = headers.get(HeaderInternalField.TIER_STORAGE_BUCKET_NAME_KEY);
                    final String objectName = headers.get(HeaderInternalField.TIER_STORAGE_OBJECT_NAME_KEY);
                    Assert.assertTrue(bucketName.equals(BUCKET_NAME));
                    Assert.assertTrue(objectName.equals(topicName + key));
                    closed = true;
                    break;
                }
            }

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            if (producer != null) {
                producer.close();
            }
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public Consumer<byte[]> getConsumer() {
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfiguration.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        consumerProps.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        consumerProps.put(ConsumerConfiguration.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DatabusConsumer<byte[]>(consumerProps, new ByteArrayDeserializer(), tierStorage  );
    }

    public Consumer<byte[]> getConsumerWOTierStorage() {
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfiguration.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        consumerProps.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        consumerProps.put(ConsumerConfiguration.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DatabusConsumer<byte[]>(consumerProps, new ByteArrayDeserializer());
    }
    public Producer<byte[]> getProducer() {
        final Map config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-id-sample");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, "150000");
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
        return new DatabusTierStorageProducer<byte[]>(config, new ByteArraySerializer(), tierStorage);
    }

    public ProducerRecord<byte[]> getProducerRecord(final String topic, final byte[] payload, String key) {
        final TierStorageMetadata tStorageMetadata =
                new TierStorageMetadata(BUCKET_NAME, topic + key);
        final RoutingData routingData = new RoutingData(topic, key, null, tStorageMetadata);
        final Headers headers = new Headers();
        final MessagePayload<byte[]> messagePayload = new MessagePayload<>(payload);
        return new ProducerRecord<>(routingData, headers, messagePayload);
    }



}
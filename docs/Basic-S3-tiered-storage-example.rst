S3 Tierered Storage Example
---------------------------

This sample demonstrates how to produce messages to Kafka topic and S3 bucket. At the same time it shows how a consumer
is able to read messages from Kafka and S3 in seamless fashion.

Benefits Tiered Storage Feature

- Reduces costs because it stores the message payload in S3 bucket and uses a Kafka message part as control and index
- It stores in Kafka topic and S3 bucket atomically.
- Exposes a new extended Producer type to differentiate from the regular one.
- Consumer is backward compatible.
- Consumer is able to read raw kafka, databus or Kakfa + S3 messages.
- Previous consumer (2.4.1 and below) won't break when reading a Tiered Storage message.


Code highlights are shown below:

Sample Code
~~~~~~~~~~~

.. code:: java

    package sample;

    import broker.ClusterHelper;
    import com.amazonaws.ClientConfiguration;
    import com.opendxl.databus.common.RecordMetadata;
    import com.opendxl.databus.common.internal.builder.TopicNameBuilder;
    import com.opendxl.databus.consumer.Consumer;
    import com.opendxl.databus.consumer.ConsumerConfiguration;
    import com.opendxl.databus.consumer.ConsumerRecord;
    import com.opendxl.databus.consumer.ConsumerRecords;
    import com.opendxl.databus.consumer.DatabusConsumer;
    import com.opendxl.databus.entities.Headers;
    import com.opendxl.databus.entities.MessagePayload;
    import com.opendxl.databus.entities.RoutingData;
    import com.opendxl.databus.entities.S3TierStorage;
    import com.opendxl.databus.entities.TierStorage;
    import com.opendxl.databus.entities.TierStorageMetadata;
    import com.opendxl.databus.producer.Callback;
    import com.opendxl.databus.producer.DatabusTierStorageProducer;
    import com.opendxl.databus.producer.Producer;
    import com.opendxl.databus.producer.ProducerConfig;
    import com.opendxl.databus.producer.ProducerRecord;
    import com.opendxl.databus.serialization.ByteArrayDeserializer;
    import com.opendxl.databus.serialization.ByteArraySerializer;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    import java.nio.charset.Charset;
    import java.time.LocalDateTime;
    import java.util.Collections;
    import java.util.HashMap;
    import java.util.Map;
    import java.util.Properties;
    import java.util.UUID;
    import java.util.concurrent.ExecutorService;
    import java.util.concurrent.Executors;
    import java.util.concurrent.TimeUnit;
    import java.util.concurrent.atomic.AtomicBoolean;


    public class BasicS3TierStorageConsumerProducerExample {

        private static final String AWS_REGION = "add-aws-region-name-here";
        private static final String S3_ACCESS_KEY = "add-your-access-key-here";
        private static final String S3_SECRET_KEY = "add-your-secret-key-here";
        private final Producer<byte[]> producer;
        private final ExecutorService executor;
        private final TierStorage tierStorage;
        private Consumer<byte[]> consumer;
        private String producerTopic = "topic1";
        private String consumerTopic = "topic1";

        private static final long PRODUCER_TIME_CADENCE_MS = 1000L;
        private static final long CONSUMER_TIME_CADENCE_MS = 1000L;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private static Logger LOG = LoggerFactory.getLogger(BasicS3TierStorageConsumerProducerExample.class);

        public BasicS3TierStorageConsumerProducerExample() {

            // Start Kafka cluster
            ClusterHelper.getInstance().addBroker(9092).zookeeperPort(2181).start();

            // Prepare a S3 Tiered Storage
            ClientConfiguration awsClientConfiguration = new ClientConfiguration();
            this.tierStorage = new S3TierStorage(AWS_REGION, awsClientConfiguration, S3_ACCESS_KEY, S3_SECRET_KEY);

            // Prepare a Producer
            this.producer = getProducer();

            // Prepare a Consumer
            this.consumer = getConsumer();

            // Subscribe to topic
            this.consumer.subscribe(Collections.singletonList(consumerTopic));

            this.executor = Executors.newFixedThreadPool(2);

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

        public Consumer<byte[]> getConsumer() {
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            consumerProps.put(ConsumerConfiguration.GROUP_ID_CONFIG, "cg1");
            consumerProps.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
            consumerProps.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
            consumerProps.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
            return new DatabusConsumer<byte[]>(consumerProps, new ByteArrayDeserializer(), tierStorage);
        }

        private Runnable getProducerTask() {
            return () -> {
                LOG.info("Producer started");
                while (!closed.get()) {

                    // Prepare a record
                    final String message = "Hello World at "+ LocalDateTime.now();

                    // user should provide the encoding
                    final byte[] payload = message.getBytes(Charset.defaultCharset());
                    final ProducerRecord<byte[]> producerRecord = getProducerRecord(producerTopic, payload);

                    // Send the record
                    // Get headers as String
                    final StringBuilder headers = new StringBuilder().append("[");
                    producerRecord.getHeaders().getAll().forEach((k, v) -> headers.append("[" + k + ":" + v + "]"));
                    headers.append("]");

                    producer.send(producerRecord, new MyCallback(producerRecord.getRoutingData().getShardingKey()));
                    LOG.info("[PRODUCER -> KAFKA][SENDING MSG] ID " + producerRecord.getRoutingData().getShardingKey() +
                            " TOPIC:" + TopicNameBuilder.getTopicName(producerTopic, null) +
                            " HEADERS:" + headers +
                            " PAYLOAD:" + message);

                    justWait(PRODUCER_TIME_CADENCE_MS);
                }
                producer.flush();
                producer.close();
                LOG.info("Producer closed");

            };
        }

        private Runnable getConsumerTask() {
            return () -> {
                try {
                    LOG.info("Consumer started");
                    while (!closed.get()) {

                        // Polling the databus
                        final ConsumerRecords<byte[]> records = consumer.poll(CONSUMER_TIME_CADENCE_MS);

                        // Iterate records
                        for (ConsumerRecord<byte[]> record : records) {

                            // Get headers as String
                            final StringBuilder headers = new StringBuilder().append("[");
                            record.getHeaders().getAll().forEach((k, v) -> headers.append("[" + k + ":" + v + "]"));
                            headers.append("]");

                            LOG.info("[CONSUMER <- KAFKA][MSG RCEIVED] ID " + record.getKey() +
                                    " TOPIC:" + record.getComposedTopic() +
                                    " KEY:" + record.getKey() +
                                    " PARTITION:" + record.getPartition() +
                                    " OFFSET:" + record.getOffset() +
                                    " TIMESTAMP:" + record.getTimestamp() +
                                    " HEADERS:" + headers +
                                    " PAYLOAD:" + new String(record.getMessagePayload().getPayload()));
                        }
                        consumer.commitAsync();
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                } finally {
                    consumer.unsubscribe();
                    try {
                        consumer.close();
                    } catch (Exception e) {
                        LOG.error(e.getMessage());
                    }
                    LOG.info("Consumer closed");

                }

            };
        }

        public ProducerRecord<byte[]> getProducerRecord(final String topic, final byte[] payload) {
            String key = String.valueOf(System.currentTimeMillis());
            TierStorageMetadata tStorageMetadata = new TierStorageMetadata("databus-poc-test", topic + key);
            RoutingData routingData = new RoutingData(topic, key, null, tStorageMetadata);
            Headers headers = new Headers();
            headers.put("k","v");
            MessagePayload<byte[]> messagePayload = new MessagePayload<>(payload);
            return new ProducerRecord<>(routingData, headers, messagePayload);
        }

        private void justWait(long time) {
            try {
                Thread.sleep(time);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private static class MyCallback implements Callback {

            private String shardingKey;

            public MyCallback(String shardingKey) {

                this.shardingKey = shardingKey;
            }

            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    LOG.warn("Error sending a record " + exception.getMessage());
                    return;
                }
                LOG.info("[PRODUCER <- KAFKA][OK MSG SENT] ID " + shardingKey +
                        " TOPIC:" + metadata.topic() +
                        " PARTITION:" + metadata.partition() +
                        " OFFSET:" + metadata.offset());
            }
        }

        synchronized private void stopExample(final ExecutorService executor) {
            try {
                closed.set(true);
                consumer.wakeup();
                ClusterHelper.getInstance().stop();
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            } finally {
                executor.shutdownNow();
            }
        }

        public void startExample() throws InterruptedException {

            Runnable consumerTask = getConsumerTask();
            Runnable producerTask = getProducerTask();

            executor.submit(consumerTask);
            executor.submit(producerTask);

            Runtime.getRuntime().addShutdownHook(
                    new Thread(
                            new Runnable() {
                                public void run() {
                                    stopExample(executor);
                                    LOG.info("Example finished");
                                }
                            }));

        }


        public static void main(String[] args) throws InterruptedException {
            LOG.info("Ctrl-C to finish");
            new BasicS3TierStorageConsumerProducerExample().startExample();
        }

    }


The first step is to create a ``DatabusTierStorageProducer`` by passing a ``S3TierStorage`` instance

.. code:: java

        // Prepares a S3 Tiered Storage
        this.tierStorage = new S3TierStorage(AWS_REGION, awsClientConfiguration, S3_ACCESS_KEY, S3_SECRET_KEY);
        ...
        public Producer<byte[]> getProducer() {
            ...
            // Creates a Tiered Storage Producer
            return new DatabusTierStorageProducer<byte[]>(config, new ByteArraySerializer(), tierStorage);
        }

Then a ``ProducerRecord`` message should be created by using ``TierStorageMetadata`` instance. The S3 bucket and
the S3 object name must be specified.

.. code:: java

        public ProducerRecord<byte[]> getProducerRecord(final String topic, final byte[] payload) {
            String key = String.valueOf(System.currentTimeMillis());
            TierStorageMetadata tStorageMetadata = new TierStorageMetadata("databus-poc-test", topic + key);
            RoutingData routingData = new RoutingData(topic, key, null, tStorageMetadata);
            ...
            MessagePayload<byte[]> messagePayload = new MessagePayload<>(payload);
            return new ProducerRecord<>(routingData, headers, messagePayload);
        }

Finally it sends the message to Kafka and S3

.. code:: java

    producer.send(producerRecord, ...);


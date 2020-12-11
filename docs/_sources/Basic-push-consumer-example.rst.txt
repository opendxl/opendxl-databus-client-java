Push Consumer Example
---------------------

This sample demonstrates how to consume messages from the
DXL Databus client by using a DatabusPushConsumer consumer in a
running Kafka cluster.

The DatabusPushConsumer allows clients to focus on receiving message from the databus and not focusing
on transport-level mechanism details such as commit and retry.

Benefits

- Eliminates rebalancing due to internal use of consumer pause and resume mechanisms while records are being processed.
- Provides ability to reprocess the same set of records automatically when failures occur.
- Push model instead of polling model. Records are read from Kafka and pushed into client's logic implementation. Continuous polling is eliminated.
- Hides complex operations such as poll, commit, pause, resume, and seek which are handled internally. 


Code highlights are shown below:

Sample Code
~~~~~~~~~~~

.. code:: java

    package sample;

    import broker.ClusterHelper;
    import com.opendxl.databus.common.RecordMetadata;
    import com.opendxl.databus.common.internal.builder.TopicNameBuilder;
    import com.opendxl.databus.consumer.*;
    import com.opendxl.databus.entities.Headers;
    import com.opendxl.databus.entities.MessagePayload;
    import com.opendxl.databus.entities.RoutingData;
    import com.opendxl.databus.producer.*;
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
    import java.util.concurrent.ExecutorService;
    import java.util.concurrent.Executors;
    import java.util.concurrent.TimeUnit;
    import java.util.concurrent.atomic.AtomicBoolean;


    public class BasicPushConsumerExample {

        private static final String PRODUCE_TOPIC = "topic1";
        private static final String CONSUME_TOPIC = "topic1";
        private DatabusPushConsumerFuture databusPushConsumerFuture;

        private static final long PRODUCER_TIME_CADENCE_MS = 1000L;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private static Logger LOG = LoggerFactory.getLogger(BasicPushConsumerExample.class);
        private ExecutorService executor;

        public BasicPushConsumerExample() {

            // Start Kafka cluster
            startKafkaCluster();

            // Start producing messages to Databus
            produceMessages();

            // Create a Push Consumer
            DatabusPushConsumerListenerStatus databusPushConsumerStatus = null;
            try(DatabusPushConsumer<byte[]> consumer =
                        new DatabusPushConsumer(getConsumerConfig(),
                                new ByteArrayDeserializer(), new MessageProcessor())) {

                // Subscribe to topic
                consumer.subscribe(Collections.singletonList(CONSUME_TOPIC));

                // Start pushing messages into MessageProcessor in an async fashion
                this.databusPushConsumerFuture = consumer.pushAsync();

                // Wait until the example is stopped
                databusPushConsumerStatus = this.databusPushConsumerFuture.get();

            } catch (Exception e) {
                LOG.error("ERROR" + e.getMessage(), e);
            } finally {
                LOG.info("Push consumer status:" + databusPushConsumerStatus.getStatus());
            }
        }

        private void startKafkaCluster() {
            ClusterHelper
                    .getInstance()
                    .addBroker(9092)
                    .zookeeperPort(2181)
                    .start();
        }

        private Properties getConsumerConfig() {
            // Start pushing messages coming from Databus
            final Properties config = new Properties();
            config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            config.put(ConsumerConfiguration.GROUP_ID_CONFIG, "consumer-group-1");
            config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "false");
            config.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id");
            return config;
        }

        /**
         * This is the implementation performed by the Databus SDK client to process messages
         */
        class MessageProcessor implements DatabusPushConsumerListener<byte[]> {

            @Override
            public DatabusPushConsumerListenerResponse onConsume(ConsumerRecords<byte[]> records) {
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

                return DatabusPushConsumerListenerResponse.CONTINUE_AND_COMMIT;
            }
        }

        private void produceMessages() {

            final Map config = new HashMap<String, Object>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-id-sample");
            config.put(ProducerConfig.LINGER_MS_CONFIG, "100");
            config.put(ProducerConfig.BATCH_SIZE_CONFIG, "150000");
            Producer<byte[]> producer =  new DatabusProducer<>(config, new ByteArraySerializer());

            Runnable produceMessagesTask = () -> {
                LOG.info("Producer started");
                while (!closed.get()) {

                    // Prepare a record
                    final String message = "Hello World at:" + LocalDateTime.now();

                    // user should provide the encoding
                    final byte[] payload = message.getBytes(Charset.defaultCharset());

                    String key = String.valueOf(System.currentTimeMillis());
                    RoutingData routingData = new RoutingData(PRODUCE_TOPIC, key, null);
                    Headers headers = null;
                    MessagePayload<byte[]> messagePayload = new MessagePayload<>(payload);
                    ProducerRecord<byte[]> producerRecord = new ProducerRecord<>(routingData, headers, messagePayload);


                    // Send the record
                    producer.send(producerRecord, new ProducerCallback(producerRecord.getRoutingData().getShardingKey()));
                    LOG.info("[PRODUCER -> KAFKA][SENDING MSG] ID " + producerRecord.getRoutingData().getShardingKey() +
                            " TOPIC:" + TopicNameBuilder.getTopicName(PRODUCE_TOPIC, null) +
                            " PAYLOAD:" + message);

                    justWait(PRODUCER_TIME_CADENCE_MS);
                }
                producer.flush();
                producer.close();
                LOG.info("Producer closed");

            };

            executor = Executors.newFixedThreadPool(1);
            executor.submit(produceMessagesTask);
        }



        private void justWait(long time) {
            try {
                Thread.sleep(time);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private static class ProducerCallback implements Callback {

            private String shardingKey;

            public ProducerCallback(String shardingKey) {

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

        synchronized private void stopExample() {
            try {
                closed.set(true);
                ClusterHelper.getInstance().stop();
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            } finally {
                executor.shutdownNow();
                LOG.info("Example finished");

            }
        }

        public void startExample() throws InterruptedException {

            Runtime.getRuntime().addShutdownHook(
                    new Thread(
                            new Runnable() {
                                public void run() {
                                    stopExample();
                                    LOG.info("Example finished");
                                }
                            }));

        }


        public static void main(String[] args) throws InterruptedException {
            LOG.info("Ctrl-C to finish");
            new BasicPushConsumerExample().startExample();
        }

    }


Steps to create a Push Consumer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Create a DatabusPushConsumer by using a specific consumer configuration and a DatabusPushConsumerListener instance
that will handle the received records. In this particular case the MessageProcessor class implements
the DatabusPushConsumerListener interface to handle messages from the Databus.

.. code:: java

    ...
    DatabusPushConsumer<byte[]> consumer =
                new DatabusPushConsumer(getConsumerConfig(),
                        new ByteArrayDeserializer(), new MessageProcessor());
    ...

2. Subscribe to a topic

.. code:: java

    consumer.subscribe(Collections.singletonList(CONSUME_TOPIC));

3. Start pushing messages into MessageProcessor in an async fashion.

.. code:: java

        databusPushConsumerFuture = consumer.pushAsync();

4. Monitor Push Consumer

.. code:: java

    ...
    // Wait indefinitely
    databusPushConsumerFuture.get();

    ...
    // Wait for a while then check the listener status
    while (true)  {
        try {
            DatabusPushConsumerStatus status =
                    this.databusPushConsumerFuture.get(100, TimeUnit.MILLISECONDS);
            // if this line is reached, means the listener has finished.
            return;
        } catch (TimeoutException e) {
            // TimeoutException means that listener is still working, so it continue the loop
            continue;
        } finally {
            LOG.info("Push consumer status:" + databusPushConsumerStatus.getStatus());
            LOG.info("Push consumer status:" + databusPushConsumerStatus.getListenerResult());
        }
    }
    ...



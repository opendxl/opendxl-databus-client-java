/*---------------------------------------------------------------------------*
* Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
*---------------------------------------------------------------------------*/

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
import org.apache.log4j.Logger;

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

public class TransactionConsumerProducerExample {

    private final Producer<byte[]> producer;
    private final ExecutorService executor;
    private Consumer<byte[]> consumer;
    private String producerTopic = "topic1";
    private String consumerTopic = "topic1";

    private static final long PRODUCER_TIME_CADENCE_MS = 1000L;
    private static final long CONSUMER_TIME_CADENCE_MS = 1000L;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private static Logger LOG = Logger.getLogger(BasicConsumerProducerExample.class);


    public TransactionConsumerProducerExample() {

        // Start Kafka cluster
        ClusterHelper
                .getInstance()
                .addBroker(9092)
                .addBroker(9093)
                .addBroker(9094)
                .zookeeperPort(2181)
                .start();

        ClusterHelper.getInstance().addTransactionalTopic(producerTopic,3,3);

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
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "producer-transactional-id-sample");
        config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "7000");
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        return new DatabusProducer<>(config, new ByteArraySerializer());
    }

    public Consumer<byte[]> getConsumer() {
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfiguration.GROUP_ID_CONFIG, "consumer-group-1");
        consumerProps.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        consumerProps.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        consumerProps.put(ConsumerConfiguration.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new DatabusConsumer<>(consumerProps, new ByteArrayDeserializer());
    }

    private Runnable getProducerTask() {
        return () -> {
            LOG.info("Producer started");
            producer.initTransactions();
            while (!closed.get()) {
                try {
                    producer.beginTransaction();

                    for (int i = 0; i < 2; i++) {
                        // Prepare a record
                        String message = "Hello World at:" + LocalDateTime.now() + "-" + i;

                        // user should provide the encoding
                        final byte[] payload = message.getBytes(Charset.defaultCharset());
                        final ProducerRecord<byte[]> producerRecord = getProducerRecord(producerTopic, payload);

                        // Send the record
                        producer.send(producerRecord);
                        LOG.info("[PRODUCER -> KAFKA][SENDING MSG] ID " + producerRecord.getRoutingData().getShardingKey() +
                                " TOPIC:" + TopicNameBuilder.getTopicName(producerTopic, null) +
                                " PAYLOAD:" + message);
                    }

                    producer.commitTransaction();
                    LOG.info("SUCCESS TRANSACTION");
                } catch (Exception e) {
                    // For all other exceptions, just abort the transaction.
                    LOG.info("ERROR " + e.getMessage());
                    producer.abortTransaction();
                }

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

                        LOG.info("[CONSUMER <- KAFKA][MSG RECEIVED] ID " + record.getKey() +
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
                consumer.close();
                LOG.info("Consumer closed");
            }
        };
    }

    public ProducerRecord<byte[]> getProducerRecord(final String topic, final byte[] payload) {
        String key = String.valueOf(System.currentTimeMillis());
        RoutingData routingData = new RoutingData(topic, key, null);
        Headers headers = null;
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
        new TransactionConsumerProducerExample().startExample();
    }
}
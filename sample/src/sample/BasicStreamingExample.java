/**
 * Copyright (c) 2019 McAfee LLC - All Rights Reserved
 */

package sample;

import broker.ClusterHelper;
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
import com.opendxl.databus.entities.internal.DatabusMessage;
import com.opendxl.databus.producer.Callback;
import com.opendxl.databus.producer.DatabusProducer;
import com.opendxl.databus.producer.Producer;
import com.opendxl.databus.producer.ProducerConfig;
import com.opendxl.databus.producer.ProducerRecord;
import com.opendxl.databus.serialization.ByteArrayDeserializer;
import com.opendxl.databus.serialization.ByteArraySerializer;
import com.opendxl.databus.serialization.SerdeDatabus;
import com.opendxl.databus.serialization.internal.DatabusKeyDeserializer;
import com.opendxl.databus.serialization.internal.DatabusKeySerializer;
import com.opendxl.databus.serialization.internal.MessageDeserializer;
import com.opendxl.databus.serialization.internal.MessageSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class BasicStreamingExample {

    private final ExecutorService executor;
    private final Producer<byte[]> producer;
    private final Consumer<byte[]> consumer;
    private KafkaStreams stream;
    private String inputTopic = "input-topic";
    private String outputTopic = "output-topic";
    private static final long PRODUCER_TIME_CADENCE_MS = 2000L;
    private static final long CONSUMER_TIME_CADENCE_MS = 500L;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private static Logger LOG = LoggerFactory.getLogger(BasicStreamingExample.class);

    public BasicStreamingExample() {

        // Start Kafka cluster
        ClusterHelper
                .getInstance()
                .addBroker(9092)
                .zookeeperPort(2181)
                .start();

        // Prepare a Producer
        this.producer = createProducer();

        // Prepare a Consumer
        this.consumer = createConsumer();
        this.consumer.subscribe(Collections.singletonList(outputTopic));

        // Prepare the stream and run stream processing
        this.stream = createStream();
        this.stream.start();

        this.executor = Executors.newFixedThreadPool(3);
    }

    public Producer<byte[]> createProducer() {
        final Map config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, "150000");
        return new DatabusProducer<>(config, new ByteArraySerializer());
    }

    public Consumer<byte[]> createConsumer() {
        final Properties config = new Properties();
        config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfiguration.GROUP_ID_CONFIG, "cg");
        config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer");
        return new DatabusConsumer<>(config, new ByteArrayDeserializer());
    }

    public KafkaStreams createStream() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        final StreamsBuilder streamBuilder  = new StreamsBuilder();
        final Topology topology = streamBuilder.build();
        topology.addSource("SOURCE_NAME",new DatabusKeyDeserializer(), new MessageDeserializer(),inputTopic);
        topology.addProcessor("PROCESSOR_NAME",() -> new DatabusMessageDistributor(),"SOURCE_NAME");

        final StoreBuilder<KeyValueStore<String, DatabusMessage>> storeBuilder =
                        Stores.keyValueStoreBuilder(
                                Stores.inMemoryKeyValueStore("keyvaluestore"),
                                Serdes.String(),
                                new SerdeDatabus());
        storeBuilder.build();
        topology.addStateStore(storeBuilder,"PROCESSOR_NAME");
        topology.addSink("SINK_NAME",outputTopic,new DatabusKeySerializer(), new MessageSerializer(), "PROCESSOR_NAME");

        LOG.info(String.valueOf(topology.describe()));
        return new KafkaStreams(topology, config);

    }

    private Runnable produceToInputTopic() {
        return () -> {
            LOG.info("Producer started");
            while (!closed.get()) {

                // Prepare a record
                final String message = "Hello World at:" + LocalDateTime.now();

                // user should provide the encoding
                final byte[] payload = message.getBytes(Charset.defaultCharset());
                final ProducerRecord<byte[]> producerRecord = buildProducerRecord(inputTopic, payload);

                // Send the record and set an async callback to get producer result
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            LOG.info("Error sending a record " + exception.getMessage());
                            return;
                        }
                        LOG.info(
                                "MSG SENT --> TOPIC:" + metadata.topic() + " PARTITION:" + metadata.partition() +
                                        " OFFSET:" + metadata.offset());

                    }
                });
                LOG.info("SEND MSG --> TOPIC:"+ TopicNameBuilder.getTopicName(inputTopic,null) + " PAYLOAD:"+ message);

                justWait(PRODUCER_TIME_CADENCE_MS);
            }
            producer.flush();
            producer.close();
            LOG.info("Producer closed");

        };
    }

    private Runnable consumeFromOutputTopic() {
        return () -> {
            try {
                LOG.info("Consumer started");
                while (!closed.get()) {

                    // Polling the databus
                    final ConsumerRecords<byte[]> records = consumer.poll(CONSUMER_TIME_CADENCE_MS);

                    // Iterate records
                    for(ConsumerRecord<byte[]> record : records ) {

                        // Get headers as String
                        final StringBuilder headers = new StringBuilder().append("[");
                        record.getHeaders().getAll().forEach((k, v) -> headers.append("[" + k + ":" + v + "]"));
                        headers.append("]");

                        LOG.info("MSG RECV <-- TOPIC:" + record.getComposedTopic()
                                + " KEY:" + record.getKey()
                                + " PARTITION:" + record.getPartition()
                                + " OFFSET:" + record.getOffset()
                                + " TIMESTAMP:" + record.getTimestamp()
                                + " HEADERS:" + headers
                                + " PAYLOAD:" + new String(record.getMessagePayload().getPayload()));
                    }
                    consumer.commitSync();
                }
            } catch (Exception e) {
            } finally {
                consumer.unsubscribe();
                try {
                    consumer.close();
                } catch (IOException e) {
                }
                LOG.info("Consumer closed");

            }

        };
    }

    public ProducerRecord<byte[]> buildProducerRecord(final String topic, final byte[] payload) {
        String key = String.valueOf(System.currentTimeMillis());
        RoutingData routingData = new RoutingData(topic, key, null);
        Headers headers = new Headers();
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

    private void stopExample(ExecutorService executor) {
        try {
            stream.close();
            closed.set(true);
            consumer.wakeup();
            ClusterHelper.getInstance().stop();
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOG.warn(e.getMessage());
        }
        finally {
            executor.shutdownNow();
        }
    }

    public void startExample() throws InterruptedException {

        executor.submit(consumeFromOutputTopic());
        executor.submit(produceToInputTopic());
        executor.submit(stateQuery());

        Runtime.getRuntime().addShutdownHook(
                new Thread(
                        new Runnable() {
                            public void run() {
                                stopExample(executor);
                                LOG.info("Example finished");
                            }
                        }));

    }

    private Runnable stateQuery() {
        return () -> {

            while(true) {
                ReadOnlyKeyValueStore<String, DatabusMessage> keyValueStore =
                        this.stream.store("keyvaluestore", QueryableStoreTypes.keyValueStore());

                KeyValueIterator<String, DatabusMessage> iter = keyValueStore.all();
                while (iter.hasNext()) {
                    KeyValue<String, DatabusMessage> entry = iter.next();
                    LOG.info(entry.key + entry.value);
                }
            }
        };
    }

    private static class DatabusMessageDistributor implements Processor<String, DatabusMessage> {

        private ProcessorContext processorContext;

        @Override
        public void init(final ProcessorContext processorContext) {

            this.processorContext = processorContext;
        }

        @Override
        public void process(final String shardingKey, final DatabusMessage inputMessage) {

            String enrichedString = new String(inputMessage.getPayload()) + " ENRICHED BY STREAM PROCESSOR";
            Headers inputMessageHeaders = inputMessage.getHeaders();
            inputMessageHeaders.put("NewKey","NewValue");

            DatabusMessage outputMessage = new DatabusMessage(inputMessageHeaders, enrichedString.getBytes());
            this.processorContext.forward(shardingKey, outputMessage);
        }

        @Override
        public void close() {

        }
    }

    public static void main(String[] args) throws InterruptedException {
        LOG.info("Ctrl-C to finish");

        new BasicStreamingExample().startExample();

    }


}

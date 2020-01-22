/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package sample;

import broker.ClusterHelper;
import com.opendxl.databus.common.TopicPartition;
import com.opendxl.databus.consumer.Consumer;
import com.opendxl.databus.consumer.ConsumerConfiguration;
import com.opendxl.databus.consumer.DatabusConsumer;
import com.opendxl.databus.producer.DatabusProducer;
import com.opendxl.databus.producer.Producer;
import com.opendxl.databus.producer.ProducerConfig;
import com.opendxl.databus.producer.ProducerRecord;
import com.opendxl.databus.consumer.metric.ConsumerMetric;
import com.opendxl.databus.consumer.metric.ConsumerMetricPerClientIdAndTopicPartitions;
import com.opendxl.databus.consumer.metric.ConsumerMetricPerClientIdAndTopics;
import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.entities.RoutingData;
import com.opendxl.databus.serialization.ByteArrayDeserializer;
import com.opendxl.databus.serialization.ByteArraySerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class ConsumerMetricsExample {

    private final Producer<byte[]> producer;
    private final ExecutorService executor;
    private final ScheduledExecutorService reportMetricsScheduler;
    private Consumer<byte[]> consumer;
    private final DecimalFormat decimalFormat;

    private static final String PRODUCER_TOPIC = "topic1";
    private static final String CONSUMER_TOPIC = "topic1";
    private static final long PRODUCER_TIME_CADENCE_MS = 0;
    private static final long CONSUMER_TIME_CADENCE_MS = 100;
    private static final long CONSUMER_POLL_TIMEOUT = 500;
    private static final long REPORT_METRICS_INITIAL_DELAY = 10000;
    private static final long REPORT_METRICS_PERIOD = 10000;
    private static final int BROKER_PORT = 9092;
    private static final int ZOOKEEPER_PORT = 2182;

    private static final String INTEGER_FORMAT_PATTERN = "###,###,###,###,###,###";

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private static Logger LOG = LoggerFactory.getLogger(ConsumerMetricsExample.class);


    public ConsumerMetricsExample() {


        // Start Kafka cluster
        ClusterHelper
                .getInstance()
                .addBroker(BROKER_PORT)
                .zookeeperPort(ZOOKEEPER_PORT)
                .start();

        // Prepare a Producer
        this.producer = getProducer();

        // Prepare a Consumer
        this.consumer = getConsumer();

        // Subscribe to topic
        this.consumer.subscribe(Collections.singletonList(CONSUMER_TOPIC));

        this.executor = Executors.newFixedThreadPool(2);

        this.reportMetricsScheduler = Executors.newScheduledThreadPool(1);

        decimalFormat = new DecimalFormat(INTEGER_FORMAT_PATTERN);


    }

    public Producer<byte[]> getProducer() {
        final Map config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-id-sample");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, "150000");
        return new DatabusProducer<>(config, new ByteArraySerializer());
    }

    public Consumer<byte[]> getConsumer() {
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfiguration.GROUP_ID_CONFIG, "consumer-group-1");
        consumerProps.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        consumerProps.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        return new DatabusConsumer<>(consumerProps, new ByteArrayDeserializer());
    }

    private Runnable getProducerTask() {
        return () -> {
            LOG.info("Producer started");
            while (!closed.get()) {

                // Prepare a record
                final String message = "Hello World at:" + LocalDateTime.now();

                // user should provide the encoding
                final byte[] payload = message.getBytes(Charset.defaultCharset());
                final ProducerRecord<byte[]> producerRecord = getProducerRecord(PRODUCER_TOPIC, payload);

                // Send the record
                producer.send(producerRecord);

                justWait(PRODUCER_TIME_CADENCE_MS);
            }
            producer.close();
            LOG.info("Producer closed");

        };
    }

    private Runnable getConsumerTask() {
        return () -> {
            try {
                LOG.info("Consumer started");
                int pollCount = 0;
                while (!closed.get()) {

                    // Polling the databus
                    consumer.poll(CONSUMER_POLL_TIMEOUT);

                    consumer.commitSync();
                    justWait(CONSUMER_TIME_CADENCE_MS);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage());
            } finally {
                consumer.unsubscribe();
                try {
                    consumer.close();
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                }
                LOG.info("Consumer closed");

            }

        };
    }


    private Runnable reportMetrics() {
        return () -> {
            try {
                ConsumerMetricPerClientIdAndTopics recordsTotalMetric = consumer.recordsTotalMetric();
                ConsumerMetricPerClientIdAndTopics bytesTotalMetric = consumer.bytesTotalMetric();
                ConsumerMetricPerClientIdAndTopics recordsPerSecMetric = consumer.recordsPerSecondAvgMetric();
                ConsumerMetricPerClientIdAndTopics bytesPerSecondAvgMetric = consumer.bytesPerSecondAvgMetric();
                ConsumerMetricPerClientIdAndTopicPartitions recordsLagPerTopicPartition
                        = consumer.recordsLagPerTopicPartition();

                LOG.info("");
                LOG.info("CONSUMER TOTAL:"
                        + decimalFormat.format(recordsTotalMetric.getValue()) + "rec "
                        + decimalFormat.format(bytesTotalMetric.getValue()) + "bytes");
                 LOG.info("CONSUMER RATE:"
                        + decimalFormat.format(recordsPerSecMetric.getValue()) + "rec "
                        + decimalFormat.format(bytesPerSecondAvgMetric.getValue()) + "bytes");

                for(Map.Entry<String, ConsumerMetric > topicMetric : recordsPerSecMetric.getTopicMetrics().entrySet()) {
                    LOG.info(" - " + topicMetric.getKey() +":"+ decimalFormat.format(topicMetric.getValue().getValue()) + "rec/sec");
                }

                for(Map.Entry<String, ConsumerMetric > topicMetric : bytesPerSecondAvgMetric.getTopicMetrics().entrySet()) {
                    LOG.info(" - " + topicMetric.getKey() +":"+ decimalFormat.format(topicMetric.getValue().getValue()) + "bytes/sec");
                }
                LOG.info("CONSUMER MAX LAG FOR ANY PARTITION:" + decimalFormat.format(consumer.recordsLagMaxMetric().getValue()) + "rec");

                Map<TopicPartition, ConsumerMetric> topicPartitionsMetrics =
                        recordsLagPerTopicPartition.getTopicPartitionsMetrics();

                for( Map.Entry<TopicPartition, ConsumerMetric> tpMetric: topicPartitionsMetrics.entrySet()) {
                    LOG.info(" - " + tpMetric.getKey().topic() +"-"+ tpMetric.getKey().partition() + " " + decimalFormat.format(tpMetric.getValue().getValue())+ "rec");
                }




            } catch (Exception e) {
                e.printStackTrace();
            } finally {
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


    synchronized private void stopExample(final ExecutorService executor) {
        try {
            closed.set(true);
            consumer.wakeup();
            ClusterHelper
                    .getInstance()
                    .stop();
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

        reportMetricsScheduler.scheduleAtFixedRate(reportMetrics(),
                REPORT_METRICS_INITIAL_DELAY,
                REPORT_METRICS_PERIOD,
                TimeUnit.MILLISECONDS);

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
        new ConsumerMetricsExample().startExample();
    }

}

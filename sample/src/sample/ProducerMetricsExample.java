/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package sample;

import broker.ClusterHelper;
import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.entities.RoutingData;
import com.opendxl.databus.producer.DatabusProducer;
import com.opendxl.databus.producer.Producer;
import com.opendxl.databus.producer.ProducerConfig;
import com.opendxl.databus.producer.ProducerRecord;
import com.opendxl.databus.producer.metric.ProducerMetric;
import com.opendxl.databus.serialization.ByteArraySerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerMetricsExample {

    /**
     * A {@link Producer} instance to use in the sample.
     */
    private final Producer<byte[]> producer;

    /**
     * Support instances to run the sample in a thread safe schema.
     */
    private final ExecutorService executor;
    private final ScheduledExecutorService reportMetricsScheduler;

    /**
     * A String which contains the topic name.
     */
    private String producerTopic = "topic1";

    /**
     * A {@link DecimalFormat} to format the data provided by the metrics.
     */
    private final DecimalFormat decimalFormat;

    /**
     * List of static fields user for Kakfa and Zookeeper configuration
     */
    private static final long PRODUCER_TIME_CADENCE_MS = 0;
    private static final long REPORT_METRICS_INITIAL_DELAY = 10000;
    private static final long REPORT_METRICS_PERIOD = 10000;
    private static final int BROKER_PORT = 9092;
    private static final int ZOOKEEPER_PORT = 2182;

    private static final String INTEGER_FORMAT_PATTERN = "###,###,###,###,###,###";

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private static Logger LOG = LoggerFactory.getLogger(ProducerMetricsExample.class);

    public ProducerMetricsExample() {
        // Start Kafka cluster
        ClusterHelper
                .getInstance()
                .addBroker(BROKER_PORT)
                .zookeeperPort(ZOOKEEPER_PORT)
                .start();

        // Prepare a Producer
        this.producer = getProducer();

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

    private Runnable getProducerTask() {
        return () -> {
            LOG.info("Producer started");
            while (!closed.get()) {

                // Prepare a record
                final String message = "Hello World at:" + LocalDateTime.now();

                // user should provide the encoding
                final byte[] payload = message.getBytes(Charset.defaultCharset());
                final ProducerRecord<byte[]> producerRecord = getProducerRecord(producerTopic, payload);

                // Send the record
                producer.send(producerRecord);

                justWait(PRODUCER_TIME_CADENCE_MS);
            }
            producer.close();
            LOG.info("Producer closed");
        };
    }

    private Runnable reportMetrics() {
        return () -> {
            try {
                LOG.info("**************************************************************************************");
                getProducerByClientIdMetrics();
                getProducerByClientIdAndTopicMetrics();
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    }

    private void getProducerByClientIdMetrics(){
        // Producer per clientId Metrics
        String clientId = (String) producer.getConfiguration().get(ProducerConfig.CLIENT_ID_CONFIG);
        ProducerMetric recordBatchSizeAvgMetric = producer.recordBatchSizeAvgMetric();
        ProducerMetric recordBatchSizeMaxMetric = producer.recordBatchSizeMaxMetric();
        ProducerMetric recordSizeMaxMetric = producer.recordSizeMaxMetric();
        ProducerMetric recordSizeAvgMetric = producer.recordSizeAvgMetric();
        ProducerMetric recordSendRateMetric = producer.recordSendRateMetric();
        ProducerMetric recordSendTotalMetric = producer.recordSendTotalMetric();

        LOG.info("");
        LOG.info("PRODUCER METRICS PER CLIENT ID");
        LOG.info("");
        LOG.info("PRODUCER METRICS PER CLIENT ID " + clientId + " TOTAL RECORDS: "
                + recordSendTotalMetric.getValue() + " rec");
        LOG.info("PRODUCER METRICS PER CLIENT ID " + clientId + " MAX BYTES BATCH SIZE: "
                + recordBatchSizeMaxMetric.getValue() + " bytes");
        LOG.info("PRODUCER METRICS PER CLIENT ID " + clientId + " MAX RECORDS SIZE: "
                + decimalFormat.format(recordSizeMaxMetric.getValue()) + " rec");
        LOG.info("PRODUCER METRICS PER CLIENT ID " + clientId + " AVG RECORDS PER SEC: "
                + decimalFormat.format(recordSendRateMetric.getValue()) + " rec/sec");
        LOG.info("PRODUCER METRICS PER CLIENT ID " + clientId + " AVG BYTES BATCH SIZE PER SEC: "
                + decimalFormat.format(recordBatchSizeAvgMetric.getValue()) + " bytes/sec");
        LOG.info("PRODUCER METRICS PER CLIENT ID " + clientId + " AVG RECORDS SIZE PER SEC: "
                + decimalFormat.format(recordSizeAvgMetric.getValue()) + " bytes/rec");
    }

    private void getProducerByClientIdAndTopicMetrics(){
        // Producer per topics and clientId Metrics
        ProducerMetric recordsTotalMetric =
                producer.recordSendTotalPerTopicMetric(producerTopic);
        ProducerMetric bytesTotalMetric =
                producer.recordByteTotalPerTopicMetric(producerTopic);
        ProducerMetric recordsPerSecMetric =
                producer.recordSendRatePerTopicMetric(producerTopic);
        ProducerMetric bytesPerSecondAvgMetric =
                producer.recordByteRatePerTopicMetric(producerTopic);

        LOG.info("");
        LOG.info("PRODUCER METRICS PER TOPIC");
        LOG.info("");
        LOG.info("PRODUCER METRICS TOPIC " + producerTopic + " TOTAL RECORDS: "
                + recordsTotalMetric.getValue() + " rec");
        LOG.info("PRODUCER METRICS TOPIC " + producerTopic + " TOTAL BYTES: "
                + bytesTotalMetric.getValue() + " bytes");
        LOG.info("PRODUCER METRICS TOPIC " + producerTopic + " AVG RECORDS PER SEC: "
                + decimalFormat.format(recordsPerSecMetric.getValue()) + " rec/sec");
        LOG.info("PRODUCER METRICS TOPIC " + producerTopic + " AVG BYTES PER SEC: "
                + bytesPerSecondAvgMetric.getValue() + " bytes/sec");
        LOG.info("");
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

        Runnable producerTask = getProducerTask();

        executor.submit(producerTask);
        reportMetricsScheduler.scheduleAtFixedRate(reportMetrics(),
                REPORT_METRICS_INITIAL_DELAY,
                REPORT_METRICS_PERIOD,
                TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(
                new Thread(
                        () -> {
                            stopExample(executor);
                            LOG.info("Example finished");
                        }));

    }

    public static void main(String[] args) throws InterruptedException {
        LOG.info("Ctrl-C to finish");
        new ProducerMetricsExample().startExample();
    }
}

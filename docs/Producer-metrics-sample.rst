Producer Metrics Example
------------------------

This sample demonstrates how to get producer metrics from the DXL
Databus client by using a producer in a running Kafka cluster. The type
of metrics that can be obtained are the following:

-  Producer metrics associated with a clientId.
-  Producer metrics associated with a clientId and a topic.

Code highlights are shown below:

Sample Code
~~~~~~~~~~~

.. code:: java

        public static void main(String[] args) throws InterruptedException {
            LOG.info("Ctrl-C to finish");
            new ProducerMetricsExample().startExample();
        }

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

        // Start the producer metrics example
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

        // Instance a thread to get Kafka metrics
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

        // Gets the metrics related to the clientId
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

        // Gets the metrics related to the clientId for a given topic
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

The first step is to instance the Kafka cluster to run the example.

The constructor method ``ProducerMetricsExample()`` is responsible for accomplishing that.

After that, the ``startExample()`` method starts running the producer to send messages,
invoking the method ``getProducerTask()`` which initiates the metrics collecting thread.

The metrics thread has two parameters which must be configured:

-  ``REPORT_METRICS_INITIAL_DELAY`` which is the time to wait prior
   collecting metrics.
-  ``REPORT_METRICS_PERIOD`` which is the interval at which to
   collect metrics from Kafka.

Once the producer and metrics threads have started, metrics are displayed.

The ``reportMetrics()`` method is responsible for collecting producer metrics from the Kafka cluster.
``reportMetrics()`` calls ``getProducerByClientIdMetrics()`` to collect producer metrics for an
associated clientId and also calls ``getProducerByClientIdAndTopicMetrics()`` to get the
producer metrics for an associated clientId with a specific topic.

The producer metrics associated to a clientId obtained are as follows:

+----------------------+-----------------------------------------+
| Metric Name          | Description                             |
+======================+=========================================+
| record-send-total    | The total number of records sent.       |
+----------------------+-----------------------------------------+
| record-send-rate     | The average number of records sent per  |
|                      | second.                                 |
+----------------------+-----------------------------------------+
| record-size-avg      | The average record size.                |
+----------------------+-----------------------------------------+
| record-size-max      | The maximum record size.                |
+----------------------+-----------------------------------------+
| record-error-total   | The total number of record sends that   |
|                      | resulted in errors.                     |
+----------------------+-----------------------------------------+
| record-error-rate    | The average per-second number of record |
|                      | sends that resulted in errors.          |
+----------------------+-----------------------------------------+
| batch-size-max       | The max number of bytes sent per        |
|                      | partition per-request.                  |
+----------------------+-----------------------------------------+
| batch-size-avg       | The average number of bytes sent per    |
|                      | partition per-request.                  |
+----------------------+-----------------------------------------+

The producer metrics associated to a clientId and a topicId are:

+----------------------+-----------------------------------------+
| Metric Name          | Description                             |
+======================+=========================================+
| record-send-total    | The total number of records sent for a  |
|                      | topic.                                  |
+----------------------+-----------------------------------------+
| record-send-rate     | The average number of records sent per  |
|                      | second for a topic.                     |
+----------------------+-----------------------------------------+
| record-error-total   | The total number of records sent that   |
|                      | resulted in errors for a topic.         |
+----------------------+-----------------------------------------+
| record-error-rate    | The average per-second number of        |
|                      | records sent that resulted in errors    |
|                      | for a topic.                            |
+----------------------+-----------------------------------------+
| byte-total           | The total number of bytes sent for a    |
|                      | topic.                                  |
+----------------------+-----------------------------------------+
| byte-rate            | The average number of bytes sent per    |
|                      | second for a topic.                     |
+----------------------+-----------------------------------------+

Further information about Kafka monitoring and metrics can be found
`here <https://kafka.apache.org/documentation/#monitoring>`__.

Run the sample
~~~~~~~~~~~~~~

Prerequisites
^^^^^^^^^^^^^

-  Java Development Kit 8 (JDK 8) or later.

Running
^^^^^^^

To run this sample execute the runsample script as follows:

::

    $ ./runsample sample.ProducerMetricsExample

The output shows:

::

    Zookeeper node started: localhost:2182
    Kafka broker started: localhost:9092
    Producer started
    **************************************************************************************

    PRODUCER METRICS PER CLIENT ID

    PRODUCER METRICS PER CLIENT ID producer-id-sample TOTAL RECORDS: 5090324.0 rec
    PRODUCER METRICS PER CLIENT ID producer-id-sample MAX BYTES BATCH SIZE: 149975.0 bytes
    PRODUCER METRICS PER CLIENT ID producer-id-sample MAX RECORDS SIZE: 143 rec
    PRODUCER METRICS PER CLIENT ID producer-id-sample AVG RECORDS PER SEC: 128.524 rec/sec
    PRODUCER METRICS PER CLIENT ID producer-id-sample AVG BYTES BATCH SIZE PER SEC: 88.554 bytes/sec
    PRODUCER METRICS PER CLIENT ID producer-id-sample AVG RECORDS SIZE PER SEC: 143 bytes/rec

    PRODUCER METRICS PER TOPIC

    PRODUCER METRICS TOPIC topic1 TOTAL RECORDS: 5090324.0 rec
    PRODUCER METRICS TOPIC topic1 TOTAL BYTES: 3.41020172E8 bytes
    PRODUCER METRICS TOPIC topic1 AVG RECORDS PER SEC: 128.521 rec/sec
    PRODUCER METRICS TOPIC topic1 AVG BYTES PER SEC: 8610098.517938748 bytes/sec

    **************************************************************************************

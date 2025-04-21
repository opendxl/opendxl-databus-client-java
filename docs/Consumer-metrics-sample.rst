Consumer Metrics Example
------------------------

This sample demonstrates collecting consumer metrics from the DXL
Databus client by using a consumer in a running Kafka cluster. The type
of metrics that can be obtained are as follows:

-  Consumer metrics associated to a clientId.
-  Consumer metrics associated to a clientId and a topic.

The purpose of this example is to show ``Consumer`` metric values varying across
time, so a ``Consumer`` instance is created and it must continuously
consume records from Kafka. In order to provide these records, the
example instantiates a ``Producer`` to continue producing records to
specific topics for the ``Consumer`` to consume from. The ``Producer``
instance has only a helper role in this example so it is not covered in
detail.

Code highlights are shown below:

Sample Code
~~~~~~~~~~~

.. code:: java

    public static void main(String[] args) throws InterruptedException {
        LOG.info("Ctrl-C to finish");
        new ConsumerMetricsExample().startExample();
    }

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

        // Subscribe Consumer to topic
        this.consumer.subscribe(Collections.singletonList(consumerTopic));

        // Set up two threads: one to produce records and another to consume them
        this.executor = Executors.newFixedThreadPool(2);

        // Set up a thread to periodically collect consumer metrics
        this.reportMetricsScheduler = Executors.newScheduledThreadPool(1);

        // Decimal format to apply to collected consumer metrics
        decimalFormat = new DecimalFormat(INTEGER_FORMAT_PATTERN);

    }

    // Create consumer instance
    public Consumer<byte[]> getConsumer() {
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfiguration.GROUP_ID_CONFIG, "consumer-group-1");
        consumerProps.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        consumerProps.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        return new DatabusConsumer<>(consumerProps, new ByteArrayDeserializer());
    }

    // Start the consumer metrics sample
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

    // Consumer loop to continuously get records from Kafka
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
                consumer.close();
                LOG.info("Consumer closed");
            }
        };
    }

    // Instance a thread to get Kafka consumer metrics
    private Runnable reportMetrics() {
        return () -> {
            try {
                ConsumerMetricPerClientIdAndTopics recordsTotalMetric
                        = consumer.recordsTotalMetric();
                ConsumerMetricPerClientIdAndTopics bytesTotalMetric
                        = consumer.bytesTotalMetric();
                ConsumerMetricPerClientIdAndTopics recordsPerSecMetric
                        = consumer.recordsPerSecondAvgMetric();
                ConsumerMetricPerClientIdAndTopics bytesPerSecondAvgMetric
                        = consumer.bytesPerSecondAvgMetric();
                ConsumerMetricPerClientIdAndTopicPartitions recordsLagPerTopicPartition
                        = consumer.recordsLagPerTopicPartition();

                LOG.info("");
                LOG.info("CONSUMER TOTAL:"
                        + decimalFormat.format(recordsTotalMetric.getValue()) + "rec "
                        + decimalFormat.format(bytesTotalMetric.getValue()) + "bytes");
                 LOG.info("CONSUMER RATE:"
                        + decimalFormat.format(recordsPerSecMetric.getValue()) + "rec "
                        + decimalFormat.format(bytesPerSecondAvgMetric.getValue()) + "bytes");

                for(Map.Entry<String, ConsumerMetric > topicMetric
                        : recordsPerSecMetric.getTopicMetrics().entrySet()) {
                    LOG.info(" - " + topicMetric.getKey()
                             + ":" + decimalFormat.format(topicMetric.getValue().getValue())
                             + "rec/sec");
                }

                for(Map.Entry<String, ConsumerMetric > topicMetric
                        : bytesPerSecondAvgMetric.getTopicMetrics().entrySet()) {
                    LOG.info(" - " + topicMetric.getKey()
                             + ":" + decimalFormat.format(topicMetric.getValue().getValue())
                             + "bytes/sec");
                }
                LOG.info("CONSUMER MAX LAG FOR ANY PARTITION:"
                         + decimalFormat.format(consumer.recordsLagMaxMetric().getValue())
                         + "rec");

                Map<TopicPartition, ConsumerMetric> topicPartitionsMetrics =
                        recordsLagPerTopicPartition.getTopicPartitionsMetrics();

                for( Map.Entry<TopicPartition, ConsumerMetric> tpMetric
                       : topicPartitionsMetrics.entrySet()) {
                    LOG.info(" - " + tpMetric.getKey().topic()
                             + "-" + tpMetric.getKey().partition()
                             + " " + decimalFormat.format(tpMetric.getValue().getValue())
                             + "rec");
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
            }
        };
    }

    // Producer loop to continuously produce records to Kafka
    private Runnable getProducerTask() {
        return () -> {
            LOG.info("Producer started");
            while (!closed.get()) {

                // Prepare a record
                final String message = "Hello World at:" + LocalDateTime.now();

                // user should provide the encoding
                final byte[] payload = message.getBytes(Charset.defaultCharset());
                final ProducerRecord<byte[]> producerRecord = getProducerRecord(producerTopic,
                                                                                payload);

                // Send the record
                producer.send(producerRecord);

                justWait(PRODUCER_TIME_CADENCE_MS);
            }
            producer.close();
            LOG.info("Producer closed");

        };
    }

The first step is to create the Kafka cluster and the ``Consumer`` and
the helper ``Producer`` with which to run the example. The constructor method
``ConsumerMetricsExample()`` is responsible for accomplishing that. It also
subscribes the ``Consumer`` to the selected topic ``CONSUMER_TOPIC``.

Second, the ``startExample()`` method creates a consumer thread by
invoking ``getConsumerTask()``. This starts the thread for the
``Consumer`` instance which continuously receives records.

Third, the ``startExample()`` method creates a producer thread
by calling ``getProducerTask()``. This starts the thread for the
``Producer`` which sends records to the topic that the ``Consumer`` is
subscribed to.

Finally, the ``startExample()`` method starts the metrics collecting thread
which will periodically call ``reportMetrics()``. The metrics thread has
two parameters which must be configured:

-  ``REPORT_METRICS_INITIAL_DELAY`` which is the time to wait prior
   collecting metrics.
-  ``REPORT_METRICS_PERIOD`` which is the interval at which to
   collect metrics from Kafka.

After the consumer, producer and metrics threads have started, the
sample periodically displays the consumer metrics.

The ``reportMetrics()`` method is responsible for collecting consumer
metrics from the Kafka cluster.

The ``Consumer`` metrics obtained by the sample are as follows:

+-------------------------+-----------------------------------------+
| Metric Name             | Description                             |
+=========================+=========================================+
| records-consumed-total  | Total number of records consumed per    |
|                         | consumer and its topics.                |
+-------------------------+-----------------------------------------+
| bytes-consumed-total    | Total bytes consumed per consumer and   |
|                         | its topics.                             |
+-------------------------+-----------------------------------------+
| records-consumed-rate   | Average number of records consumed per  |
|                         | seconds for each consumer and its       |
|                         | topics.                                 |
+-------------------------+-----------------------------------------+
| bytes-consumed-rate     | Average bytes consumed per second for   |
|                         | each consumer and its topics.           |
+-------------------------+-----------------------------------------+
| records-lag-max         | The maximum lag in terms of number of   |
|                         | records for any partition.              |
+-------------------------+-----------------------------------------+
| records-lag             | The latest lag of the partition.        |
+-------------------------+-----------------------------------------+

Further information about Kafka monitoring and metrics can be found
`here <https://kafka.apache.org/documentation/#monitoring>`__.

Finally, pressing ``CTRL+C`` shuts down the example.
The shut down steps involve:

-  stop producer thread and close ``Producer`` instance
-  stop consumer thread, unsubscribe ``Consumer`` instance from topics
   and close it
-  stop Kafka cluster

Run the sample
~~~~~~~~~~~~~~

Prerequisites
^^^^^^^^^^^^^

-  Java Development Kit 8 (JDK 8) or later.

Running
^^^^^^^

To run this sample execute the runsample script as follows:

::

    $ ./runsample sample.ConsumerMetricsExample

The output shows:

::

    Ctrl-C to finish
    Zookeeper node started: localhost:2181
    Kafka broker started: localhost:9092
    Consumer started
    Producer started

    CONSUMER TOTAL:12,220rec 822,133bytes
    CONSUMER RATE:382rec 25,698bytes
     - topic1:382rec/sec
     - topic1:25,697bytes/sec
    CONSUMER MAX LAG FOR ANY PARTITION:9,984rec
     - topic1-1 0rec
     - topic1-0 6,984rec
     - topic1-3 0rec
     - topic1-2 0rec
     - topic1-5 0rec
     - topic1-4 0rec

    CONSUMER TOTAL:49,703rec 3,343,564bytes
    CONSUMER RATE:1,184rec 79,641bytes
     - topic1:1,184rec/sec
     - topic1:79,641bytes/sec
    CONSUMER MAX LAG FOR ANY PARTITION:18,847rec
     - topic1-1 0rec
     - topic1-0 0rec
     - topic1-3 0rec
     - topic1-2 0rec
     - topic1-5 10,847rec
     - topic1-4 0rec

    CONSUMER TOTAL:94,409rec 6,351,728bytes
    CONSUMER RATE:1,816rec 122,189bytes
     - topic1:1,816rec/sec
     - topic1:122,186bytes/sec
    CONSUMER MAX LAG FOR ANY PARTITION:63,553rec
     - topic1-1 18,007rec
     - topic1-0 55,553rec
     - topic1-3 0rec
     - topic1-2 0rec
     - topic1-5 3,948rec
     - topic1-4 7,511rec

    CONSUMER TOTAL:138,297rec 9,306,623bytes
    CONSUMER RATE:4,324rec 290,987bytes
     - topic1:4,324rec/sec
     - topic1:290,987bytes/sec
    CONSUMER MAX LAG FOR ANY PARTITION:238,941rec
     - topic1-1 18,007rec
     - topic1-0 48,235rec
     - topic1-3 100,829rec
     - topic1-2 157,391rec
     - topic1-5 231,441rec
     - topic1-4 7,511rec

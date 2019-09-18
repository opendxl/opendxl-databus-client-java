Producer Consumer Example
-------------------------

This sample demonstrates how to produce and consume messages from the
DXL Databus client by using a DatabusProducer and DatabusConsumer in a
running Kafka cluster.

Code highlights are shown below:

Sample Code
~~~~~~~~~~~

.. code:: java

        public static void main(String[] args) throws InterruptedException {
            LOG.info("Ctrl-C to finish");
            new BasicConsumerProducerExample().startExample();
        }

        public BasicConsumerProducerExample() {
            // Start Kafka cluster
            ClusterHelper
                    .getInstance()
                    .addBroker(9092)
                    .zookeeperPort(2181)
                    .start();

            // Prepare a Producer
            this.producer = getProducer();

            // Prepare a Consumer
            this.consumer = getConsumer();

            // Subscribe to topic
            this.consumer.subscribe(Collections.singletonList(consumerTopic));

            this.executor = Executors.newFixedThreadPool(2);
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
                    consumer.close();
                    LOG.info("Consumer closed");
                }
            };
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
                    producer.send(producerRecord, new MyCallback(producerRecord.getRoutingData().getShardingKey()));
                    LOG.info("[PPODUCER -> KAFKA][SENDING MSG] ID " + producerRecord.getRoutingData().getShardingKey() +
                            " TOPIC:" + TopicNameBuilder.getTopicName(producerTopic, null) +
                            " PAYLOAD:" + message);

                    justWait(PRODUCER_TIME_CADENCE_MS);
                }
                producer.flush();
                producer.close();
                LOG.info("Producer closed");

            };
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

| The first step is to create the instance of the Kafka cluster to run
  the example.
| The constructor method ``ProducerMetricsExample()`` is in charge of
  doing that.
| This method also creates a DatabusConsumer instance calling to
  ``getConsumer()`` method. For producer is the same approach, calling
  to ``getProducer()`` method, to create an instance of DatabusProducer.

.. code:: java

        public Consumer<byte[]> getConsumer() {
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            consumerProps.put(ConsumerConfiguration.GROUP_ID_CONFIG, "consumer-group-1");
            consumerProps.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
            consumerProps.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
            consumerProps.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
            return new DatabusConsumer<>(consumerProps, new ByteArrayDeserializer());
        }

        public Producer<byte[]> getProducer() {
            final Map config = new HashMap<String, Object>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-id-sample");
            config.put(ProducerConfig.LINGER_MS_CONFIG, "100");
            config.put(ProducerConfig.BATCH_SIZE_CONFIG, "150000");
            return new DatabusProducer<>(config, new ByteArraySerializer());
        }

DatabusConsumer and DatabusProducer are created with configuration maps
set as parameters.

DatabusConsumer receives the following configuration:

+-------------------------------+-----------------------------------------+
| Config Parameter Name         | Description                             |
+===============================+=========================================+
| ``BOOTSTRAP_SERVERS_CONFIG``  | The Kafka broker and port to listen.    |
+-------------------------------+-----------------------------------------+
| ``GROUP_ID_CONFIG``           | The consumer group associated.          |
+-------------------------------+-----------------------------------------+
| ``ENABLE_AUTO_COMMIT_CONFIG`` | If auto-commit will be enabled or not.  |
+-------------------------------+-----------------------------------------+
| ``SESSION_TIMEOUT_MS_CONFIG`` | The heartbeat interval in ms to check   |
|                               | if the Kakfa broker is alive.           |
+-------------------------------+-----------------------------------------+
| ``CLIENT_ID_CONFIG``          | The related clientId.                   |
+-------------------------------+-----------------------------------------+

DatabusProducer receives the following configuration:

+------------------------------+-----------------------------------------+
| Config Parameter Name        | Description                             |
+==============================+=========================================+
| ``BOOTSTRAP_SERVERS_CONFIG`` | The Kafka broker and port to listen.    |
+------------------------------+-----------------------------------------+
| ``CLIENT_ID_CONFIG``         | The related clientId.                   |
+------------------------------+-----------------------------------------+
| ``LINGER_MS_CONFIG``         | The amount of time in ms to wait for    |
|                              | additional messages before sending the  |
|                              | current batch.                          |
+------------------------------+-----------------------------------------+
| ``BATCH_SIZE_CONFIG``        | the amount of memory in bytes (not      |
|                              | messages!) that will be used for each   |
|                              | batch.                                  |
+------------------------------+-----------------------------------------+

After this, the consumer subscribes to a topic in the following line:

.. code:: java

        this.consumer.subscribe(Collections.singletonList(consumerTopic));

| Then the ``BasicConsumerProducerExample()`` constructor is executed,
  the
| ``startExample()`` method is called. This method calls two internal
  methods for the producer and consumer: ``getConsumerTask()`` and
  ``getProducerTask()``. Both methods execute threads, in order to
  produce and consume messages respectively.

Here in detail both methods will be explained:

``getConsumerTask()``
~~~~~~~~~~~~~~~~~~~~~

.. code:: java

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
                        //consumer.commitSync();
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

Consumer thread runs until sample stops or an exception is triggered.
When this happens the while loop breaks. Until that, the consumer polls
the produced records.

.. code:: java

        final ConsumerRecords<byte[]> records = consumer.poll(CONSUMER_TIME_CADENCE_MS);

The ``CONSUMER_TIME_CADENCE_MS`` is the time, in ms, spent waiting in
poll if data is not available.

When the poll finished the consumer logs the data of the received
messages and calls the commit method.

.. code:: java

        consumer.commitAsync();

``commitAsync()``, commits the last offset and carry on.

When the sample stops, unsubscribe and close method of the consumer are
called. These methods do the following:

-  Unsubscribe from topics currently subscribed.
-  Close the consumer. This will close the network connections and
   sockets.

.. code:: java

        consumer.unsubscribe();
        consumer.close();

``getProducerTask()``
~~~~~~~~~~~~~~~~~~~~~

.. code:: java

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
                    producer.send(producerRecord, new MyCallback(producerRecord.getRoutingData().getShardingKey()));
                    LOG.info("[PPODUCER -> KAFKA][SENDING MSG] ID " + producerRecord.getRoutingData().getShardingKey() +
                            " TOPIC:" + TopicNameBuilder.getTopicName(producerTopic, null) +
                            " PAYLOAD:" + message);

                    justWait(PRODUCER_TIME_CADENCE_MS);
                }
                producer.flush();
                producer.close();
                LOG.info("Producer closed");

            };
        }

Producer thread runs until sample stops or an exception is triggered.
When this happens the while loop breaks. Until that, the producer sends
the produced records.

| First the producer creates a message and make it into an array of
  bytes.
| After this, a producer record is created calling to the
  ``getProducerRecord()`` method.

.. code:: java

        public ProducerRecord<byte[]> getProducerRecord(final String topic, final byte[] payload) {
            String key = String.valueOf(System.currentTimeMillis());
            RoutingData routingData = new RoutingData(topic, key, null);
            Headers headers = null;
            MessagePayload<byte[]> messagePayload = new MessagePayload<>(payload);
            return new ProducerRecord<>(routingData, headers, messagePayload);
        }

In this method the a ``ProducerRecord`` instance is created, adding to
his constructor a ``RoutingData`` object with topic and key, ``Headers``
object and a ``MessagePayload`` object with the message content.

Now, at this point the next step is send the message. To do that the
producer calls the send method.

.. code:: java

        producer.send(producerRecord, new MyCallback(producerRecord.getRoutingData().getShardingKey()));

| This method sends a producer record and associates a callback for each
  sent execution. The callback is used because send is asynchronous and
  this method will return immediately once the record has been stored in
  the buffer of records waiting to be sent. This allows sending many
  records in parallel without blocking to wait for the response after
  each one.
| Fully non-blocking usage can make use of the callback parameter to
  provide a callback that will be invoked when the request is complete.

After send method executes the ``justWait()`` method is called to wait
and produce a new record. ``PRODUCER_TIME_CADENCE_MS`` is the time in ms
that the producer waits to send a new message.

Finally when sample stops flush and close method are called.

.. code:: java

        producer.flush();
        producer.close();

Flush method method makes all buffered records immediately available to
send and blocks on the completion of the requests associated with these
records. Flush gives a convenient way to ensure all previously sent
messages have actually completed.

Close method closes producer and frees resources such as connections,
threads, and buffers associated with the producer.

Run the sample
~~~~~~~~~~~~~~

Prerequisites
^^^^^^^^^^^^^

-  Java Development Kit 8 (JDK 8) or later.

Running
^^^^^^^

To run this sample execute the runsample script as follows:

::

    $ ./runsample sample.BasicConsumerProducerExample

The output shows:

::

    Zookeeper node started: localhost:2181
    Kafka broker started: localhost:9092
    Consumer started
    Producer started
    [PPODUCER -> KAFKA][SENDING MSG] ID 1567720470608 TOPIC:topic1 PAYLOAD:Hello World at:2019-09-05T18:54:30.608
    [PRODUCER <- KAFKA][OK MSG SENT] ID 1567720470608 TOPIC:topic1 PARTITION:4 OFFSET:0
    [PPODUCER -> KAFKA][SENDING MSG] ID 1567720471866 TOPIC:topic1 PAYLOAD:Hello World at:2019-09-05T18:54:31.866
    [PRODUCER <- KAFKA][OK MSG SENT] ID 1567720471866 TOPIC:topic1 PARTITION:5 OFFSET:0
    [PPODUCER -> KAFKA][SENDING MSG] ID 1567720472871 TOPIC:topic1 PAYLOAD:Hello World at:2019-09-05T18:54:32.870
    [PRODUCER <- KAFKA][OK MSG SENT] ID 1567720472871 TOPIC:topic1 PARTITION:3 OFFSET:0
    [PPODUCER -> KAFKA][SENDING MSG] ID 1567720473871 TOPIC:topic1 PAYLOAD:Hello World at:2019-09-05T18:54:33.871
    [PRODUCER <- KAFKA][OK MSG SENT] ID 1567720473871 TOPIC:topic1 PARTITION:0 OFFSET:0
    [PPODUCER -> KAFKA][SENDING MSG] ID 1567720474876 TOPIC:topic1 PAYLOAD:Hello World at:2019-09-05T18:54:34.876
    [PRODUCER <- KAFKA][OK MSG SENT] ID 1567720474876 TOPIC:topic1 PARTITION:1 OFFSET:0
    [CONSUMER <- KAFKA][MSG RCEIVED] ID 1567720474876 TOPIC:topic1 KEY:1567720474876 PARTITION:1 OFFSET:0 TIMESTAMP:1567720474876 HEADERS:[] PAYLOAD:Hello World at:2019-09-05T18:54:34.876
    [PPODUCER -> KAFKA][SENDING MSG] ID 1567720475880 TOPIC:topic1 PAYLOAD:Hello World at:2019-09-05T18:54:35.880
    [PRODUCER <- KAFKA][OK MSG SENT] ID 1567720475880 TOPIC:topic1 PARTITION:1 OFFSET:1
    [CONSUMER <- KAFKA][MSG RCEIVED] ID 1567720475880 TOPIC:topic1 KEY:1567720475880 PARTITION:1 OFFSET:1 TIMESTAMP:1567720475880 HEADERS:[] PAYLOAD:Hello World at:2019-09-05T18:54:35.880

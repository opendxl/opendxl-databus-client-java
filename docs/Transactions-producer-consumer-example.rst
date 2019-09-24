Transactions producer consumer example
--------------------------------------

This sample demonstrates how to produce and consume messages from the
DXL Databus client by using a DatabusProducer and DatabusConsumer in a
running Kafka cluster with Transactions.

Code highlights are shown below:

Sample Code
~~~~~~~~~~~

.. code:: java

        public static void main(String[] args) throws Exception {
            LOG.info("Ctrl-C to finish");
            new TransactionConsumerProducerExample().startExample();
        }

        public TransactionConsumerProducerExample() throws Exception {

            // Start Kafka cluster
            ClusterHelper
                    .getInstance()
                    .addBroker(9092)
                    .addBroker(9093)
                    .addBroker(9094)
                    .zookeeperPort(2181)
                    .start();

            // Create a new Kafka Transactional topic
            ClusterHelper.getInstance().addNewKafkaTopic(producerTopic, TRANSACTIONAL_TOPIC_REPLICATION_FACTOR,
                    TRANSACTIONAL_TOPIC_PARTITION_NUMBER);

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

        private Runnable getProducerTask() {
            return () -> {
                LOG.info("Producer started");
                producer.initTransactions();
                while (!closed.get()) {
                    try {

                        // Start Transaction
                        producer.beginTransaction();

                        LOG.info("[TRANSACTION BEGIN]");

                        // Send Transaction messages
                        for (int i = 0; i < TRANSACTION_MESSAGES_NUMBER; i++) {
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

                        // Commit transaction
                        producer.commitTransaction();

                        LOG.info("[TRANSACTION COMMITTED SUCCESSFUL]");
                    } catch (Exception e) {
                        // In case of exceptions, just abort the transaction.
                        LOG.info("[TRANSACTION ERROR][ABORTING TRANSACTION] CAUSE " + e.getMessage());
                        producer.abortTransaction();
                    }

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
| The constructor method ``TransactionConsumerProducerExample()`` is in
  charge of doing that.
| This method also creates a DatabusConsumer instance calling to
  ``getConsumer()`` method. For producer is the same approach, calling
  to ``getProducer()`` method, to create an instance of DatabusProducer.
  Also calls the method
  ``ClusterHelper.getInstance().addNewKafkaTopic()`` to create a
  transactional topic. A transactional topic is a Kafka topic with 3
  partitions at least and replication factor of 3 at least, so it's
  necessary to create this topic a minimum of 3 running brokers. Thats
  why in the constructor we have to add 3 brokers instances.

.. code:: java

            ClusterHelper
                    .getInstance()
                    .addBroker(9092)
                    .addBroker(9093)
                    .addBroker(9094)
                    .zookeeperPort(2181)
                    .start();

Also ``getConsumer()`` and ``getProducer()`` methods has custom
configurations to enable transactions:

.. code:: java

        public Consumer<byte[]> getConsumer() {
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            consumerProps.put(ConsumerConfiguration.GROUP_ID_CONFIG, "consumer-group-1");
            consumerProps.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
            consumerProps.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
            consumerProps.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
            // Configure isolation level as read_commited in order to consume transaction messages
            consumerProps.put(ConsumerConfiguration.ISOLATION_LEVEL_CONFIG, "read_committed");
            return new DatabusConsumer<>(consumerProps, new ByteArrayDeserializer());
        }

        public Producer<byte[]> getProducer() {
            final Map config = new HashMap<String, Object>();
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            config.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-id-sample");
            config.put(ProducerConfig.LINGER_MS_CONFIG, "100");
            config.put(ProducerConfig.BATCH_SIZE_CONFIG, "150000");
            // Configure transactional Id and transaction timeout to produce transactional messages
            config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "producer-transactional-id-sample");
            config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "7000");
            config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
            return new DatabusProducer<>(config, new ByteArraySerializer());
        }

DatabusConsumer receives the following basic configuration:

+--------------------------------+-----------------------------------------+
| Config Parameter Name          | Description                             |
+================================+=========================================+
| ``BOOTSTRAP_SERVERS_CONFIG``   | The Kafka broker and port to listen.    |
+--------------------------------+-----------------------------------------+
| ``GROUP_ID_CONFIG``            | The consumer group associated.          |
+--------------------------------+-----------------------------------------+
| ``ENABLE_AUTO_COMMIT_CONFIG``  | If auto-commit will be enabled or not.  |
+--------------------------------+-----------------------------------------+
| ``SESSION_TIMEOUT_MS_CONFIG``  | The heartbeat interval in ms to check   |
|                                | if the Kafka broker is alive.           |
+--------------------------------+-----------------------------------------+
| ``CLIENT_ID_CONFIG``           | The related clientId.                   |
+--------------------------------+-----------------------------------------+

And this configuration parameter to consume transactions messages:

+-------------------------------+-----------------------------------------+
| Config Parameter Name         | Description                             |
+===============================+=========================================+
| ``ISOLATION_LEVEL_CONFIG``    | Controls how to read messages written   |
|                               | transactionally. If set to              |
|                               | ``read_committed`` ,                    |
|                               | ``consumer.poll()`` will only return    |
|                               | transactional messages which have been  |
|                               | committed.                              |
+-------------------------------+-----------------------------------------+

DatabusProducer receives the following basic configuration:

+-------------------------------+-----------------------------------------+
| Config Parameter Name         | Description                             |
+===============================+=========================================+
| ``BOOTSTRAP_SERVERS_CONFIG``  | The Kafka broker and port to listen.    |
+-------------------------------+-----------------------------------------+
| ``CLIENT_ID_CONFIG``          | The related clientId.                   |
+-------------------------------+-----------------------------------------+
| ``LINGER_MS_CONFIG``          | The amount of time in ms to wait for    |
|                               | additional messages before sending the  |
|                               | current batch.                          |
+-------------------------------+-----------------------------------------+
| ``BATCH_SIZE_CONFIG``         | the amount of memory in bytes (not      |
|                               | messages!) that will be used for each   |
|                               | batch.                                  |
+-------------------------------+-----------------------------------------+

Then add the configurations parameter to produce transactions messages:

+---------------------------------+-----------------------------------------+
| Config Parameter Name           | Description                             |
+=================================+=========================================+
| ``TRANSACTIONAL_ID_CONFIG``     | This enables reliability semantics      |
|                                 | which span multiple producer sessions   |
|                                 | since it allows the client to guarantee |
|                                 | that transactions using the same        |
|                                 | TransactionalId have been completed     |
|                                 | prior to starting any new transactions. |
+---------------------------------+-----------------------------------------+
| ``TRANSACTION_TIMEOUT_CONFIG``  | The maximum amount of time in ms that   |
|                                 | the transaction coordinator will wait   |
|                                 | for a transaction status update from    |
|                                 | the producer before proactively         |
|                                 | aborting the ongoing transaction.       |
+---------------------------------+-----------------------------------------+

After call ``getProducer()`` and ``getConsumer()`` methods, the consumer
subscribes to a topic in the following line:

.. code:: java

        this.consumer.subscribe(Collections.singletonList(consumerTopic));

| Then the ``TransactionConsumerProducerExample()`` constructor is
  executed, the
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
                producer.initTransactions();
                while (!closed.get()) {
                    try {

                        // Start Transaction
                        producer.beginTransaction();

                        LOG.info("[TRANSACTION BEGIN]");

                        // Send Transaction messages
                        for (int i = 0; i < TRANSACTION_MESSAGES_NUMBER; i++) {
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

                        // Commit transaction
                        producer.commitTransaction();

                        LOG.info("[TRANSACTION COMMITTED SUCCESSFUL]");
                    } catch (Exception e) {
                        // In case of exceptions, just abort the transaction.
                        LOG.info("[TRANSACTION ERROR][ABORTING TRANSACTION] CAUSE " + e.getMessage());
                        producer.abortTransaction();
                    }

                    justWait(PRODUCER_TIME_CADENCE_MS);
                }

                producer.flush();
                producer.close();
                LOG.info("Producer closed");
            };
        }

Producer thread runs until sample stops or an exception is triggered.
When this happens the while loop breaks. Until that, the producer sends
the produced records in a transaction.

First the producer calls the ``initTransactions()`` method to enable
transactions in the producer.

Then executes in the loop the ``beginTransactionsMethod()`` to start a
new Transaction.

| After this the producer creates batch of messages (with an associated
  producer record) to send in the transaction. The number of messages
  created for the transaction is determined by the value of the
  ``TRANSACTION_MESSAGES_NUMBER``.
| Each producer record is created calling to the ``getProducerRecord()``
  method.

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

When all messages are sent the ``commitTransaction()`` method is called.
This commits the ongoing transaction and will flush any unsent records
before actually committing the transaction.

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

| If for any reason transaction fails, the ``abortTransaction()`` method
  is called.
| Any unflushed produced messages will be aborted when this call is
  made.

Run the sample
~~~~~~~~~~~~~~

Prerequisites
^^^^^^^^^^^^^

-  Java Development Kit 8 (JDK 8) or later.

Running
^^^^^^^

To run this sample execute the runsample script as follows:

::

    $ ./runsample sample.TransactionConsumerProducerExample

The output shows:

::

    Zookeeper node started: localhost:2181
    Kafka broker started: localhost:9092
    Kafka broker started: localhost:9093
    Kafka broker started: localhost:9094
    Created topic topic1.
    Consumer started
    Producer started
    [TRANSACTION BEGIN]
    [PRODUCER -> KAFKA][SENDING MSG] ID 1569250588449 TOPIC:topic1 PAYLOAD:Hello World at:2019-09-23T11:56:28.449-0
    [PRODUCER -> KAFKA][SENDING MSG] ID 1569250588449 TOPIC:topic1 PAYLOAD:Hello World at:2019-09-23T11:56:28.449-1
    [PRODUCER -> KAFKA][SENDING MSG] ID 1569250588450 TOPIC:topic1 PAYLOAD:Hello World at:2019-09-23T11:56:28.450-2
    [PRODUCER -> KAFKA][SENDING MSG] ID 1569250588450 TOPIC:topic1 PAYLOAD:Hello World at:2019-09-23T11:56:28.450-3
    [PRODUCER -> KAFKA][SENDING MSG] ID 1569250588450 TOPIC:topic1 PAYLOAD:Hello World at:2019-09-23T11:56:28.450-4
    [TRANSACTION COMMITTED SUCCESSFUL]
    [CONSUMER <- KAFKA][MSG RECEIVED] ID 1569250588449 TOPIC:topic1 KEY:1569250588449 PARTITION:2 OFFSET:5 TIMESTAMP:1569250588449 HEADERS:[] PAYLOAD:Hello World at:2019-09-23T11:56:28.449-0
    [CONSUMER <- KAFKA][MSG RECEIVED] ID 1569250588449 TOPIC:topic1 KEY:1569250588449 PARTITION:2 OFFSET:6 TIMESTAMP:1569250588450 HEADERS:[] PAYLOAD:Hello World at:2019-09-23T11:56:28.449-1
    [CONSUMER <- KAFKA][MSG RECEIVED] ID 1569250588450 TOPIC:topic1 KEY:1569250588450 PARTITION:2 OFFSET:7 TIMESTAMP:1569250588450 HEADERS:[] PAYLOAD:Hello World at:2019-09-23T11:56:28.450-2
    [CONSUMER <- KAFKA][MSG RECEIVED] ID 1569250588450 TOPIC:topic1 KEY:1569250588450 PARTITION:2 OFFSET:8 TIMESTAMP:1569250588450 HEADERS:[] PAYLOAD:Hello World at:2019-09-23T11:56:28.450-3
    [CONSUMER <- KAFKA][MSG RECEIVED] ID 1569250588450 TOPIC:topic1 KEY:1569250588450 PARTITION:2 OFFSET:9 TIMESTAMP:1569250588450 HEADERS:[] PAYLOAD:Hello World at:2019-09-23T11:56:28.450-4

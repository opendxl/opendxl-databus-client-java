package com.opendxl.databus.consumer;

import broker.ClusterHelper;
import com.opendxl.databus.common.TopicPartition;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.producer.ProducerRecord;
import com.opendxl.databus.serialization.ByteArrayDeserializer;
import com.opendxl.databus.util.Constants;
import com.opendxl.databus.util.ConsumerHelper;
import com.opendxl.databus.util.Topic;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.extensions.PA;

import static com.opendxl.databus.util.ProducerHelper.produceTo;

@Ignore
public class DatabusPushConsumerTest {


    private ConsumerHelper consumerHelper;

    @BeforeClass
    public static void beforeClass() throws IOException {
        ClusterHelper.getInstance()
                .addBroker(Integer.valueOf(Constants.KAFKA_PORT))
                .zookeeperPort(Integer.valueOf(Constants.ZOOKEEPER_PORT))
                .start();
    }

    @AfterClass
    public static void afterClass() {
        ClusterHelper.getInstance().stop();
    }


    @Test
    public void shouldConsumeAndPushRecordsToListener() {

        // Create a topic with 3 partitions
        final String topicName = createTopic()
                .partitions(3)
                .go();

        final int numOfRecordsToProduce = 10;


        Properties config = new Properties();
        config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfiguration.GROUP_ID_CONFIG, topicName);
        config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        config.put(ConsumerConfiguration.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (DatabusPushConsumer consumer = new DatabusPushConsumer(config, new ByteArrayDeserializer(),
                new DatabusPushConsumerListener() {
                    @Override
                    public DatabusPushConsumerListenerResponse onConsume(ConsumerRecords records) {
                        Assert.assertTrue(records.count() == numOfRecordsToProduce);
                        return DatabusPushConsumerListenerResponse.STOP_AND_COMMIT;
                    }
                })) {

            consumer.subscribe(Arrays.asList(topicName));
            DatabusPushConsumerFuture databusPushConsumerFuture = consumer.pushAsync(Duration.ofMillis(2000));


            // produceWithStreamingSDK records
            Map<String, ProducerRecord<byte[]>> recordsProduced = null;

            recordsProduced = produceTo(topicName)
                    .numberOfRecords(numOfRecordsToProduce)
                    .produce()
                    .asMap();

            // Check that all records were produced successfully
            // Set max.poll.interval.ms timeout
            Assert.assertEquals(numOfRecordsToProduce, recordsProduced.size());

            DatabusPushConsumerStatus databusPushConsumerStatus = databusPushConsumerFuture.get();
            Assert.assertTrue(databusPushConsumerStatus.getException() == null);
            Assert.assertTrue(databusPushConsumerStatus.getListenerResult()
                    == DatabusPushConsumerListenerResponse.STOP_AND_COMMIT);
            Assert.assertTrue(databusPushConsumerStatus.getStatus()
                    == DatabusPushConsumerStatus.Status.STOPPED);

            Assert.assertTrue(databusPushConsumerFuture.isDone());
        } catch (InterruptedException | ExecutionException e) {
            Assert.fail();
            e.printStackTrace();
        }
    }

    @Test
    public void shouldThereBeAnExceptionInStatusWhenListenerThrowsOne() {

        // Create a topic with 3 partitions
        final String topicName = createTopic()
                .partitions(3)
                .go();

        Properties config = new Properties();
        config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfiguration.GROUP_ID_CONFIG, topicName);
        config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        config.put(ConsumerConfiguration.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfiguration.MAX_POLL_RECORDS_CONFIG, 1);


        try (DatabusPushConsumer consumer = new DatabusPushConsumer(config, new ByteArrayDeserializer(),
                new DatabusPushConsumerListener() {
                    @Override
                    public DatabusPushConsumerListenerResponse onConsume(ConsumerRecords records) {
                        int i = 0;
                        int x = 10 / i;
                        System.out.println(x);
                        return DatabusPushConsumerListenerResponse.STOP_AND_COMMIT;
                    }
                })) {

            consumer.subscribe(Arrays.asList(topicName));
            DatabusPushConsumerFuture databusPushConsumerFuture = consumer.pushAsync(Duration.ofMillis(1000));

            final int numOfRecordsToProduce = 10;

            // produceWithStreamingSDK records
            Map<String, ProducerRecord<byte[]>> recordsProduced = null;

            recordsProduced = produceTo(topicName)
                    .numberOfRecords(numOfRecordsToProduce)
                    .produce()
                    .asMap();

            // Check that all records were produced successfully
            // Set max.poll.interval.ms timeout
            Assert.assertEquals(numOfRecordsToProduce, recordsProduced.size());

            DatabusPushConsumerStatus databusPushConsumerStatus = databusPushConsumerFuture.get();
            Assert.assertTrue(databusPushConsumerFuture.isDone());
            Assert.assertTrue(  databusPushConsumerStatus.getException() != null);
            Assert.assertTrue( databusPushConsumerStatus.getException() instanceof ExecutionException );
            Assert.assertTrue( databusPushConsumerStatus.getException().getCause() instanceof ArithmeticException );
            Assert.assertTrue( databusPushConsumerStatus.getListenerResult() == null );
            Assert.assertTrue( databusPushConsumerStatus.getStatus() == DatabusPushConsumerStatus.Status.STOPPED );


        } catch (InterruptedException | ExecutionException e) {
            Assert.fail();
            e.printStackTrace();
        }

    }



    @Test
    public void shouldConsumeFromTheSamePositionWhenListenerAsksRetry() {

        long offsetToSeekForPartition0 = 4;
        long offsetToSeekForPartition1 = 3;
        long offsetToSeekForPartition2 = 7;

        // Create a topic with 3 partitions
        final String topicName = createTopic()
                .partitions(3)
                .go();

        Properties config = new Properties();
        config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfiguration.GROUP_ID_CONFIG, topicName);
        config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        config.put(ConsumerConfiguration.MAX_POLL_RECORDS_CONFIG, 5);


        TopicPartition tp0 = new  TopicPartition(topicName,0);
        TopicPartition tp1 = new  TopicPartition(topicName,1);
        TopicPartition tp2 = new  TopicPartition(topicName,2);
        Map<TopicPartition, Integer> actualPositionPerTopicPartition = new HashMap<>();
        actualPositionPerTopicPartition.put(tp0, null);
        actualPositionPerTopicPartition.put(tp1, null);
        actualPositionPerTopicPartition.put(tp2, null);

        ConsumerListenerRetry<byte[]> consumerListenerRetry = new ConsumerListenerRetry(actualPositionPerTopicPartition);

        try (DatabusPushConsumer consumer = new DatabusPushConsumer(config, new ByteArrayDeserializer(), consumerListenerRetry)) {

            consumer.subscribe(Arrays.asList(topicName));

            final int numOfRecordsToProduce = 1000;

            // produceWithStreamingSDK records
            Map<String, ProducerRecord<byte[]>> recordsProduced = null;

            recordsProduced = produceTo(topicName)
                    .numberOfRecords(numOfRecordsToProduce)
                    .produce()
                    .asMap();

            // Check that all records were produced successfully
            Assert.assertEquals(numOfRecordsToProduce, recordsProduced.size());

            consumer.pause(consumer.assignment());
            consumer.poll(0);
            consumer.resume(consumer.assignment());
            consumer.seek(tp0,offsetToSeekForPartition0);
            consumer.seek(tp1,offsetToSeekForPartition1);
            consumer.seek(tp2,offsetToSeekForPartition2);

            DatabusPushConsumerFuture databusPushConsumerFuture = consumer.pushAsync(Duration.ofMillis(1000));

            DatabusPushConsumerStatus databusPushConsumerStatus = databusPushConsumerFuture.get();
            Assert.assertTrue(databusPushConsumerFuture.isDone());


            Map<TopicPartition, Long> actualPosition = consumerListenerRetry.getPosition();
            Assert.assertTrue(actualPosition.get(tp0) == offsetToSeekForPartition0);
            Assert.assertTrue(actualPosition.get(tp1) == offsetToSeekForPartition1);
            Assert.assertTrue(actualPosition.get(tp2) == offsetToSeekForPartition2);


            Assert.assertTrue( databusPushConsumerStatus.getException() == null);
            Assert.assertTrue( databusPushConsumerStatus.getListenerResult() == DatabusPushConsumerListenerResponse.STOP_AND_COMMIT );
            Assert.assertTrue( databusPushConsumerStatus.getStatus() == DatabusPushConsumerStatus.Status.STOPPED );

        } catch (InterruptedException | ExecutionException e) {
            Assert.fail();
            e.printStackTrace();
        }

    }


    @Test
    public void shouldConsumeFromTheSamePositionFromTwoTopicsWhenListenerAsksRetry() {

        long offsetToSeekForT1Partition0 = 4;
        long offsetToSeekForT1Partition1 = 3;
        long offsetToSeekForT1Partition2 = 7;

        long offsetToSeekForT2Partition0 = 12;
        long offsetToSeekForT2Partition1 = 5;
        long offsetToSeekForT2Partition2 = 8;

        // Create a topic with 3 partitions
        final String topicName1 = createTopic()
                .partitions(3)
                .go();

        final String topicName2 = createTopic()
                .partitions(3)
                .go();

        Properties config = new Properties();
        config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfiguration.GROUP_ID_CONFIG, topicName1);
        config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        config.put(ConsumerConfiguration.MAX_POLL_RECORDS_CONFIG, 5);

        TopicPartition t1p0 = new  TopicPartition(topicName1,0);
        TopicPartition t1p1 = new  TopicPartition(topicName1,1);
        TopicPartition t1p2 = new  TopicPartition(topicName1,2);
        Map<TopicPartition, Integer> actualPositionPerTopicPartition = new HashMap<>();
        actualPositionPerTopicPartition.put(t1p0, null);
        actualPositionPerTopicPartition.put(t1p1, null);
        actualPositionPerTopicPartition.put(t1p2, null);

        TopicPartition t2p0 = new  TopicPartition(topicName2,0);
        TopicPartition t2p1 = new  TopicPartition(topicName2,1);
        TopicPartition t2p2 = new  TopicPartition(topicName2,2);
        actualPositionPerTopicPartition.put(t2p0, null);
        actualPositionPerTopicPartition.put(t2p1, null);
        actualPositionPerTopicPartition.put(t2p2, null);

        ConsumerListenerRetry<byte[]> consumerListenerRetry = new ConsumerListenerRetry(actualPositionPerTopicPartition);

        try (DatabusPushConsumer consumer = new DatabusPushConsumer(config, new ByteArrayDeserializer(), consumerListenerRetry)) {

            consumer.subscribe(Arrays.asList(topicName1,topicName2));

            final int numOfRecordsToProduce = 1000;

            // produceWithStreamingSDK records
            Map<String, ProducerRecord<byte[]>> recordsProduced1 = null;
            Map<String, ProducerRecord<byte[]>> recordsProduced2 = null;


            recordsProduced1 = produceTo(topicName1)
                    .numberOfRecords(numOfRecordsToProduce)
                    .produce()
                    .asMap();

            recordsProduced2 = produceTo(topicName2)
                    .numberOfRecords(numOfRecordsToProduce)
                    .produce()
                    .asMap();

            // Check that all records were produced successfully
            Assert.assertEquals(numOfRecordsToProduce, recordsProduced1.size());
            Assert.assertEquals(numOfRecordsToProduce, recordsProduced2.size());


            consumer.pause(consumer.assignment());
            consumer.poll(0);
            consumer.resume(consumer.assignment());
            consumer.seek(t1p0,offsetToSeekForT1Partition0);
            consumer.seek(t1p1,offsetToSeekForT1Partition1);
            consumer.seek(t1p2,offsetToSeekForT1Partition2);

            consumer.seek(t2p0,offsetToSeekForT2Partition0);
            consumer.seek(t2p1,offsetToSeekForT2Partition1);
            consumer.seek(t2p2,offsetToSeekForT2Partition2);

            DatabusPushConsumerFuture databusPushConsumerFuture = consumer.pushAsync(Duration.ofMillis(1000));

            DatabusPushConsumerStatus databusPushConsumerStatus = databusPushConsumerFuture.get();
            Assert.assertTrue(databusPushConsumerFuture.isDone());


            Map<TopicPartition, Long> actualPosition = consumerListenerRetry.getPosition();
            Assert.assertTrue(actualPosition.get(t1p0) == offsetToSeekForT1Partition0);
            Assert.assertTrue(actualPosition.get(t1p1) == offsetToSeekForT1Partition1);
            Assert.assertTrue(actualPosition.get(t1p2) == offsetToSeekForT1Partition2);
            Assert.assertTrue(actualPosition.get(t2p0) == offsetToSeekForT2Partition0);
            Assert.assertTrue(actualPosition.get(t2p1) == offsetToSeekForT2Partition1);
            Assert.assertTrue(actualPosition.get(t2p2) == offsetToSeekForT2Partition2);

            Assert.assertTrue( databusPushConsumerStatus.getException() == null);
            Assert.assertTrue( databusPushConsumerStatus.getListenerResult() == DatabusPushConsumerListenerResponse.STOP_AND_COMMIT );
            Assert.assertTrue( databusPushConsumerStatus.getStatus() == DatabusPushConsumerStatus.Status.STOPPED );

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }




    @Test
    public void shouldFailWhenCallingPushAsyncOnAClosedPushConsumer() {

        // Create a topic with 3 partitions
        final String topicName = createTopic()
                .partitions(3)
                .go();

        final int numOfRecordsToProduce = 10;


        Properties config = new Properties();
        config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfiguration.GROUP_ID_CONFIG, topicName);
        config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        config.put(ConsumerConfiguration.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (DatabusPushConsumer consumer = new DatabusPushConsumer(config, new ByteArrayDeserializer(),
                new DatabusPushConsumerListener() {
                    @Override
                    public DatabusPushConsumerListenerResponse onConsume(ConsumerRecords records) {
                        Assert.assertTrue(records.count() == numOfRecordsToProduce);
                        return DatabusPushConsumerListenerResponse.STOP_AND_COMMIT;
                    }
                })) {

            consumer.subscribe(Arrays.asList(topicName));
            DatabusPushConsumerFuture databusPushConsumerFuture = consumer.pushAsync(Duration.ofMillis(1000));


            // produceWithStreamingSDK records
            Map<String, ProducerRecord<byte[]>> recordsProduced = null;

            recordsProduced = produceTo(topicName)
                    .numberOfRecords(numOfRecordsToProduce)
                    .produce()
                    .asMap();

            // Check that all records were produced successfully
            // Set max.poll.interval.ms timeout
            Assert.assertEquals(numOfRecordsToProduce, recordsProduced.size());

            // wait until finishes.
            databusPushConsumerFuture.get();
            Assert.assertTrue(databusPushConsumerFuture.isDone());

            consumer.pushAsync(Duration.ofMillis(1000));
            Assert.fail();

        } catch (DatabusClientRuntimeException e) {
            Assert.assertTrue(e.getCausedByClass().equals(DatabusPushConsumer.class.getName()));
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail();
        } catch (ExecutionException e) {
            e.printStackTrace();
            Assert.fail();        }
    }



    @Test
    public void shouldNotFailWhenTwoOrMorePushAsynAreCalled() {

        // Create a topic with 3 partitions
        final String topicName = createTopic()
                .partitions(3)
                .go();

        final int numOfRecordsToProduce = 10;


        Properties config = new Properties();
        config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfiguration.GROUP_ID_CONFIG, topicName);
        config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        config.put(ConsumerConfiguration.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (DatabusPushConsumer consumer = new DatabusPushConsumer(config, new ByteArrayDeserializer(),
                new DatabusPushConsumerListener() {
                    @Override
                    public DatabusPushConsumerListenerResponse onConsume(ConsumerRecords records) {
                        Assert.assertTrue(records.count() == numOfRecordsToProduce);
                        return DatabusPushConsumerListenerResponse.STOP_AND_COMMIT;
                    }
                })) {

            consumer.subscribe(Arrays.asList(topicName));
            DatabusPushConsumerFuture databusPushConsumerFuture = consumer.pushAsync(Duration.ofMillis(1000));
            DatabusPushConsumerFuture databusPushConsumerFuture1 = consumer.pushAsync(Duration.ofMillis(1000));

            Assert.assertTrue(databusPushConsumerFuture.equals(databusPushConsumerFuture1));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }


    @Test
    public void shouldFailWhenCallPollAndPushAsyncWasAlreadyCalled1() {

        // Create a topic with 3 partitions
        final String topicName = createTopic()
                .partitions(3)
                .go();

        final int numOfRecordsToProduce = 10;


        Properties config = new Properties();
        config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfiguration.GROUP_ID_CONFIG, topicName);
        config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        config.put(ConsumerConfiguration.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (DatabusPushConsumer consumer = new DatabusPushConsumer(config, new ByteArrayDeserializer(),
                new DatabusPushConsumerListener() {
                    @Override
                    public DatabusPushConsumerListenerResponse onConsume(ConsumerRecords records) {
                        Assert.assertTrue(records.count() == numOfRecordsToProduce);
                        return DatabusPushConsumerListenerResponse.STOP_AND_COMMIT;
                    }
                })) {

            consumer.subscribe(Arrays.asList(topicName));
            DatabusPushConsumerFuture databusPushConsumerFuture = consumer.pushAsync(Duration.ofMillis(1000));
            consumer.poll(Duration.ofMillis(1000));

        } catch (DatabusClientRuntimeException e) {
            Assert.assertTrue(e.getCausedByClass().equals(DatabusPushConsumer.class.getName()));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }


    @Test
    public void shouldFailWhenCallPollAndPushAsyncWasAlreadyCalled2() {

        // Create a topic with 3 partitions
        final String topicName = createTopic()
                .partitions(3)
                .go();

        final int numOfRecordsToProduce = 10;


        Properties config = new Properties();
        config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfiguration.GROUP_ID_CONFIG, topicName);
        config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        config.put(ConsumerConfiguration.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (DatabusPushConsumer consumer = new DatabusPushConsumer(config, new ByteArrayDeserializer(),
                new DatabusPushConsumerListener() {
                    @Override
                    public DatabusPushConsumerListenerResponse onConsume(ConsumerRecords records) {
                        Assert.assertTrue(records.count() == numOfRecordsToProduce);
                        return DatabusPushConsumerListenerResponse.STOP_AND_COMMIT;
                    }
                })) {

            consumer.subscribe(Arrays.asList(topicName));
            DatabusPushConsumerFuture databusPushConsumerFuture = consumer.pushAsync(Duration.ofMillis(1000));
            consumer.poll(1000L);

        } catch (DatabusClientRuntimeException e) {
            Assert.assertTrue(e.getCausedByClass().equals(DatabusPushConsumer.class.getName()));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void shouldConsumeAllRecordsWhenNewTopicPartitionsAreAssignedOrRemoved() {
        // Create a topic with 6 partitions
        final int NUM_PARTITIONS = 6;
        final String topicName = createTopic()
                .partitions(NUM_PARTITIONS)
                .go();

        final int numOfRecordsToProduce = 1000000;

        // DatabusPushConsumer properties
        Properties config = new Properties();
        config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfiguration.GROUP_ID_CONFIG, topicName);
        config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        config.put(ConsumerConfiguration.CLIENT_ID_CONFIG, "consumer-id-sample");
        config.put(ConsumerConfiguration.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // List to collect all records consumed by DatabusPushConsumer
        final List<ConsumerRecord<byte[]>> pushConsumerRecords = new ArrayList<>();

        try (DatabusPushConsumer databusPushConsumer = new DatabusPushConsumer(config, new ByteArrayDeserializer(),
                new DatabusPushConsumerListener<byte[]>() {
                    @Override
                    public DatabusPushConsumerListenerResponse onConsume(ConsumerRecords<byte[]> records) {
                        for (ConsumerRecord<byte[]> consumerRecord : records) {
                            pushConsumerRecords.add(consumerRecord);
                        }

                        return DatabusPushConsumerListenerResponse.CONTINUE_AND_COMMIT;
                    }
                })) {

            databusPushConsumer.subscribe(Arrays.asList(topicName));
            // Start consuming with DatabusPushConsumer
            DatabusPushConsumerFuture databusPushConsumerFuture = databusPushConsumer.pushAsync(Duration.ofMillis(1000));

            // Task to manage the 5 DatabusConsumers in the same consumer group of DatabusPushConsumer
            // These consumers subscribe to the same topic DatabusPushConsumer does
            // They consume during 4 seconds, then they close connection, leave consumer group, stay idle
            // during 3 seconds. After that, they repeat this behavior: consume during 4 seconds and stay closed
            // during 3 seconds over and over.
            // Consequently DatabusPushConsumer consumes from only 1 TopicPartition during the 4 seconds periods and
            // it consumes from 6 TopicPartitions during the 3 seconds periods.
            // The cycle repeats until no more records are consumed.
            Callable<List<ConsumerRecord<byte[]>>> onOffConsumers = onOffConsumersInSameGroup(
                    NUM_PARTITIONS - 1,
                    config,
                    topicName,
                    3
            );
            // Start executing DatabusConsumers. This task returns the records consumed by them.
            ExecutorService executor = Executors.newFixedThreadPool(1);
            final Future<List<ConsumerRecord<byte[]>>> onOffConsumersFuture = executor.submit(onOffConsumers);

            // produceWithStreamingSDK records
            Map<String, ProducerRecord<byte[]>> recordsProduced = produceTo(topicName)
                    .numberOfRecords(numOfRecordsToProduce)
                    .produce()
                    .asMap();

            // Wait to get records consumed by DatabusConsumers
            List<ConsumerRecord<byte[]>> databusConsumerRecords = onOffConsumersFuture.get();
            shutdownAndAwaitTermination(executor);

            // Wait to stop pushDatabusConsumer
            DatabusPushConsumerStatus databusPushConsumerStatus = null;
            do {
                try {
                    databusPushConsumerStatus = databusPushConsumerFuture.get(5000L, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                }
            } while (databusPushConsumerStatus.getStatus().equals(DatabusPushConsumerStatus.Status.PROCESSING));

            // Put all consumed records together in a collection,
            // e.g.: records consumed by DatabusPushConsumer and DatabusConsumers
            List<ConsumerRecord<byte[]>> allConsumedRecords = new ArrayList<>();
            allConsumedRecords.addAll(databusConsumerRecords);
            allConsumedRecords.addAll(pushConsumerRecords);

            // Check that all records were produced successfully
            Assert.assertEquals(numOfRecordsToProduce, recordsProduced.size());
            // Check that all records were consumed successfully
            Assert.assertEquals(numOfRecordsToProduce, allConsumedRecords.size());

            // Compare produced and consumed payloads
            List<String> producedPayloads = new ArrayList<>();
            recordsProduced.values().stream().forEach(record ->
                    producedPayloads.add(new String(record.payload().getPayload(), Charset.defaultCharset())));
            List<String> consumedPayloads = new ArrayList<>();
            allConsumedRecords.stream().forEach(record ->
                    consumedPayloads.add(new String(record.getMessagePayload().getPayload(), Charset.defaultCharset())));
            Collections.sort(producedPayloads);
            Collections.sort(consumedPayloads);
            Assert.assertTrue(producedPayloads.equals(consumedPayloads));

            // Stop and close DatabusPushConsumer
            AtomicBoolean stopRequested = (AtomicBoolean) PA.getValue(databusPushConsumer, "stopRequested");
            stopRequested.set(true);
            Thread.sleep(1000);
            databusPushConsumer.close();

            // Check DatabusPushConsumer status
            Assert.assertTrue(databusPushConsumerStatus.getException() == null);
            Assert.assertTrue(databusPushConsumerStatus.getListenerResult()
                    == DatabusPushConsumerListenerResponse.CONTINUE_AND_COMMIT);
            Assert.assertTrue(databusPushConsumerStatus.getStatus()
                    == DatabusPushConsumerStatus.Status.STOPPED);
            Assert.assertTrue(databusPushConsumerFuture.isDone());

        } catch (InterruptedException | ExecutionException e) {
            Assert.fail();
            e.printStackTrace();
        }

    }


    class ConsumerListenerRetry<P> implements DatabusPushConsumerListener<P> {

        private Map<TopicPartition, Long> actualPositionPerTopicPartition;

        public ConsumerListenerRetry(final Map<TopicPartition, Long> actualPositionPerTopicPartition) {

            this.actualPositionPerTopicPartition = actualPositionPerTopicPartition;
        }

        public Map<TopicPartition, Long> getPosition() {
            return actualPositionPerTopicPartition;
        }

        @Override
        public DatabusPushConsumerListenerResponse onConsume(ConsumerRecords<P> records) {


            for (TopicPartition tp : records.partitions()) {
                if(actualPositionPerTopicPartition.get(tp) == null ) {
                    List<ConsumerRecord> recordsPerTopicPartition = records.records(tp);
                    actualPositionPerTopicPartition.put(tp, recordsPerTopicPartition.get(0).getOffset());
                }
            }

            for (Map.Entry<TopicPartition, Long> entry : actualPositionPerTopicPartition.entrySet()) {
                if(entry.getValue() == null) {
                    return DatabusPushConsumerListenerResponse.RETRY;
                }
            }
            return DatabusPushConsumerListenerResponse.STOP_AND_COMMIT;

        }
    }

    class ConsumerListener implements DatabusPushConsumerListener {

        @Override
        public DatabusPushConsumerListenerResponse onConsume(ConsumerRecords records) {

            return DatabusPushConsumerListenerResponse.CONTINUE_AND_COMMIT;

        }
    }



    private Topic.Builder createTopic() {
        return new Topic.Builder();
    }

    /**
     * Periodically instantiate a given number of DatabusConsumers belonging to the given consumer group
     *
     * @param numberOfConsumers number of consumers to instantiate
     * @param config configuration to apply to all consumers - the method only changes the consumer-id to be unique
     * @param topicName topic which consumers will subscribe to
     * @param exitAfterPollsWithoutRecords this is the exit criterion from this method: it will exit when the consumers
     *                                     did not consume any records within the specified number of polls
     * @return records consumed by DatabusConsumers
     */
    private Callable<List<ConsumerRecord<byte[]>>> onOffConsumersInSameGroup(
            int numberOfConsumers, Properties config, String topicName, int exitAfterPollsWithoutRecords) {

        Callable<List<ConsumerRecord<byte[]>>> listCallable = () -> {
            // consumedRecords list contains all records consumed by all DatabusConsumer threads spawn in this method
            // (which are run by a brand new ExecutorService instantiated on each for loop iteration)
            List<ConsumerRecord<byte[]>> consumedRecords = Collections.synchronizedList(new ArrayList<>());

            // count the number of consecutive polls that get no records
            // if they reach the given 'exitAfterPollsWithoutRecords' value, then exit this method
            int emptyPollsCounter = 0;

            // exit condition: no record has to be consumed in last 'exitAfterPollsWithoutRecords' consecutive loops
            while(emptyPollsCounter < exitAfterPollsWithoutRecords) {
                // Launch many consumers so DatabusPushConsumer assigned TopicPartitions will reduce to only one
                // When continuePolling is set to false, then consumers will stop consuming
                final AtomicBoolean continuePolling = new AtomicBoolean(true);

                // Each onOffConsumer() task starts a DatabusConsumer which returns the List of its consumed records
                List<Callable<List<ConsumerRecord<byte[]>>>> onOffConsumerTasks = new ArrayList<>();
                // Set up the onOffConsumerTasks
                for (int j = 0; j < numberOfConsumers; ++j) {
                    final String consumerId = "consumer-" + j;
                    onOffConsumerTasks.add(onOffConsumer(consumerId, config, topicName, continuePolling));
                }

                // Start executing the onOffConsumerTasks
                ExecutorService executor = Executors.newFixedThreadPool(numberOfConsumers + 1);
                // letConsumersRunThenStopThenWait task controls that DatabusConsumers will consume for just 4 seconds
                // and after that they will disconnect for 3 seconds
                Future<AtomicBoolean> continuePollingLastValue = executor.submit(
                        letConsumersRunThenStopThenWait(4000L, 3000L, continuePolling));
                // Get the records consumed by DatabusConsumers
                List<Future<List<ConsumerRecord<byte[]>>>> oneLoopConsumedRecords = executor.invokeAll(onOffConsumerTasks);

                int previouslyConsumedRecords = consumedRecords.size();
                // save records consumed in this loop
                for (Future<List<ConsumerRecord<byte[]>>> someRecords : oneLoopConsumedRecords) {
                    consumedRecords.addAll(someRecords.get());
                }

                // shutdown executor service
                shutdownAndAwaitTermination(executor);

                // update counter used in while condition
                if (previouslyConsumedRecords == consumedRecords.size()) {
                    // there are no new records, so increment exit condition counter
                    emptyPollsCounter++;
                } else {
                    // there are new consumed records, so reset exit condition counter
                    emptyPollsCounter = 0;
                }

            }

            return consumedRecords;
        };

        return listCallable;
    }

    /**
     * Task to control the 'continuePolling' flag which controls DatabusConsumer polls
     *
     * @param runPeriod number of milliseconds DatabusConsumers will poll
     * @param stopPeriod number of milliseconds DatabusConsumers will NOT poll
     * @param continuePolling flog to set to true/false depending on DatabusConsumers shall poll or not
     * @return continuePolling status
     */
    private Callable<AtomicBoolean> letConsumersRunThenStopThenWait(final long runPeriod, final long stopPeriod,
                                                                    final AtomicBoolean continuePolling) {
        Callable<AtomicBoolean> callable = () -> {
            // Wait run period
            Thread.sleep(runPeriod);
            // Tell onOffConsumers to stop
            continuePolling.set(false);
            // Wait stop period
            Thread.sleep(stopPeriod);

            return continuePolling;
        };

        return callable;
    }
    
    /**
     * Start a DatabusConsumer to consume records
     *
     * @param consumerId the consumer id
     * @param config the consumer configuration properties
     * @param topicName the topic the consumer will consume from
     * @param continuePolling flag to tell consumer when to poll records:
     *                        true: continue polling
     *                        false: stop polling and return consumed records
     * @return records consumed
     */
    private Callable<List<ConsumerRecord<byte[]>>> onOffConsumer(final String consumerId, final Properties config,
                                                                 final String topicName, final AtomicBoolean continuePolling) {

        // Set up a task so consumer will consume in a thread
        Callable<List<ConsumerRecord<byte[]>>> onOffConsumerCallable = () -> {
            // Set consumer id
            config.put(ConsumerConfiguration.CLIENT_ID_CONFIG, consumerId);

            DatabusConsumer consumer = null;
            // Set up collection to collect all consumed records
            List<ConsumerRecord<byte[]>> oneConsumerConsumedRecords = new ArrayList<>();
            try {
                // Create consumer
                consumer = new DatabusConsumer<>(config, new ByteArrayDeserializer());
                // Subscribe consumer
                consumer.subscribe(Arrays.asList(topicName));
                // Start polling
                while (continuePolling.get()) {
                    final ConsumerRecords<byte[]> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<byte[]> record : records) {
                        oneConsumerConsumedRecords.add(record);
                    }
                    consumer.commitSync();
                }
            } finally {
                consumer.close();
                return oneConsumerConsumedRecords;
            }
        };

        return onOffConsumerCallable;
    }

    /**
     * Shutdown an executor service and its running tasks
     *
     * @param pool executor service to be shutdown
     */
    void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                pool.shutdownNow();
                if (!pool.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

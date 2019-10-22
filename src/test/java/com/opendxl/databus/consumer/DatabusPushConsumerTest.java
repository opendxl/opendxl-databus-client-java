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
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.opendxl.databus.util.ProducerHelper.produceTo;

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
    public void shouldConsumeAndPushRecordsToListsner() {

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

            while(!databusPushConsumerFuture.isDone()) {
                Thread.sleep(500);
            }
            DatabusPushConsumerListenerStatus databusPushConsumerListenerStatus = databusPushConsumerFuture.get();
            Assert.assertTrue(databusPushConsumerListenerStatus.getException() == null);
            Assert.assertTrue(databusPushConsumerListenerStatus.getListenerResult()
                    == DatabusPushConsumerListenerResponse.STOP_AND_COMMIT);
            Assert.assertTrue(databusPushConsumerListenerStatus.getStatus()
                    == DatabusPushConsumerListenerStatus.Status.STOPPED);

            Assert.assertTrue(databusPushConsumerFuture.isDone());
        } catch (IOException | InterruptedException | ExecutionException e) {
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

            while(!databusPushConsumerFuture.isDone()) {
                Thread.sleep(500);
            }

            DatabusPushConsumerListenerStatus databusPushConsumerListenerStatus = databusPushConsumerFuture.get();
            Assert.assertTrue(  databusPushConsumerListenerStatus.getException() != null);
            Assert.assertTrue( databusPushConsumerListenerStatus.getException() instanceof ExecutionException );
            Assert.assertTrue( databusPushConsumerListenerStatus.getException().getCause() instanceof ArithmeticException );
            Assert.assertTrue( databusPushConsumerListenerStatus.getListenerResult() == null );
            Assert.assertTrue( databusPushConsumerListenerStatus.getStatus() == DatabusPushConsumerListenerStatus.Status.STOPPED );

        } catch (IOException | InterruptedException | ExecutionException e) {
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

            while(!databusPushConsumerFuture.isDone()) {
                Thread.sleep(500);
            }

            Map<TopicPartition, Long> actualPosition = consumerListenerRetry.getPosition();
            Assert.assertTrue(actualPosition.get(tp0) == offsetToSeekForPartition0);
            Assert.assertTrue(actualPosition.get(tp1) == offsetToSeekForPartition1);
            Assert.assertTrue(actualPosition.get(tp2) == offsetToSeekForPartition2);

            DatabusPushConsumerListenerStatus databusPushConsumerListenerStatus = databusPushConsumerFuture.get();
            Assert.assertTrue( databusPushConsumerListenerStatus.getException() == null);
            Assert.assertTrue( databusPushConsumerListenerStatus.getListenerResult() == DatabusPushConsumerListenerResponse.STOP_AND_COMMIT );
            Assert.assertTrue( databusPushConsumerListenerStatus.getStatus() == DatabusPushConsumerListenerStatus.Status.STOPPED );

        } catch (IOException | InterruptedException | ExecutionException e) {
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

            while(!databusPushConsumerFuture.isDone()) {
                Thread.sleep(500);
            }

            Map<TopicPartition, Long> actualPosition = consumerListenerRetry.getPosition();
            Assert.assertTrue(actualPosition.get(t1p0) == offsetToSeekForT1Partition0);
            Assert.assertTrue(actualPosition.get(t1p1) == offsetToSeekForT1Partition1);
            Assert.assertTrue(actualPosition.get(t1p2) == offsetToSeekForT1Partition2);
            Assert.assertTrue(actualPosition.get(t2p0) == offsetToSeekForT2Partition0);
            Assert.assertTrue(actualPosition.get(t2p1) == offsetToSeekForT2Partition1);
            Assert.assertTrue(actualPosition.get(t2p2) == offsetToSeekForT2Partition2);

            DatabusPushConsumerListenerStatus databusPushConsumerListenerStatus = databusPushConsumerFuture.get();
            Assert.assertTrue( databusPushConsumerListenerStatus.getException() == null);
            Assert.assertTrue( databusPushConsumerListenerStatus.getListenerResult() == DatabusPushConsumerListenerResponse.STOP_AND_COMMIT );
            Assert.assertTrue( databusPushConsumerListenerStatus.getStatus() == DatabusPushConsumerListenerStatus.Status.STOPPED );

        } catch (IOException | InterruptedException | ExecutionException e) {
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

            while(!databusPushConsumerFuture.isDone()) {
                Thread.sleep(500);
            }

            consumer.pushAsync(Duration.ofMillis(1000));
            Assert.fail();

        } catch (DatabusClientRuntimeException e) {
            Assert.assertTrue(e.getCausedByClass().equals(DatabusPushConsumer.class.getName()));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }



    @Test
    public void test6() {

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
    public void test7() {

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
    public void test8() {

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
}

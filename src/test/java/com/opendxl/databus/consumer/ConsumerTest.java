/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

import broker.ClusterHelper;
import com.opendxl.databus.common.TopicPartition;
import com.opendxl.databus.util.Constants;
import com.opendxl.databus.util.ConsumerHelper;
import com.opendxl.databus.util.ProducerHelper;
import com.opendxl.databus.util.Topic;
import com.opendxl.databus.exception.DatabusClientRuntimeException;
import com.opendxl.databus.producer.ProducerRecord;
import com.opendxl.databus.serialization.ByteArrayDeserializer;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ConsumerTest {

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

    //////// S U B S C R I P T I O N   S E C T I O N

    @Test
    public void shouldSubscribeToTenantGroupTopics() {
        try {
            final String topic1 = "topic1";
            final String topic2 = "topic2";
            final String tenantGroup = "group0";

            final List<String> topics = Arrays.asList(topic1, topic2);
            final Map<String, List<String>> groupTopics = new HashMap<>();
            groupTopics.put(tenantGroup, topics);
            Consumer<byte[]> consumer = new DatabusConsumer(getProperties(), new ByteArrayDeserializer());
            consumer.subscribe(groupTopics);
            Set<String> subscription = consumer.subscription();

            Assert.assertTrue(subscription.size() == 2);
            Assert.assertTrue(subscription.contains(topic1.concat("-").concat(tenantGroup)));
            Assert.assertTrue(subscription.contains(topic2.concat("-").concat(tenantGroup)));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void shouldSubscribeToTenantGroupTopicsWithRebalanceListener() {
        try {
            final String topic1 = "topic1";
            final String topic2 = "topic2";
            final String tenantGroup = "group0";
            TestConsumerRebalanceListener listener = new TestConsumerRebalanceListener();

            final List<String> topics = Arrays.asList(topic1, topic2);
            final Map<String, List<String>> groupTopics = new HashMap<>();
            groupTopics.put(tenantGroup, topics);
            Consumer<byte[]> consumer = new DatabusConsumer(getProperties(), new ByteArrayDeserializer());
            consumer.subscribe(groupTopics, listener);
            Set<String> subscription = consumer.subscription();

            Assert.assertTrue(subscription.size() == 2);
            Assert.assertTrue(subscription.contains(topic1.concat("-").concat(tenantGroup)));
            Assert.assertTrue(subscription.contains(topic2.concat("-").concat(tenantGroup)));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void shouldConsumeRecordsSuccessfully() {
        try {

            final int numOfRecordsForTest = 10;

            // Create a topic with 3 partitions
            final String topicName = createTopic()
                    .partitions(3)
                    .go();

            // produceWithStreamingSDK records
            Map<String, ProducerRecord<byte[]>> recordsProduced = produceTo(topicName)
                    .numberOfRecords(numOfRecordsForTest)
                    .produce()
                    .asMap();


            // Check that all records were produced successfully
            Assert.assertEquals(numOfRecordsForTest, recordsProduced.size());

            // consume records
            List<ConsumerRecord<byte[]>> recordsConsumed = consumeFrom(topicName)
                    .consumerGroup(topicName) // We use topic name for the consumer group name
                    .numberOfRecords(numOfRecordsForTest)
                    .config(new HashMap<String, Object>() {{
                        put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, false);
                        put(ConsumerConfiguration.AUTO_OFFSET_RESET_CONFIG, "earliest");
                    }})
                    .consume()
                    .close()
                    .asList();

            // Check that all records were consumed successfully
            Assert.assertEquals(numOfRecordsForTest, recordsConsumed.size());
            for (ConsumerRecord<byte[]> recordConsumed : recordsConsumed) {
                ProducerRecord<byte[]> producerRecord = recordsProduced.get(recordConsumed.getKey());
                Assert.assertEquals(producerRecord.getRoutingData().getTopic(), recordConsumed.getTopic());
                Assert.assertTrue(Arrays.equals(producerRecord.payload().getPayload(),
                        recordConsumed.getMessagePayload().getPayload()));
            }

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    //////// C O N S U M E R   O F F S E T S   S E E K   P O S I T I O N

    @Test
    public void shouldSeekAOffsetAndConsumeFromThere() {
        try {

            final int numOfRecordsToProduce = 100;
            final int initialPartition = 0;
            final long initialOffset = 10;
            final long numOfRecordsToConsume = numOfRecordsToProduce - initialOffset;

            // Create a topic with 3 partitions
            final String topicName = createTopic()
                    .partitions(3)
                    .go();

            // produceWithStreamingSDK 100 records
            Map<String, ProducerRecord<byte[]>> recordsProduced = produceTo(topicName)
                    .numberOfRecords(numOfRecordsToProduce)
                    .produce()
                    .asMap();

            // Check that all records were produced successfully
            Assert.assertEquals(numOfRecordsToProduce, recordsProduced.size());


            // Consume records. The consumer will Partition initialPartition will start from offset initialOffset
            List<ConsumerRecord<byte[]>> recordsConsumed = consumeFrom(topicName, initialPartition, initialOffset)
                    .consumerGroup(topicName) // We use topic name for the consumer group name
                    .numberOfRecords((int)numOfRecordsToConsume)
                    .config(new HashMap<String, Object>() {{
                        put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, false);
                        put(ConsumerConfiguration.AUTO_OFFSET_RESET_CONFIG, "earliest");
                    }})
                    .consume()
                    .close()
                    .asList();

            // Check that the initialOffset is the first records from initialPartition
            boolean isOffsetFound = false;
            for (ConsumerRecord<byte[]> recordConsumed : recordsConsumed) {
                if(recordConsumed.getPartition() == 0) {
                    Assert.assertEquals(initialOffset , recordConsumed.getOffset());
                    isOffsetFound = true;
                    break;
                }
            }
            Assert.assertTrue(isOffsetFound);

            Assert.assertTrue(numOfRecordsToConsume == (long)recordsConsumed.size());


        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

    }

    //////// C O N S U M E R   C O N F I G U R A T I O N

    @Test
    /**
     * max.poll.records
     */
    public void shouldConsumeAsRecordsAsMaxPollRecordConfig() {

        try {
            final int numOfRecordsToProduce = 100;
            final int numOfRecordsToConsumed = 1;

            // Create a topic
            final String topicName = createTopic()
                    .partitions(1)
                    .go();

            // produceWithStreamingSDK 100 records
            Map<String, ProducerRecord<byte[]>> recordsProduced = produceTo(topicName)
                    .numberOfRecords(numOfRecordsToProduce)
                    .produce()
                    .asMap();

            // Check that all records were produced successfully
            Assert.assertEquals(numOfRecordsToProduce, recordsProduced.size());

            // Consume 1 record because of max.poll.records = 1
            List<ConsumerRecord<byte[]>> recordsConsumed = consumeFrom(topicName)
                    .config(new HashMap<String, Object>() {{
                        put(ConsumerConfiguration.MAX_POLL_RECORDS_CONFIG, numOfRecordsToConsumed); // Read 1 record
                        put(ConsumerConfiguration.AUTO_OFFSET_RESET_CONFIG, "earliest");
                    }})
                    .createAndSubscribe()
                    .poll(6000)
                    .commit()
                    .asList();

            // Check that the record was read from partition 0 offset 0
            Assert.assertEquals(numOfRecordsToConsumed, recordsConsumed.size());
            ConsumerRecord<byte[]> record = recordsConsumed.get(0);
            Assert.assertEquals(0,record.getPartition());
            Assert.assertEquals(0L,record.getOffset());

            // Consume next 1 record
            recordsConsumed = getConsumerHelper()
                    .poll(6000)
                    .commit()
                    .close()
                    .asList();

            // Check that the record was read from partition 0 offset 1 (next record)
            record = recordsConsumed.get(0);
            Assert.assertEquals(numOfRecordsToConsumed, recordsConsumed.size());
            Assert.assertEquals(0,record.getPartition());
            Assert.assertEquals(1L,record.getOffset());

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }



    //////// R E B A L A N C I N G


    @Test
    public void ShouldRebalanceBecauseMaxPollIntervalTimeout() {
        try {

            final int numOfRecordsToProduce = 10;

            // Create a topic with 3 partitions
            final String topicName = createTopic()
                    .partitions(1)
                    .go();

            // produceWithStreamingSDK records
            Map<String, ProducerRecord<byte[]>> recordsProduced = produceTo(topicName)
                    .numberOfRecords(numOfRecordsToProduce)
                    .produce()
                    .asMap();

            // Check that all records were produced successfully
            // Set max.poll.interval.ms timeout
            Assert.assertEquals(numOfRecordsToProduce, recordsProduced.size());

            // Create a consumer and make a poll
            ConsumerHelper consumerHelper = consumeFrom(topicName)
                    .config(new HashMap<String, Object>() {{
                        put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, false);
                        put(ConsumerConfiguration.MAX_POLL_INTERVAL_MS_CONFIG, 1000);
                    }})
                    .createAndSubscribe()
                    .poll(0);


            // Wait more time than max.poll.interval.ms setting to force a consumer group rebalancing
            sleepFor(3000);

            // Try Commit but it should fail because of rebalancing
            try {
                consumerHelper.commit();
            } catch (DatabusClientRuntimeException e) {
                Assert.assertTrue(e.getCause() instanceof CommitFailedException);
            } finally {
                consumerHelper.close();
            }

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void ShouldNotRebalanceBecauseConsumerIsPaused() {
        try {

            ExecutorService executor = Executors.newFixedThreadPool(1);
            final int numOfRecordsToProduce = 10;

            // Create a topic with 3 partitions
            final String topicName = createTopic()
                    .partitions(1)
                    .go();

            // produceWithStreamingSDK records
            Map<String, ProducerRecord<byte[]>> recordsProduced = produceTo(topicName)
                    .numberOfRecords(numOfRecordsToProduce)
                    .produce()
                    .asMap();

            // Check that all records were produced successfully
            // Set max.poll.interval.ms timeout
            Assert.assertEquals(numOfRecordsToProduce, recordsProduced.size());

            // Create a consumer and make a poll
            ConsumerHelper consumerHelper = consumeFrom(topicName)
                    .config(new HashMap<String, Object>() {{
                        put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, false);
                        put(ConsumerConfiguration.MAX_POLL_INTERVAL_MS_CONFIG, 1000);
                    }})
                    .createAndSubscribe()
                    .poll(0);

            consumerHelper.pause();

            Callable<Boolean> backgroundTask = () -> {
                sleepFor(4000);
                return true;
            };
            Future<Boolean> future = executor.submit(backgroundTask);

            while(!future.isDone()) {
                List<ConsumerRecord<byte[]>> consumerRecords = consumerHelper.poll(0).asList();
            }
            executor.shutdownNow();

            consumerHelper.resume();
            consumerHelper.commit();
            consumerHelper.commited();


            //Assert.assertTrue();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }




    // TODO: UT with SSL certificates, SSL + SCRAM
    //////// F A C I L I T I E S

    private Properties getProperties() {
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT));
        consumerProps.put(ConsumerConfiguration.GROUP_ID_CONFIG, "cg1");
        consumerProps.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfiguration.SESSION_TIMEOUT_MS_CONFIG, "30000");
        return consumerProps;
    }

    public ConsumerHelper getConsumerHelper() {
        return consumerHelper;
    }

    public void setConsumerHelper(final ConsumerHelper consumerHelper) {
        this.consumerHelper = consumerHelper;
    }

    public class TestConsumerRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("revoked " + partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("assigned " + partitions);
        }
    }

    public ProducerHelper produceTo(final String topicName) {
        return ProducerHelper.produceTo(topicName);
    }


    private Topic.Builder createTopic() {
        return new Topic.Builder();
    }

    private ConsumerHelper consumeFrom(final String topicName, final int parition, long offset) {
        ConsumerHelper consumerHelper = ConsumerHelper.consumeFrom(topicName, parition, offset);
        setConsumerHelper(consumerHelper);
        return consumerHelper;
    }

    private ConsumerHelper consumeFrom(final String topicName) {
        ConsumerHelper consumerHelper = ConsumerHelper.consumeFrom(topicName);
        setConsumerHelper(consumerHelper);
        return consumerHelper;
    }

    private void sleepFor(final int time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli;

import broker.ClusterHelper;
import com.opendxl.databus.cli.entity.ConsumerRecordResult;
import com.opendxl.databus.cli.entity.ExecutionResult;
import com.opendxl.databus.producer.ProducerRecord;
import com.opendxl.databus.util.Constants;
import com.opendxl.databus.util.ProducerHelper;
import com.opendxl.databus.util.Topic;
import org.junit.*;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import java.io.IOException;
import java.util.List;
import java.util.Map;
@Ignore
public class CommandLineInterfaceTest {

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @BeforeClass
    public static void startCluster() throws IOException {
        ClusterHelper.getInstance()
                .addBroker(Integer.valueOf(Constants.KAFKA_PORT))
                .zookeeperPort(Integer.valueOf(Constants.ZOOKEEPER_PORT))
                .start();
    }

    @AfterClass
    public static void stopCluster() {
        ClusterHelper.getInstance().stop();
    }


    @Test
    public void shouldProduceSuccessfullyWithAllArguments() {
        try {
            // Setup CLI parameters
            String args = "--operation produce"
                    + " --brokers " + Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT)
                    + " --to-topic topic1"
                    + " --sharding-key a123" // optional
                    + " --msg Hello_World!"
                    + " --config linger.ms=1000,batch.size=100000,compression.type=lz4" //optional
                    + " --headers correlationId=1234,clientId=56567" //optional
                    + " --tenant-group group0" //optional
                    + " --partition 0"; //optional


            // Test
            CommandLineInterface cli = new CommandLineInterface(args.split(" "));
            ExecutionResult executionResult = cli.execute();
            Assert.assertTrue(executionResult.getCode().equals("OK"));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void shouldProduceSuccessfullyOnlyWithMandatoryArguments() {
        try {
            // Setup CLI parameters
            String args = "--operation produce"
                    + " --brokers " + Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT)
                    + " --to-topic topic1"
                    + " --msg Hello_World!";

            // Test
            CommandLineInterface cli = new CommandLineInterface(args.split(" "));
            ExecutionResult executionResult = cli.execute();
            Assert.assertTrue(executionResult.getCode().equals("OK"));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void shouldFailWhenClusterIsDown() throws IOException {
        try {

            stopCluster();

            // Setup CLI parameters
            String args = "--operation produce"
                    + " --brokers " + Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT)
                    + " --to-topic topic1"
                    + " --msg Hello_World!";

            // Test
            CommandLineInterface cli = new CommandLineInterface(args.split(" "));
            ExecutionResult executionResult = cli.execute();
            Assert.assertTrue(executionResult.getCode().equals("ERROR"));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            startCluster();
        }
    }


    @Test
    public void shouldFailWhenThereisNoOptions() {
        exit.expectSystemExit();
        String[] args = new String[0];
        CommandLineInterface.main(args);
    }

    @Test
    public void shouldFailWhenOperationOptionIsMissing() {
        exit.expectSystemExit();
        // Setup CLI parameters
        String args = " --brokers " + Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT)
                + " --topics topic1"
                + " --msg Hello_World!";
        CommandLineInterface.main(args.split(" "));
    }

    @Test
    public void shouldFailWhenOperationIsUnknown() {
        exit.expectSystemExit();
        String args = "--operation unknown";
        CommandLineInterface.main(args.split(" "));
    }

    @Test
    public void shouldFailWhenOperationArgumentIsMissing() {
        exit.expectSystemExit();
        String args = "--operation";
        CommandLineInterface.main(args.split(" "));
    }

    @Test
    public void shouldFailWhenBrokerIsMissing() {
        exit.expectSystemExit();
        // Setup CLI parameters
        String args = "--operation produce"
                + " --topics topic1"
                + " --msg Hello_World!";
        CommandLineInterface.main(args.split(" "));
    }                                             

    @Test
    public void shouldFailWhenTopicIsMissing() {
        exit.expectSystemExit();
        // Setup CLI parameters
        String args = "--operation produce"
                + " --brokers " + Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT)
                + " --msg Hello_World!";
        CommandLineInterface.main(args.split(" "));
    }


    @Test
    public void shouldFailWhenMessageIsMissing() {
        exit.expectSystemExit();
        // Setup CLI parameters
        String args = "--operation produce"
                + " --brokers " + Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT)
                + " --to-topic topic1";
        CommandLineInterface.main(args.split(" "));
    }

    @Test
    public void shouldConsumeSuccessfullyWithAllArguments() {
        try {

            final int numOfRecordsForTest = 10;

            // Create a topic with 3 partitions
            final String topicName = createTopic()
                    .partitions(1)
                    .go();

            // produceWithStreamingSDK records
            Map<String, ProducerRecord<byte[]>> recordsProduced = produceTo(topicName)
                    .tenantGroup("group0")
                    .numberOfRecords(numOfRecordsForTest)
                    .produce()
                    .asMap();

            // Check that all records were produced successfully
            Assert.assertEquals(numOfRecordsForTest, recordsProduced.size());

            // Setup CLI parameters
            String args = "--operation consume"
                    + " --brokers " + Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT)
                    + " --from-topic " + topicName
                    + " --config enable.auto.commit=false,auto.offset.reset=earliest" //optional
                    + " --consume-records 10 " //optional
                    + " --consume-timeout 30000" //optional
                    + " --tenant-group group0" // optional
                    + " --cg cg1"; // optional

            CommandLineInterface cli = new CommandLineInterface(args.split(" "));
            ExecutionResult executionResult = cli.execute();
            List<ConsumerRecordResult> result = (List<ConsumerRecordResult>) executionResult.getResult();

            Assert.assertTrue(result.size() > 0);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void shouldConsumeSuccessfullyOnlyWithMandatoryArguments() {
        try {

            final int numOfRecordsForTest = 10;

            // Create a topic with 3 partitions
            final String topicName = createTopic()
                    .partitions(1)
                    .go();

            // produceWithStreamingSDK records
            Map<String, ProducerRecord<byte[]>> recordsProduced = produceTo(topicName)
                    .numberOfRecords(numOfRecordsForTest)
                    .produce()
                    .asMap();

            // Check that all records were produced successfully
            Assert.assertEquals(numOfRecordsForTest, recordsProduced.size());

            // Setup CLI parameters
            String args = "--operation consume"
                    + " --brokers " + Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT)
                    + " --config auto.offset.reset=earliest"
                    + " --from-topic " + topicName;

            CommandLineInterface cli = new CommandLineInterface(args.split(" "));
            ExecutionResult executionResult = cli.execute();
            List<ConsumerRecordResult> result = (List<ConsumerRecordResult>) executionResult.getResult();

            Assert.assertTrue(result.size() > 0);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void shouldFailWhenConsumeAndFromTopicIsMissing() {
        exit.expectSystemExit();
        // Setup CLI parameters
        String args = "--operation consume"
                + " --brokers " + Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT)
                + " --config enable.auto.commit=false,auto.offset.reset=earliest" //optional
                + " --consume-records 10 " //optional
                + " --consume-timeout 30000" //optional
                + " --tenant-group group0" // optional
                + " --cg cg1"; // optional
        CommandLineInterface.main(args.split(" "));
    }

    @Test
    public void shouldFailWithInvalidAlphanumericPartition() {
        exit.expectSystemExit();
        // Setup CLI parameters
        String args = "--operation produce"
                + " --brokers " + Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT)
                + " --to-topic topic1"
                + " --sharding-key a123" // optional
                + " --msg Hello_World!"
                + " --config linger.ms=1000,batch.size=100000,compression.type=lz4" //optional
                + " --headers correlationId=1234,clientId=56567" //optional
                + " --tenant-group group0" //optional
                + " --partition erer4tet569"; //optional

        CommandLineInterface.main(args.split(" "));
    }

    @Test
    public void shouldFailWithInvalidNumericPartition() {
        exit.expectSystemExit();
        // Setup CLI parameters
        String args = "--operation produce"
                + " --brokers " + Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT)
                + " --to-topic topic1"
                + " --sharding-key a123" // optional
                + " --msg Hello_World!"
                + " --config linger.ms=1000,batch.size=100000,compression.type=lz4" //optional
                + " --headers correlationId=1234,clientId=56567" //optional
                + " --tenant-group group0" //optional
                + " --partition -1"; //optional
        CommandLineInterface.main(args.split(" "));
    }

    @Test
    public void shouldFailWhenConsumeAndBrokersIsMissing() {
        exit.expectSystemExit();
        // Setup CLI parameters
        String args = "--operation consume"
                + " --from-topic " + "topicName"
                + " --config enable.auto.commit=false,auto.offset.reset=earliest" //optional
                + " --consume-records 10 " //optional
                + " --consume-timeout 30000" //optional
                + " --tenant-group group0" // optional
                + " --cg cg1"; // optional

        CommandLineInterface.main(args.split(" "));
    }

    private Topic.Builder createTopic() {
        return new Topic.Builder();
    }

    public ProducerHelper produceTo(final String topicName) {
        return ProducerHelper.produceTo(topicName);
    }

    @Test
    public void shouldFailWithEmptyPartition() {
        exit.expectSystemExit();
        // Setup CLI parameters
        String args = "--operation produce"
                + " --brokers " + Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT)
                + " --to-topic topic1"
                + " --sharding-key a123" // optional
                + " --msg Hello_World!"
                + " --config linger.ms=1000,batch.size=100000,compression.type=lz4" //optional
                + " --headers correlationId=1234,clientId=56567" //optional
                + " --tenant-group group0" //optional
                + " --partition "; //optional

        CommandLineInterface.main(args.split(" "));
    }
}
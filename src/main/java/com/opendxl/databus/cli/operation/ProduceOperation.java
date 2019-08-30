/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli.operation;

import com.opendxl.databus.cli.CliUtils;
import com.opendxl.databus.cli.CommandLineInterface;
import com.opendxl.databus.cli.entity.ExecutionResult;
import com.opendxl.databus.common.RecordMetadata;
import com.opendxl.databus.entities.Headers;
import com.opendxl.databus.entities.MessagePayload;
import com.opendxl.databus.entities.RoutingData;
import com.opendxl.databus.serialization.ByteArraySerializer;
import com.opendxl.databus.cli.Options;
import com.opendxl.databus.producer.Callback;
import com.opendxl.databus.producer.DatabusProducer;
import com.opendxl.databus.producer.Producer;
import com.opendxl.databus.producer.ProducerConfig;
import com.opendxl.databus.producer.ProducerRecord;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSet;
import joptsimple.internal.Strings;
import org.apache.log4j.Logger;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * It represents a --consume operation command line
 */
public class ProduceOperation implements CommandLineOperation {


    /**
     * Timeout to wait the result of producing a record
     */
    private static final long PRODUCER_RESULT_TIMEOUT_MS = 8000;

    /**
     * Logger
     */
    private static final Logger LOG = Logger.getLogger(ProduceOperation.class);

    /**
     * The operation name
     */
    private  static final String OPERATION_NAME = OperationArguments.PRODUCE.argumentName;

    /**
     * A synchronizer to wait a async producer result
     */
    private final CountDownLatch countDownLatch;

    /**
     * A list of mandatory options for this operation command line
     */
    private Map<Options, ArgumentAcceptingOptionSpec<String>> mandatoryOptions = new HashMap<>();

    /**
     * Command line parsed options
     */
    private final OptionSet options;

    /**
     * The result of execute --produce operation
     */
    private ExecutionResult executionResult;

    /**
     * Constructor
     *
     * @param optionSpecMap Map of options spec
     * @param options       parsed options
     */
    public ProduceOperation(final Map<Options, ArgumentAcceptingOptionSpec<String>> optionSpecMap,
                            final OptionSet options) {
        this.options = options;
        mandatoryOptions.put(Options.BROKER_LIST, optionSpecMap.get(Options.BROKER_LIST));
        mandatoryOptions.put(Options.MESSAGE, optionSpecMap.get(Options.MESSAGE));
        mandatoryOptions.put(Options.TO_TOPIC, optionSpecMap.get(Options.TO_TOPIC));

        countDownLatch = new CountDownLatch(1);
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public Map<Options, ArgumentAcceptingOptionSpec<String>> getMandatoryOptions() {
        return mandatoryOptions;
    }

    /**
     * {@inheritDoc}
     *
     */
    @Override
    public String getOperationName() {
        return OPERATION_NAME;
    }

    /**
     * {@inheritDoc}
     *
     * @return The result of consuming operation command line. Example in Json notation after serializing
     */
    @Override
    public ExecutionResult execute() {

        try {

            // Get option values
            String brokerList = options.valueOf(mandatoryOptions.get(Options.BROKER_LIST));
            String topic = options.valueOf(mandatoryOptions.get(Options.TO_TOPIC));
            String message = options.valueOf(mandatoryOptions.get(Options.MESSAGE));
            String tenantGroup = "";
            if (options.hasArgument(Options.TENANT_GROUP.getOptionName())) {
                tenantGroup = options.valueOf(Options.TENANT_GROUP.getOptionName()).toString();
            }
            String shardingKey = "";
            if (options.hasArgument(Options.SHARDING_KEY.getOptionName())) {
                shardingKey = options.valueOf(Options.SHARDING_KEY.getOptionName()).toString();
            }
            String partition = Strings.EMPTY;
            if (options.hasArgument(Options.PARTITION.getOptionName())) {
                partition = options.valueOf(Options.PARTITION.getOptionName()).toString();
            }

            // parse headers argument
            final Map<String, String> headersMap = new HashMap<>();
            if (options.hasArgument(Options.HEADERS.name().toLowerCase())) {
                Properties configArg =
                        CliUtils.stringToMap(options.valueOf(Options.HEADERS.name().toLowerCase()).toString());
                for (String key : configArg.stringPropertyNames()) {
                    headersMap.put(key, configArg.getProperty(key));
                }
            }

            // parse config arguments
            final Map config = new HashMap<String, Object>();
            if (options.hasArgument(Options.CONFIG.name().toLowerCase())) {
                Properties configArg =
                        CliUtils.stringToMap(options.valueOf(Options.CONFIG.name().toLowerCase()).toString());
                for (String key : configArg.stringPropertyNames()) {
                    config.put(key, configArg.getProperty(key));
                }
            }

            // Create a producer
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            Producer<byte[]> producer = new DatabusProducer<>(config, new ByteArraySerializer());

            // Create a Databus Message
            // TODO: Add option for shardingKey, tenantGroup and headers
            final byte[] payload = message.getBytes(Charset.defaultCharset());
            RoutingData routingData = getRoutingData(topic, shardingKey, tenantGroup, partition);

            final MessagePayload<byte[]> messagePayload = new MessagePayload<>(payload);
            final Headers headers = new Headers(headersMap);
            final ProducerRecord<byte[]> producerRecord = new ProducerRecord<>(routingData, headers, messagePayload);

            // Produce the message
            producer.send(producerRecord, new ProducerResultCallback(producerRecord.getRoutingData().getShardingKey()));
            final boolean isElapsed = countDownLatch.await(PRODUCER_RESULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (!isElapsed) {
                LOG.error("Error sending a record . TIMEOUT");
                return new ExecutionResult("ERROR", "TIMEOUT", options.asMap());
            }
            producer.flush();
            producer.close();
            return executionResult;

        } catch (Exception e) {
            CliUtils.printUsageAndFinish(CommandLineInterface.parser, e.getMessage(), e);
        }

        return null;
    }

    /**
     * Gets a Routing data object instance for the given parameters
     * @param topic The topic name
     * @param shardingKey The sharding key
     * @param tenantGroup The tenant group
     * @param partition The the given partition number
     *
     * @return An {@link RoutingData} instance
     */
    private RoutingData getRoutingData(final String topic, final String shardingKey, final String tenantGroup,
                                         final String partition) {
        RoutingData routingData;
        if (!partition.isEmpty()) {
            routingData = new RoutingData(topic, shardingKey, tenantGroup, Integer.parseInt(partition));
        } else {
            routingData = new RoutingData(topic, shardingKey, tenantGroup);
        }
        return  routingData;
    }

    /**
     *
     * @param executionResult The result of producing a record. This methos is invoked by the callback
     *
     */
    private synchronized void setProducerResult(final ExecutionResult executionResult) {
        this.executionResult = executionResult;
    }

    /**
     * Callback invoked by Kafka to set the result of producing a messages
     */
    private class ProducerResultCallback implements Callback {

        /**
         * The sharding key of the message. We want to track the message by using the shardingKey.
         */
        private String shardingKey;

        ProducerResultCallback(final String shardingKey) {
            this.shardingKey = shardingKey;
        }

        /**
         * Invoked by Kafka in asyn way to set the result of producing
         *
         * @param metadata The metadata for the record that was sent (i.e. the partition and offset). Null if an error
         *        occurred.
         * @param exception The exception thrown during processing of this record. Null if no error occurred.
         *                  Possible thrown exceptions include:
         *
         */
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                LOG.error("Error sending a record " + exception.getMessage());
                setProducerResult(new ExecutionResult("ERROR", exception.getMessage(), options.asMap()));
                countDownLatch.countDown();
                return;
            }
            final String resultMessage = new StringBuilder().append("SHARDING-KEY: ").append(shardingKey)
                    .append(" TOPICS:").append(metadata.topic())
                    .append(" PARTITION:").append(metadata.partition())
                    .append(" OFFSET:").append(metadata.offset())
                    .toString();

            LOG.info("[PRODUCER <- KAFKA][OK MSG SENT] " + resultMessage);
            setProducerResult(new ExecutionResult("OK", resultMessage, options.asMap()));
            countDownLatch.countDown();

        }
    }
}

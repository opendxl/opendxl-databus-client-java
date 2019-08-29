/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli.operation;

import com.opendxl.databus.cli.CliUtils;
import com.opendxl.databus.cli.Options;
import com.opendxl.databus.cli.entity.ConsumerRecordResult;
import com.opendxl.databus.cli.entity.ExecutionResult;
import com.opendxl.databus.consumer.Consumer;
import com.opendxl.databus.consumer.ConsumerConfiguration;
import com.opendxl.databus.consumer.ConsumerRecord;
import com.opendxl.databus.consumer.ConsumerRecords;
import com.opendxl.databus.consumer.DatabusConsumer;
import com.opendxl.databus.serialization.ByteArrayDeserializer;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSet;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * It represents a --consume operation command line
 */
public class ConsumeOperation implements CommandLineOperation {

    /**
     * Logger
     */
    private static final Logger LOG = Logger.getLogger(ConsumeOperation.class);

    /**
     * The operation name
     */
    private  static final String OPERATION_NAME = OperationArguments.CONSUME.argumentName;

    /**
     * A list of mandatory options for this operation command line
     */
    private Map<Options, ArgumentAcceptingOptionSpec<String>> mandatoryOptions = new HashMap<>();

    /**
     * Command line parsed options
     */
    private final OptionSet options;

    /**
     * Constructor
     *
     * @param optionSpecMap Map of options spec
     * @param options       parsed options
     */
    public ConsumeOperation(final Map<Options, ArgumentAcceptingOptionSpec<String>> optionSpecMap,
                            final OptionSet options) {
        this.options = options;
        mandatoryOptions.put(Options.BROKER_LIST, optionSpecMap.get(Options.BROKER_LIST));
        mandatoryOptions.put(Options.FROM_TOPIC, optionSpecMap.get(Options.FROM_TOPIC));
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
            final String brokerList = options.valueOf(mandatoryOptions.get(Options.BROKER_LIST));
            final String commaSeparatedTopics = options.valueOf(mandatoryOptions.get(Options.FROM_TOPIC));
            final List<String> topics = Arrays.stream(commaSeparatedTopics.split(",")).collect(Collectors.toList());
            String tenantGroup = "";
            if (options.hasArgument(Options.TENANT_GROUP.getOptionName())) {
                tenantGroup = options.valueOf(Options.TENANT_GROUP.getOptionName()).toString();
            }
            String consumeTimeout = "0";
            if (options.hasArgument(Options.CONSUME_TIMEOUT.getOptionName())) {
                consumeTimeout = options.valueOf(Options.CONSUME_TIMEOUT.getOptionName()).toString();
            }

            // parse config arguments
            final Map config = new HashMap<String, Object>();
            if (options.hasArgument(Options.CONFIG.name().toLowerCase())) {
                final Properties configArg =
                        CliUtils.stringToMap(options.valueOf(Options.CONFIG.name().toLowerCase()).toString());
                for (String key : configArg.stringPropertyNames()) {
                    config.put(key, configArg.getProperty(key));
                }
            }

            // Consumer Group
            String consumerGroupName = (String) config.get(ConsumerConfiguration.GROUP_ID_CONFIG);
            if (consumerGroupName == null || consumerGroupName.isEmpty())  {
                consumerGroupName = UUID.randomUUID().toString();
            }

            // Auto commit
            String enableAutoCommit = (String) config.get(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG);
            if (consumerGroupName == null || consumerGroupName.isEmpty()) {
                enableAutoCommit = "true";
            }

            // Create a consumer
            config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            config.put(ConsumerConfiguration.GROUP_ID_CONFIG, consumerGroupName);
            config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
            final Consumer<byte[]> consumer = new DatabusConsumer<>(config, new ByteArrayDeserializer());

            // Subscribe to topics
            if (tenantGroup == null || tenantGroup.isEmpty()) {
                consumer.subscribe(topics);
            } else {
                final Map<String, List<String>> groupTopics = new HashMap<>();
                groupTopics.put(tenantGroup, topics);
                consumer.subscribe(groupTopics);
            }

            // Consume records, commit and close consumer
            ConsumerRecords<byte[]> records = consumer.poll(Integer.parseInt(consumeTimeout));
            LOG.warn(records.count());
            if (records.count() > 0 && Boolean.parseBoolean(enableAutoCommit) == Boolean.FALSE) {
                consumer.commitSync();
            }

            // Create a result
            List<ConsumerRecordResult> recordResults = new ArrayList<>();
            for (ConsumerRecord<byte[]> record : records) {
                recordResults.add(new ConsumerRecordResult(record.getKey(),
                        new String(record.getMessagePayload().getPayload()),
                        record.getComposedTopic(),
                        record.getTopic(),
                        record.getTenantGroup(),
                        record.getHeaders().getAll(),
                        record.getOffset(),
                        record.getPartition(),
                        record.getTimestamp())
                );
            }

            consumer.close();

            final ExecutionResult result = new ExecutionResult("OK", recordResults, options.asMap());
            return result;

        } catch (Exception exception) {
            LOG.error("Error consuming records " + exception.getMessage());
            final ExecutionResult result = new ExecutionResult("ERROR", exception.getMessage(), options.asMap());
            return result;
        }

    }

}

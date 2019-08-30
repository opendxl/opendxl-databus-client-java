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
import java.util.concurrent.TimeUnit;
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
    private Map<Options, ArgumentAcceptingOptionSpec> mandatoryOptions = new HashMap<>();

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
    public ConsumeOperation(final Map<Options, ArgumentAcceptingOptionSpec> optionSpecMap,
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
    public Map<Options, ArgumentAcceptingOptionSpec> getMandatoryOptions() {
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
        Consumer<byte[]> consumer = null;
        try {
            // Get option values
            final String brokerList = options.valueOf(mandatoryOptions.get(Options.BROKER_LIST)).toString();
            final String commaSeparatedTopics = options.valueOf(mandatoryOptions.get(Options.FROM_TOPIC)).toString();
            final List<String> topics = Arrays.stream(commaSeparatedTopics.split(",")).collect(Collectors.toList());
            String tenantGroup = "";
            if (options.hasArgument(Options.TENANT_GROUP.getOptionName())) {
                tenantGroup = options.valueOf(Options.TENANT_GROUP.getOptionName()).toString();
            }
            final int consumeTimeoutMs = (int) options.valueOf(Options.CONSUME_TIMEOUT.getOptionName());
            final int consumeRecords = (int) options.valueOf(Options.CONSUME_RECORDS.getOptionName());

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
            final String consumerGroupName;
            if (options.hasArgument(Options.CG.name().toLowerCase())) {
                consumerGroupName = options.valueOf(Options.CG.getOptionName()).toString().toLowerCase();
            } else {
                consumerGroupName = UUID.randomUUID().toString();
            }

            // Auto commit
            String enableAutoCommit = (String) config.get(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG);
            if (enableAutoCommit == null || enableAutoCommit.isEmpty()) {
                enableAutoCommit = "true";
            }

            // Create a consumer
            config.put(ConsumerConfiguration.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            config.put(ConsumerConfiguration.GROUP_ID_CONFIG, consumerGroupName);
            config.put(ConsumerConfiguration.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
            consumer = new DatabusConsumer<>(config, new ByteArrayDeserializer());

            // Subscribe to topics
            if (tenantGroup == null || tenantGroup.isEmpty()) {
                consumer.subscribe(topics);
            } else {
                final Map<String, List<String>> groupTopics = new HashMap<>();
                groupTopics.put(tenantGroup, topics);
                consumer.subscribe(groupTopics);
            }

            // Collect records
            List<ConsumerRecordResult> recordResults = new ArrayList<>();
            final long startTime = System.nanoTime();
            ConsumerRecords<byte[]> records;
            boolean cont = true;
            do {
                // Consume records and commit
                records = consumer.poll(100);
                LOG.warn(records.count());
                if (records.count() > 0 && Boolean.parseBoolean(enableAutoCommit) == Boolean.FALSE) {
                    consumer.commitSync();
                }

                // Create a result
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

                // Loops ends when reads enough records or timeout
                if (recordResults.size() >= consumeRecords
                        || (TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime,
                                TimeUnit.NANOSECONDS) >= consumeTimeoutMs)) {
                    cont = false;
                }

            } while (cont);

            final ExecutionResult result = new ExecutionResult("OK", recordResults, options.asMap());
            return result;

        } catch (Exception exception) {
            LOG.error("Error consuming records " + exception.getMessage());
            final ExecutionResult result = new ExecutionResult("ERROR", exception.getMessage(), options.asMap());
            return result;
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }

    }

}

/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.opendxl.databus.cli.operation.OperationFactory;
import com.opendxl.databus.cli.entity.ExecutionResult;
import com.opendxl.databus.cli.operation.CommandLineOperation;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.util.HashMap;
import java.util.Map;

/**
 * Main class for the Databus client CLI.
 * This class enable Databus client to be used as a command line tool.
 * Just type java -jar JavaJarName.jar and follow instructions
 */
public class CommandLineInterface {

    /**
     * Represents the set of options arguments in the command line
     */
    private OptionSet options;

    /**
     * Operation is a CommandLineOperation instance suitable to {@link Channel} API method that will be executed.
     * Each API method is associated to an specific Operation
     */
    private CommandLineOperation operation;

    /**
     * Parses command line arguments
     */
    public static OptionParser parser = new OptionParser(false);

    /**
     * Constructor
     *
     * @param args Options and arguments passing in command line
     */
    public CommandLineInterface(final String[] args) {

        // operation option spec represented as --operation command line
        final ArgumentAcceptingOptionSpec<String> operationsOpt =
                parser.accepts("operation", "Operations: produce | consume")
                        .withRequiredArg()
                        .describedAs("operation")
                        .ofType(String.class)
                        .required();

        // Broker list as --brokers command line
        final ArgumentAcceptingOptionSpec<String> brokerListOpt =
                parser.accepts("brokers", "Comma-separated broker list: "
                        + " Example: broker1:port1,broker2:port2,...,brokerN:portN")
                        .withRequiredArg()
                        .describedAs("broker list")
                        .ofType(String.class)
                        .required();

        // topic option spec represented as --to-topic command line
        final ArgumentAcceptingOptionSpec<String> toTopicOpt =
                parser.accepts("to-topic", "Topic name to produce")
                        .withRequiredArg()
                        .describedAs("to-topic")
                        .ofType(String.class);

        // message option spec represented as --msg command line
        final ArgumentAcceptingOptionSpec<String> msgOpt =
                parser.accepts("msg", "message to be produced")
                        .withRequiredArg()
                        .describedAs("message")
                        .ofType(String.class);

        // Tenant Group option spec represented as --sharding-key command line
        final ArgumentAcceptingOptionSpec<String> shardingKeyOpt =
                parser.accepts("sharding-key", "Sharding key")
                        .withOptionalArg()
                        .describedAs("sharding-key")
                        .ofType(String.class)
                        .defaultsTo("");

        // Configuration option spec represented as --config command line
        final ArgumentAcceptingOptionSpec<String> configOpt =
                parser.accepts("config", "The producer/consumer configuration list: Example:"
                        + " linger.ms=1000,batch.size=100000,compression.type=lz4")
                        .withRequiredArg()
                        .describedAs("config")
                        .ofType(String.class);

        // Tenant Group option spec represented as --tenant-group command line
        final ArgumentAcceptingOptionSpec<String> tenantGroupOpt =
                parser.accepts("tenant-group", "Tenant Group")
                        .withOptionalArg()
                        .describedAs("tenant-group")
                        .ofType(String.class)
                        .defaultsTo("");


        // Configuration option spec represented as --headers command line
        final ArgumentAcceptingOptionSpec<String> headersOpt =
                parser.accepts("headers", "The producer headers: ")
                        .withOptionalArg()
                        .describedAs("headers")
                        .ofType(String.class)
                        .defaultsTo("");

        // topic option spec represented as --from-topic command line
        final ArgumentAcceptingOptionSpec<String> fromTopicOpt =
                parser.accepts("from-topic", "Coma-separated topic name list to consume. "
                        + "Example: topic1,topic2,...,topicN")
                        .withRequiredArg()
                        .describedAs("from-topic")
                        .ofType(String.class);

        // Consumer config option spec represented as --consume-timeout command line
        final ArgumentAcceptingOptionSpec<Integer> consumeTimeoutOpt =
                parser.accepts("consume-timeout", "Consume Poll Timeout. Time in ms that the consumer"
                        + " waits for new records during a consume operation. "
                        + " Optional parameter, if absent, it defaults to 5000 ms.")
                        .withRequiredArg()
                        .ofType(Integer.class)
                        .describedAs("consume-timeout")
                        .defaultsTo(15000);

        // Consumer config option spec represented as --consume-records command line
        final ArgumentAcceptingOptionSpec<Integer> consumeRecordsOpt =
                parser.accepts("consume-records", "Consume Poll expected records. "
                        + "Number of expected records. ")
                        .withRequiredArg()
                        .ofType(Integer.class)
                        .describedAs("consume-records")
                        .defaultsTo(1);

        // Consumer Group option spec represented as --cg command line
        final ArgumentAcceptingOptionSpec<String> consumerGroupOpt =
                parser.accepts("cg", "The consumer group name.")
                        .withRequiredArg()
                        .describedAs("cg")
                        .ofType(String.class);


        // Configuration option spec represented as --partition command line
        final ArgumentAcceptingOptionSpec<String> partitionOpt =
                parser.accepts("partition", "The partition number: ")
                        .withOptionalArg()
                        .describedAs("partition")
                        .ofType(String.class)
                        .defaultsTo("");

        if (args.length == 0) {
            CliUtils.printUsageAndFinish(parser, "There are not options");
        }

        parseOptions(args);
        final Map<Options, ArgumentAcceptingOptionSpec> optionSpecMap = new HashMap();
        optionSpecMap.put(Options.OPERATION, operationsOpt);
        optionSpecMap.put(Options.TO_TOPIC, toTopicOpt);
        optionSpecMap.put(Options.BROKER_LIST, brokerListOpt);
        optionSpecMap.put(Options.MESSAGE, msgOpt);
        optionSpecMap.put(Options.CONFIG, configOpt);
        optionSpecMap.put(Options.TENANT_GROUP, tenantGroupOpt);
        optionSpecMap.put(Options.SHARDING_KEY, shardingKeyOpt);
        optionSpecMap.put(Options.HEADERS, shardingKeyOpt);
        optionSpecMap.put(Options.FROM_TOPIC, fromTopicOpt);
        optionSpecMap.put(Options.CONSUME_TIMEOUT, consumeTimeoutOpt);
        optionSpecMap.put(Options.CONSUME_RECORDS, consumeRecordsOpt);
        optionSpecMap.put(Options.CG, consumerGroupOpt);
        optionSpecMap.put(Options.PARTITION, partitionOpt);
        this.operation = buildOperation(optionSpecMap);
        CliUtils.validateMandatoryOperationArgs(operation, parser, options);

    }


    /**
     * This method checks that  --operation argument option contains a validated value and
     * all mandatory option being included in command line. Finally, it create a {@link CommandLineOperation}
     * used later to perform the specific one.
     *
     * @param optionSpecMap Keeps a relationship between a {@link Options} and a Option Spec
     */
    private CommandLineOperation buildOperation(
            final Map<Options, ArgumentAcceptingOptionSpec> optionSpecMap) {

        if (!options.has(optionSpecMap.get(Options.OPERATION))) {
            CliUtils.printUsageAndFinish(parser, "--operation is missing");
        }

        if (options.has(optionSpecMap.get(Options.PARTITION))
                && !CliUtils.isValidPartitionNumber(options.valueOf(Options.PARTITION.getOptionName()).toString())) {
            CliUtils.printUsageAndFinish(parser, "--partition must be a number value > 0");
        }

        OperationFactory factory = new OperationFactory(optionSpecMap, options);
        return factory.getOperation(optionSpecMap.get(Options.OPERATION));
    }

    /**
     * Execute an operation
     *
     * @return ExecutionResult
     */
    public ExecutionResult execute() {
        return operation.execute();
    }


    /**
     * Entry point
     *
     * @param args Command line options and arguments
     */
    public static void main(String[] args) {
        final CommandLineInterface cli = new CommandLineInterface(args);
        final ExecutionResult executionResult = cli.execute();
        final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        CliUtils.printUsageAndFinish(gson.toJson(executionResult));
    }

    /**
     * It parses command line options and their arguments values. If they do not meet spec requirements, it shows
     * the usage and exists with a error.
     *
     * @param args Options and arguments values passed in command line
     */
    private void parseOptions(String[] args) {

        try {
            // parse and make sure options passed in command line meet option spec
            this.options = parser.parse(args);
        } catch (Exception e) {
            CliUtils.printUsageAndFinish(parser, e.getMessage());
        }

    }

}

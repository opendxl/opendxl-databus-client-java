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
import java.util.List;
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
     * Each API method is associated to an specific Operation, e.g.:
     * {@link Channel#create()} is associated to {@link com.opendxl.streaming.cli.operation.CreateOperation},
     * {@link Channel#subscribe(List)} is associated to {@link com.opendxl.streaming.cli.operation.SubscribeOperation},
     * etc. Goal of each Operation class is to call its associated API method from the command line.
     */
    private CommandLineOperation operation;
    /**
     * Parses command line arguments
     */
    public static OptionParser parser = new OptionParser(false);

    /**
     * Constructor
     *
     * @param args options and arguments passing in command line
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
                parser.accepts("brokers", "Comma-separated broker list: broker1:port1,broker2:port2,...,brokerN:portN")
                        .withRequiredArg()
                        .describedAs("broker list")
                        .ofType(String.class)
                        .required();

        // topic option spec represented as --to-topic command line
        final ArgumentAcceptingOptionSpec<String> topicIdOpt =
                parser.accepts("to-topic", "Topic name to produce")
                        .withRequiredArg()
                        .describedAs("to-topic")
                        .ofType(String.class)
                        .required();

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
                parser.accepts("config", "The producer/consumer configuration list: "
                        + "linger.ms=1000,batch.size=100000,compression.type=lz4")
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
        final Map<Options, ArgumentAcceptingOptionSpec<String>> optionSpecMap = new HashMap();
        optionSpecMap.put(Options.OPERATION, operationsOpt);
        optionSpecMap.put(Options.TO_TOPIC, topicIdOpt);
        optionSpecMap.put(Options.BROKER_LIST, brokerListOpt);
        optionSpecMap.put(Options.MESSAGE, msgOpt);
        optionSpecMap.put(Options.CONFIG, configOpt);
        optionSpecMap.put(Options.TENANT_GROUP, tenantGroupOpt);
        optionSpecMap.put(Options.SHARDING_KEY, shardingKeyOpt);
        optionSpecMap.put(Options.HEADERS, shardingKeyOpt);
        optionSpecMap.put(Options.PARTITION, partitionOpt);

        this.operation = buildOperation(optionSpecMap);
        CliUtils.validateMandatoryOperationArgs(operation, parser, options);

    }


    /**
     * This method checks that  --operation argument option contains a validated value and
     * all mandatory option being included in command line. Finally, it create a {@link CommandLineOperation}
     * used later to perform the specific one.
     *
     * @param optionSpecMap keeps a relationship between a {@link Options} and a Option Spec
     */
    private CommandLineOperation buildOperation(
            final Map<Options, ArgumentAcceptingOptionSpec<String>> optionSpecMap) {

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
     * @return ExceutionResult
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
     * @param args options and arguments values passed in command line
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

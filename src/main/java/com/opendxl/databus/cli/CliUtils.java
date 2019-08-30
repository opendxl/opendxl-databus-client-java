/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli;


import com.opendxl.databus.cli.operation.CommandLineOperation;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Facilities for command line options
 */
public class CliUtils {

    /**
     * Logger
     */
    private static final Logger LOG = Logger.getLogger(CliUtils.class);

    /**
     * A static field which represents an invalid partition, used as a default value
     * when a partition is not given as parameter in the command line.
     */
    private static final int INVALID_PARTITION = -1;

    private CliUtils() {

    }

    /**
     * Prints usage and exits without system error
     *
     * @param executionResult It represents the result of a command line operation
     */
    public static void printUsageAndFinish(final String executionResult) {
        System.out.println(executionResult);
        Runtime.getRuntime().exit(0);
    }

    /**
     * This method is invoked when the command line made up by options and argument
     * are ill formed or do not meet options spec. Then , it shows the usage and exit with a error
     *
     * @param parser  The utility capable to show the usage
     * @param message Message Error
     */
    public static void printUsageAndFinish(final OptionParser parser, final String message) {
        printUsageAndFinish(parser, message, null);
    }


    /**
     * This method is invoked when the command line made up by options and argument
     * are ill formed or do not meet options spec. Then , it shows the usage and exit with a error
     *
     * @param parser  The utility capable to show the usage
     * @param message Message Error
     * @param exception exception that caused this error
     */
    public static void printUsageAndFinish(final OptionParser parser, final String message, final Exception exception) {
        try {
            System.err.println("ERROR: " + message);
            if (exception != null) {
                exception.printStackTrace();
            }
            parser.printHelpOn(System.out);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Runtime.getRuntime().exit(1);
    }


    /**
     * This method validate the mandatory arguments for a specific operation
     *
     * @param operationArgument It represents a instance of --operation argument
     * @param parser            The utility capable to show the usage
     * @param options           Represents the set of options in the command line
     */
    public static void validateMandatoryOperationArgs(final CommandLineOperation operationArgument,
                                                      final OptionParser parser, final OptionSet options) {
        if (operationArgument == null) {
            CliUtils.printUsageAndFinish(parser, "[operation argument is unknown]");
        }

        operationArgument.getMandatoryOptions().forEach((option, mandatoryOption) -> {
            if (!options.has(mandatoryOption) && mandatoryOption.defaultValues().isEmpty()) {
                CliUtils.printUsageAndFinish(parser, mandatoryOption.toString() + " is missing for "
                        + operationArgument.getOperationName() + " operation");
            }
        });
    }


    /**
     * Get properties from comma-separated config String
     *
     * @param commaSeparatedValue comma-separated values key1=vale1,key2=value2,...,keyN=valueN
     * @return Properties instance
     */
    public static Properties stringToMap(final String commaSeparatedValue) {
        final Properties map = new Properties();
        try {
            final String[] keyValuePairs = commaSeparatedValue.split(",");

            for (String pair : keyValuePairs) {
                String[] entry = pair.split("=");
                map.put(entry[0].trim(), entry[1].trim());
            }
        } catch (Exception e) {
            CliUtils.printUsageAndFinish(CommandLineInterface.parser, e.getMessage(), e);
        }
        return map;
    }

    /**
     * This method returns if a partition provided by the command line is valid
     *
     * @param partition An argument for partition number.
     * @return If the given partition String is a valid partition number
     *
     */
    public static boolean isValidPartitionNumber(final String partition) {
        boolean isValidPartitionNumber = true;
        try {
            if (!partition.isEmpty()) {
                Integer validPartitionNumber = Integer.parseInt(partition);
                if (!(validPartitionNumber > INVALID_PARTITION)) {
                    isValidPartitionNumber = false;
                }
            } else {
                isValidPartitionNumber = false;
            }
        } catch (NumberFormatException | NullPointerException e) {
            isValidPartitionNumber = false;
        }
        return isValidPartitionNumber;
    }
}

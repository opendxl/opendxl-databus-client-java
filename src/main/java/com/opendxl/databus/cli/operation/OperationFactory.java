/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli.operation;

import com.opendxl.databus.cli.Options;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSet;

import java.util.HashMap;
import java.util.Map;

/**
 * This factory class creates instances of --operations arguments . For instance creates a
 * {@link ProduceOperation} when command line is --operation produce
 *
 */
public class OperationFactory {
    private final  Map<OperationArguments, CommandLineOperation> operationArgumentsFactoryMap = new HashMap<>();

    private final OptionSet options;

    public OperationFactory(final Map<Options, ArgumentAcceptingOptionSpec> optionSpecMap,
                            final OptionSet options) {
        this.options = options;
        operationArgumentsFactoryMap.put(OperationArguments.PRODUCE,
                new ProduceOperation(optionSpecMap, options));
        operationArgumentsFactoryMap.put(OperationArguments.CONSUME,
                new ConsumeOperation(optionSpecMap, options));
    }

    /**
     *
     * @param operationsOpt Operations supported by command line cli
     * @return Command line operation instance
     */
    public CommandLineOperation getOperation(final ArgumentAcceptingOptionSpec<String> operationsOpt) {
        String operationArgument = options.valueOf(operationsOpt);
        return operationArgumentsFactoryMap.get(OperationArguments.fromString(operationArgument));
    }

}

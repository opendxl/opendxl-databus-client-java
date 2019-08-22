/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli.operation;

import com.opendxl.databus.cli.Options;
import com.opendxl.databus.cli.entity.ExecutionResult;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSet;

import java.util.HashMap;
import java.util.Map;

/**
 * It represents a --consume operation command line
 */
public class ConsumeOperation implements CommandLineOperation {

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


        return null;
    }

}

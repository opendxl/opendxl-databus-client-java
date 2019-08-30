/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli.operation;

import com.opendxl.databus.cli.Options;
import com.opendxl.databus.cli.entity.ExecutionResult;
import joptsimple.ArgumentAcceptingOptionSpec;

import java.util.Map;

/**
 * Interface for all --operation arguments
 */
public interface CommandLineOperation {

    /**
     *
     * @return mandatory options for the specific operation
     */
    Map<Options, ArgumentAcceptingOptionSpec> getMandatoryOptions();

    /**
     *
     * @return the operation name
     */
    String getOperationName();

    /**
     * Execute the operation
     *
     * @return Execution result
     */
    ExecutionResult execute();
}

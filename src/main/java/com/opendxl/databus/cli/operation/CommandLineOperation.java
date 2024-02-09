/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
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
     * Gets the mandatory options map
     *
     * @return mandatory options for the specific operation
     */
    Map<Options, ArgumentAcceptingOptionSpec> getMandatoryOptions();

    /**
     * Gets the operation name
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

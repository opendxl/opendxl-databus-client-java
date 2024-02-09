/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli.entity;

import joptsimple.OptionSpec;

import java.util.List;
import java.util.Map;

/**
 * Represent the result of a operation command line
 */
public class ExecutionResult {

    /**
     * Status Code
     */
    private final String code;

    /**
     * <p>Object showing the data returned by the executed command. The data varies according to the executed Operation,
     */
    private final Object result;

    /**
     * List of options used for a specific operation command line
     */
    private Map<OptionSpec<?>, List<?>> options;

    /**
     * Result of a command line operation.
     *
     * @param code OK | ERROR
     * @param result Output data returned by the executed command line operation.
     * @param options List of input options entered for the operation
     */
    public ExecutionResult(final String code, final Object result, final Map<OptionSpec<?>, List<?>> options) {

        this.code = code;
        this.result = result;
        this.options = options;
    }

    /**
     * Gets the result code of an Execution result
     *
     * @return result code
     */
    public String getCode() {
        return code;
    }

    /**
     * Gets the result of an Execution result
     *
     * @return a object that represents the result according to the specific operation
     */
    public Object getResult() {
        return result;
    }

    /**
     * Gets the option list for the operation
     *
     * @return list of input options entered for the operation
     */
    public Map<OptionSpec<?>, List<?>> getOptions() {
        return options;
    }


}

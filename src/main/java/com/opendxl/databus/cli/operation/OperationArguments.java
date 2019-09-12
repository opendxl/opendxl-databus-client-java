/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli.operation;

/**
 * Arguments of --operation command line option
 */
public enum OperationArguments {

    /**
     * Operation Argument related to the produce operation
     */
    PRODUCE("produce"),

    /**
     * Operation Argument related to the consume operation
     */
    CONSUME("consume");

    /**
     * OperationArguments constructor
     * @param argumentName The argument name associated to the OperationArgument
     */
    OperationArguments(final String argumentName) {
        this.argumentName = argumentName;
    }


    /**
     * The argument name for the OperationArgument
     */
    String argumentName;

    /**
     * Return a OperationArgument enumerated based on a string
     *
     * @param operationArgumentValue A string value to be converted in a OperationArguments enumerated
     * @return a OperationArguments enumerated
     */
    public static OperationArguments fromString(final String operationArgumentValue) {
        for (OperationArguments operationArgumentEnum : OperationArguments.values()) {
            if (operationArgumentEnum.argumentName.equalsIgnoreCase(operationArgumentValue)) {
                return operationArgumentEnum;
            }
        }
        return null;
    }
}

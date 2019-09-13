/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.exception;

/**
 * A RuntimeException exception that wrap the original exception
 * <p>
 * The SDK's user is responsible for handling this run time exceptions when a API method is called.
 * The original exception that
 * can be recovered by using {@link #getCause()}  method.
 */
public class DatabusClientRuntimeException extends RuntimeException {

    /**
     * The exception class name
     */
    private final String causedByClass;

    /**
     * Constructor with all parameters
     *
     * @param message The exception message
     * @param cause The exception cause
     * @param causedByClass The class name which triggers the exception
     */
    public DatabusClientRuntimeException(final String message,
                                         final Throwable cause,
                                         final Class causedByClass) {
        super(message, cause);
        this.causedByClass = causedByClass.getName();
    }

    /**
     * Constructor with message and causedByClass parameters
     *
     * @param message The exception message
     * @param causedByClass The class name which triggers the exception
     */
    public DatabusClientRuntimeException(final String message,
                                         final Class causedByClass) {
        this(message, null, causedByClass);
    }

    /**
     * Gets the class name of the thew exception
     *
     * @return The class name that throws the exception
     */
    public String getCausedByClass() {
        return causedByClass;
    }
}

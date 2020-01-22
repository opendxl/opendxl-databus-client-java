/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.consumer;

public enum DatabusPushConsumerListenerResponse {

    // returned by listener when it wants to continue pushing more messages
    CONTINUE_AND_COMMIT,

    // returned by listener when it wants to receive the same records
    RETRY,

    // returned by listener when it wants to stop pushing messages and commit them
    STOP_AND_COMMIT,

    // returned by listener when it wants to stop pushing messages
    STOP_NO_COMMIT

}

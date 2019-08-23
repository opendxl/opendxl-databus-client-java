/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.util;

import java.util.UUID;

public class ConsumerGroupHelper {
    public static String getRandomConsumerGroupName() {
        return UUID.randomUUID().toString();
    }
}

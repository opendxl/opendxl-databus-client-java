/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.producer;

import com.opendxl.databus.producer.internal.ProducerDefaultConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class ProducerDefaultConfigurationTest {

    @Test
    public void shouldReturnNullValue() {
        String v = ProducerDefaultConfiguration.get(ProducerConfig.BATCH_SIZE_CONFIG);
        Assert.assertTrue(v == null);
    }

    @Test
    public void shouldReturnAvalidValue() {
        String value = ProducerDefaultConfiguration.get(ProducerDefaultConfiguration.MAX_BLOCK_MS_CONFIG_KEY);
        Assert.assertTrue(ProducerDefaultConfiguration.MAX_BLOCK_MS_CONFIG_DEFAULT_VALUE.equals(value));
    }
}

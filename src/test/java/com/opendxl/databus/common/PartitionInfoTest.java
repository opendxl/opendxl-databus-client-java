/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common;

import org.junit.Assert;
import org.junit.Test;

public class PartitionInfoTest {
    @Test
    public void shouldUseToStringSuccessfully() {
        try {
            String expected = "Partition(topic = topic, partition = 0, leader = none, replicas = [], isr = [])";
            PartitionInfo pi = new PartitionInfo("topic",0,null,new Node[0],new Node[0]);
            Assert.assertTrue(expected.equals(pi.toString()));
        } catch (Exception e) {
            Assert.fail("No exception expected");
        }
    }
}

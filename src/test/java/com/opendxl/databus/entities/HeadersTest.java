/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.entities;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class HeadersTest {

    @Test
    public void NotNullValuesShouldDoNothing() {
        try {
            Map<String, String> map = new HashMap<>();
            map.put("field1","value1");
            Headers h = new Headers(map);
            Assert.assertTrue(h.get("field1").equals("value1"));

        } catch (Exception e) {
            Assert.fail("Exception is not expected: " + e.getMessage());
        }
    }

    @Test
    public void NullValuesShouldBeConvertedtoEmpty() {
        try {
            Map<String, String> map = new HashMap<>();
            map.put("field1",null);
            Headers h = new Headers(map);
            Assert.assertTrue(h.get("field1").isEmpty());

        } catch (Exception e) {
            Assert.fail("Exception is not expected: " + e.getMessage());
        }
    }

    @Test
    public void PuttingANullValuesShouldBeConvertedtoEmpty() {
        try {
            Headers h = new Headers();
            h.put("field1",null);
            Assert.assertTrue(h.get("field1").isEmpty());

        } catch (Exception e) {
            Assert.fail("Exception is not expected: " + e.getMessage());
        }
    }
}

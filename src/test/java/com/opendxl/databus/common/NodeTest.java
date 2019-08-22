/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.common;

import org.junit.Assert;
import org.junit.Test;

public class NodeTest {

    @Test
    public void shouldCreateANodeInstanceSuccessfully() {
        try {
            int idExpected = 1;
            String hostExpected = "localhost";
            int portExpected = 8080;
            String stringExpected = "Node(id = "+idExpected+", host = "+hostExpected+", port = "+portExpected+")";

            Node node = new Node(idExpected,hostExpected,portExpected);
            Assert.assertTrue(idExpected == node.id());
            Assert.assertTrue(hostExpected.equals(node.host()));
            Assert.assertTrue(portExpected == node.port());
            Assert.assertTrue(stringExpected.equals(node.toString()));
        } catch (Exception e) {
            Assert.fail("No exception is expected");
        }
    }

    @Test
    public void shouldComparateTwoNodeInstance() {
        try {
            int idExpected = 1;
            String hostExpected = "localhost";
            int portExpected = 8080;
            String stringExpected = "Node(id = "+idExpected+", host = "+hostExpected+", port = "+portExpected+")";

            Node node1 = new Node(idExpected,hostExpected,portExpected);
            Node node2 = new Node(idExpected,hostExpected,portExpected);
            Node node3 = new Node(0,hostExpected,portExpected);
            Node node4 = new Node(idExpected,"217.0.0.1",portExpected);
            Node node5 = new Node(idExpected,hostExpected,8082);

            Assert.assertTrue(node2.equals(node1));
            Assert.assertFalse(node1.equals(null));
            Assert.assertFalse(node1.equals(node3));
            Assert.assertFalse(node1.equals(node4));
            Assert.assertFalse(node1.equals(node5));
        } catch (Exception e) {
            Assert.fail("No exception is expected");
        }
    }
}

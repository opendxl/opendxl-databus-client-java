/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli;

import broker.ClusterHelper;
import com.opendxl.databus.cli.entity.ExecutionResult;
import com.opendxl.databus.consumer.util.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ClusterDownTest {

    @Test
    public void shouldFailWhenClusterIsDown() throws IOException {
        try {

            // Just in case Cluster is Up
            ClusterHelper.getInstance().stop();

            // Setup CLI parameters
            String args = "--operation produce"
                    + " --brokers " + Constants.KAFKA_HOST.concat(":").concat(Constants.KAFKA_PORT)
                    + " --to-topic topic1"
                    + " --msg Hello_World!";

            // Test
            CommandLineInterface cli = new CommandLineInterface(args.split(" "));
            ExecutionResult executionResult = cli.execute();
            Assert.assertTrue(executionResult.getCode().equals("ERROR"));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

}

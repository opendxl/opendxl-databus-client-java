/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class RegularMessageStructureTest {

    private static final int ID_SIZE = 4;
    private static final int SCHEMA_VERSION = 1;

    @Test
    public void shouldGetAValidRegularStructure() throws IOException {
        try {
            final byte[] message = "Hello World".getBytes();
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(MessageStructureConstant.REGULAR_STRUCTURE_MAGIC_BYTE);
            out.write(ByteBuffer.allocate(ID_SIZE).putInt(SCHEMA_VERSION).array());
            out.write(message);
            final MessageStructure structure = MessageStructureFactory.getStructure(out.toByteArray());
            Assert.assertTrue(structure instanceof RegularMessageStructure);
            Assert.assertTrue(structure.getVersion() == MessageStructureConstant.AVRO_1_VERSION_NUMBER);
            Assert.assertTrue(Arrays.equals(structure.getPayload(), message));
            Assert.assertTrue(new String(structure.getPayload()).equals(new String(message)));
        } catch (Exception e) {
            Assert.fail();
        }
    }
}

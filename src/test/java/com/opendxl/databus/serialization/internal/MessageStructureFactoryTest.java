/*---------------------------------------------------------------------------*
 * Copyright (c) 2024 Musarubra, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.serialization.internal;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class MessageStructureFactoryTest {

    private static final byte MAGIC_BYTE = 0x0;
    private static final int ID_SIZE = 4;
    private static final int SCHEMA_VERSION = 1;

    @Test
    public void shouldReceiveLegacyStructure() {
        String json = "{\"headers\":{\"sourceId\":\"abc\",\"tenantId\":\"tenant10\"}," +
                "\"payloadBase64String\":\"wqFIb2xhISBIdWdvIGFxdcOtLg==\"}";
        byte[] serializedMessage = json.getBytes();
        final MessageStructure structure = MessageStructureFactory.getStructure(serializedMessage);
        Assert.assertTrue(structure instanceof LegacyMessageStructure);
    }

    @Test
    public void shouldReceiveRegularStructure() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(MAGIC_BYTE);
        out.write(ByteBuffer.allocate(ID_SIZE).putInt(SCHEMA_VERSION).array());
        final MessageStructure structure = MessageStructureFactory.getStructure(out.toByteArray());
        Assert.assertTrue(structure instanceof RegularMessageStructure);
    }

    @Test
    public void shouldGetAnRawStructure() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final byte[] bytes = "HelloWorld".getBytes();
        out.write(bytes);
        final MessageStructure structure = MessageStructureFactory.getStructure(out.toByteArray());
        Assert.assertTrue(Arrays.equals(structure.getPayload(),bytes));
    }
}

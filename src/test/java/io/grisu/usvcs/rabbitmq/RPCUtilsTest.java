package io.grisu.usvcs.rabbitmq;

import java.nio.charset.Charset;

import org.junit.Assert;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class RPCUtilsTest {

    @Test
    public void shouldProperlyEncodeAndDecodeTheMessage() throws Exception {
        byte[] payload = "payload".getBytes(Charset.forName("UTF-8"));
        byte[] encoded = RPCUtils.encodeMessage("test", payload);
        final Object[] objects = RPCUtils.decodeMessage(encoded);

        assertEquals("test", (String) objects[0]);
        byte[] decodedPayload = (byte[]) objects[1];
        assertEquals("payload", new String(decodedPayload, "UTF-8"));
    }

    @Test(expected = RuntimeException.class)
    public void shouldntDecodeWrongMessage() {
        RPCUtils.decodeMessage("wrong message".getBytes());
        Assert.fail("Shouldn't reachi this point");
    }

}
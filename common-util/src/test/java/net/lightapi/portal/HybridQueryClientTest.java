package net.lightapi.portal;

import org.junit.jupiter.api.Test;
import org.wildfly.common.Assert;

public class HybridQueryClientTest {
    @Test
    public void testStringFormat() {
        String expect = "{\"host\":\"lightapi.net\",\"service\":\"market\",\"action\":\"getAuthCode\",\"version\":\"0.1.0\",\"data\":{\"host\":\"networknt.com\",\"offset\":0,\"limit\":25}}";
        String host = "networknt.com";
        int offset = 0;
        int limit = 25;
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"market\",\"action\":\"getAuthCode\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        Assert.assertTrue(expect.equals(s));
    }
}

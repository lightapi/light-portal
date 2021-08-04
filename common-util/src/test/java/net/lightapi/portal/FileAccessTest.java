package net.lightapi.portal;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;


public class FileAccessTest {
    @Disabled
    @Test
    public void testWriteFile() {
        String basePath = "/home/steve/data";
        String host = "lightapi.net";
        String id = "blog1";
        String body = "This is the first post";
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(Paths.get(basePath, host, "blog", id + ".md").toFile()), StandardCharsets.UTF_8)) {
            writer.write(body);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

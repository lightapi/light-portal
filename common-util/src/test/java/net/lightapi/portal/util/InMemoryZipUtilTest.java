package net.lightapi.portal.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryZipUtilTest {
    private Map<String, String> base64Files;

    @BeforeEach
    void setUp() {
        base64Files = new HashMap<>();
    }

    @Test
    void testZipBase64Files_SingleFile() throws IOException {
        base64Files.put("test.txt", "SGVsbG8gV29ybGQh"); // "Hello World!" in base64
        byte[] zippedContent = InMemoryZipUtil.zipBase64Files(base64Files);

        assertNotNull(zippedContent);
        assertTrue(zippedContent.length > 0);

        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zippedContent))) {
            var entry = zis.getNextEntry();
            assertNotNull(entry);
            assertEquals("test.txt", entry.getName());

            byte[] content = zis.readAllBytes();
            assertEquals("Hello World!", new String(content, StandardCharsets.UTF_8));

            assertNull(zis.getNextEntry());
        }
    }

    @Test
    void testZipBase64Files_MultipleFiles() throws IOException {
        base64Files.put("file1.txt", "SGVsbG8="); // "Hello" in base64
        base64Files.put("file2.txt", "V29ybGQ="); // "World" in base64

        byte[] zippedContent = InMemoryZipUtil.zipBase64Files(base64Files);

        assertNotNull(zippedContent);
        assertTrue(zippedContent.length > 0);

        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zippedContent))) {
            var entry1 = zis.getNextEntry();
            assertNotNull(entry1);
            assertTrue(entry1.getName().matches("file[12].txt"));

            byte[] content1 = zis.readAllBytes();
            assertTrue(new String(content1, StandardCharsets.UTF_8).matches("Hello|World"));

            var entry2 = zis.getNextEntry();
            assertNotNull(entry2);
            assertTrue(entry2.getName().matches("file[12].txt"));

            byte[] content2 = zis.readAllBytes();
            assertTrue(new String(content2, StandardCharsets.UTF_8).matches("Hello|World"));

            assertNull(zis.getNextEntry());
        }
    }

    @Test
    void testZipBase64Files_EmptyMap() throws IOException {
        byte[] zippedContent = InMemoryZipUtil.zipBase64Files(base64Files);

        assertNotNull(zippedContent);
    }

    @Test
    void testZipBase64Files_InvalidBase64() throws IOException {
        base64Files.put("valid.txt", "SGVsbG8="); // "Hello" in base64
        base64Files.put("invalid.txt", "Not a valid base64 string");

        byte[] zippedContent = InMemoryZipUtil.zipBase64Files(base64Files);

        assertNotNull(zippedContent);
        assertTrue(zippedContent.length > 0);

        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zippedContent))) {
            var entry = zis.getNextEntry();
            assertNotNull(entry);
            assertEquals("valid.txt", entry.getName());

            byte[] content = zis.readAllBytes();
            assertEquals("Hello", new String(content, StandardCharsets.UTF_8));

            assertNull(zis.getNextEntry());
        }
    }

    @Test
    void testZipBase64Files_LargeFile() throws IOException {
        StringBuilder largeContent = new StringBuilder();
        largeContent.append("A".repeat(1000000));
        String base64LargeContent = java.util.Base64.getEncoder().encodeToString(largeContent.toString().getBytes(StandardCharsets.UTF_8));

        base64Files.put("large.txt", base64LargeContent);

        byte[] zippedContent = InMemoryZipUtil.zipBase64Files(base64Files);

        assertNotNull(zippedContent);
        assertTrue(zippedContent.length > 0);
        assertTrue(zippedContent.length < base64LargeContent.length()); // Check if compression occurred

        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zippedContent))) {
            var entry = zis.getNextEntry();
            assertNotNull(entry);
            assertEquals("large.txt", entry.getName());

            byte[] content = zis.readAllBytes();
            assertEquals(1000000, content.length);
            assertEquals(largeContent.toString(), new String(content, StandardCharsets.UTF_8));

            assertNull(zis.getNextEntry());
        }
    }

    @Test
    void testZipBase64Files_JsonFile() throws IOException {
        String jsonContent = "{\"name\":\"John\",\"age\":30,\"city\":\"New York\"}";
        base64Files.put("data.json", Base64.getEncoder().encodeToString(jsonContent.getBytes()));

        byte[] zippedContent = InMemoryZipUtil.zipBase64Files(base64Files);
        assertNotNull(zippedContent);

        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zippedContent))) {
            var entry = zis.getNextEntry();
            assertNotNull(entry);
            assertEquals("data.json", entry.getName());

            String unzippedContent = new String(zis.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(jsonContent, unzippedContent);
        }
    }

    @Test
    void testZipBase64Files_YamlFile() throws IOException {
        String yamlContent = "name: John\nage: 30\ncity: New York";
        base64Files.put("config.yaml", Base64.getEncoder().encodeToString(yamlContent.getBytes()));

        byte[] zippedContent = InMemoryZipUtil.zipBase64Files(base64Files);
        assertNotNull(zippedContent);

        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zippedContent))) {
            var entry = zis.getNextEntry();
            assertNotNull(entry);
            assertEquals("config.yaml", entry.getName());

            String unzippedContent = new String(zis.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(yamlContent, unzippedContent);
        }
    }

    @Test
    void testZipBase64Files_XmlFile() throws IOException {
        String xmlContent = "<root><name>John</name><age>30</age><city>New York</city></root>";
        base64Files.put("data.xml", Base64.getEncoder().encodeToString(xmlContent.getBytes()));

        byte[] zippedContent = InMemoryZipUtil.zipBase64Files(base64Files);
        assertNotNull(zippedContent);

        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zippedContent))) {
            var entry = zis.getNextEntry();
            assertNotNull(entry);
            assertEquals("data.xml", entry.getName());

            String unzippedContent = new String(zis.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(xmlContent, unzippedContent);
        }
    }

    @Test
    void testZipBase64Files_KeyStore() throws Exception {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, "password".toCharArray());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ks.store(baos, "password".toCharArray());

        base64Files.put("keystore.jks", Base64.getEncoder().encodeToString(baos.toByteArray()));

        byte[] zippedContent = InMemoryZipUtil.zipBase64Files(base64Files);
        assertNotNull(zippedContent);

        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zippedContent))) {
            var entry = zis.getNextEntry();
            assertNotNull(entry);
            assertEquals("keystore.jks", entry.getName());

            byte[] unzippedContent = zis.readAllBytes();
            KeyStore unzippedKs = KeyStore.getInstance(KeyStore.getDefaultType());
            unzippedKs.load(new ByteArrayInputStream(unzippedContent), "password".toCharArray());

            assertEquals(ks.size(), unzippedKs.size());
        }
    }

    @Test
    void testZipBase64Files_Certificate() throws Exception {
        String certContent =
            """
                -----BEGIN CERTIFICATE-----
                MIIDkzCCAnugAwIBAgIEUBGbJDANBgkqhkiG9w0BAQsFADB6MQswCQYDVQQGEwJDQTEQMA4GA1UE
                CBMHT250YXJpbzEQMA4GA1UEBxMHVG9yb250bzEmMCQGA1UEChMdTmV0d29yayBOZXcgVGVjaG5v
                bG9naWVzIEluYy4xDDAKBgNVBAsTA0FQSTERMA8GA1UEAxMIU3RldmUgSHUwHhcNMTYwOTIyMjI1
                OTIxWhcNMjYwODAxMjI1OTIxWjB6MQswCQYDVQQGEwJDQTEQMA4GA1UECBMHT250YXJpbzEQMA4G
                A1UEBxMHVG9yb250bzEmMCQGA1UEChMdTmV0d29yayBOZXcgVGVjaG5vbG9naWVzIEluYy4xDDAK
                BgNVBAsTA0FQSTERMA8GA1UEAxMIU3RldmUgSHUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
                AoIBAQCqYfarFwug2DwpG/mmcW77OluaHVNsKEVJ/BptLp5suJAH/Z70SS5pwM4x2QwMOVO2ke8U
                rsAws8allxcuKXrbpVt4evpO1Ly2sFwqB1bjN3+VMp6wcT+tSjzYdVGFpQAYHpeA+OLuoHtQyfpB
                0KCveTEe3KAG33zXDNfGKTGmupZ3ZfmBLINoey/X13rY71ITt67AY78VHUKb+D53MBahCcjJ9YpJ
                UHG+Sd3d4oeXiQcqJCBCVpD97awWARf8WYRIgU1xfCe06wQ3CzH3+GyfozLeu76Ni5PwE1tm7Dhg
                EDSSZo5khmzVzo4G0T2sOeshePc5weZBNRHdHlJA0L0fAgMBAAGjITAfMB0GA1UdDgQWBBT9rnek
                spnrFus5wTszjdzYgKll9TANBgkqhkiG9w0BAQsFAAOCAQEAT8udTfUGBgeWbN6ZAXRI64VsSJj5
                1sNUN1GPDADLxZF6jArKU7LjBNXn9bG5VjJqlx8hQ1SNvi/t7FqBRCUt/3MxDmGZrVZqLY1kZ2e7
                x+5RykbspA8neEUtU8sOr/NP3O5jBjU77EVec9hNNT5zwKLevZNL/Q5mfHoc4GrIAolQvi/5fEqC
                8OMdOIWS6sERgjaeI4tXxQtHDcMo5PeLW0/7t5sgEsadZ+pkdeEMVTmLfgf97bpNNI7KF5uEbYnQ
                NpwCT+NNC5ACmJmKidrfW23kml1C7vr7YzTevw9QuH/hN8l/Rh0fr+iPEVpgN6Zv00ymoKGmjuuW
                owVmdKg/0w==
                -----END CERTIFICATE-----""";

        // Encode the entire certificate content, including BEGIN and END lines
        String base64Cert = Base64.getEncoder().encodeToString(certContent.getBytes(StandardCharsets.UTF_8));
        base64Files.put("cert.crt", base64Cert);

        byte[] zippedContent = InMemoryZipUtil.zipBase64Files(base64Files);
        assertNotNull(zippedContent);

        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zippedContent))) {
            var entry = zis.getNextEntry();
            assertNotNull(entry);
            assertEquals("cert.crt", entry.getName());

            byte[] unzippedContent = zis.readAllBytes();
            String unzippedCertContent = new String(unzippedContent, StandardCharsets.UTF_8);

            // Verify the unzipped content matches the original
            assertEquals(certContent, unzippedCertContent);

            // Now try to generate the certificate
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate cert = (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(unzippedContent));

            assertNotNull(cert);
        }
    }

    @Test
    void testZipBase64Files_MixedFileTypes() throws IOException {
        base64Files.put("data.json", Base64.getEncoder().encodeToString("{\"key\":\"value\"}".getBytes()));
        base64Files.put("config.yaml", Base64.getEncoder().encodeToString("key: value".getBytes()));
        base64Files.put("data.xml", Base64.getEncoder().encodeToString("<root><key>value</key></root>".getBytes()));

        byte[] zippedContent = InMemoryZipUtil.zipBase64Files(base64Files);
        assertNotNull(zippedContent);

        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zippedContent))) {
            int fileCount = 0;
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                assertTrue(entry.getName().matches("data\\.json|config\\.yaml|data\\.xml"));
                fileCount++;
            }
            assertEquals(3, fileCount);
        }
    }
}

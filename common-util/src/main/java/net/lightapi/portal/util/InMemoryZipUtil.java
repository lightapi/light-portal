package net.lightapi.portal.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.zip.ZipOutputStream;

public class InMemoryZipUtil {

    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(InMemoryZipUtil.class);

    private InMemoryZipUtil() {
        // Prevent instantiation
    }

    /**
     * Zips a map of base64 encoded files in memory.
     * Each entry in the map represents a file name and its corresponding base64 encoded content.
     *
     * @param base64Files a map where keys are file names and values are base64 encoded file contents
     * @return a byte array containing the zipped files
     * @throws IOException if an I/O error occurs during zipping
     */
    public static byte[] zipBase64Files(Map<String, String> base64Files) throws IOException {
        // Implementation for zipping base64 files in memory
        try(ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ZipOutputStream zos = new ZipOutputStream(baos)) {
            for (Map.Entry<String, String> entry: base64Files.entrySet()) {
                String fileName = entry.getKey();
                String base64Content = entry.getValue();

                byte[] fileContent;
                try {
                    // Decode the base64 content
                    fileContent = java.util.Base64.getDecoder().decode(base64Content);
                } catch (Exception e) {
                    LOGGER.warn("Unable to decode base64 content for file '{}': {}", fileName, e.getMessage());
                    continue; // Skip files with invalid base64 content
                }

                // Create a zip entry for the file
                zos.putNextEntry(new java.util.zip.ZipEntry(fileName));

                // Write the file content to the zip entry
                zos.write(fileContent);

                // Close the current zip entry
                zos.closeEntry();
            }
            zos.finish();
            return baos.toByteArray();
        }
    }
}

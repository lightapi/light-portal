package net.lightapi.portal;

import com.networknt.config.Config;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

public class PortalUtil {

    private static final HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1) //Good practice to specify the version
            .connectTimeout(Duration.ofSeconds(10)) //Add a connection timeout
            .build();

    public static String readUrl(String url) throws IOException, InterruptedException, URISyntaxException {
        // HttpClient is *not* AutoCloseable.  Do NOT use try-with-resources here.
        // The HttpClient is intended to be long-lived and reused.

        // HttpRequest is a value object. No need to close it.
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(url)) // Use constructor instead of create
                .GET() // Explicitly state the method, good practice.
                .timeout(Duration.ofSeconds(30)) // Add a request timeout.
                .build();

        // HttpResponse is also a value object. No need to close it.
        HttpResponse<String> response = client.send(
                request, HttpResponse.BodyHandlers.ofString()
        );

        // Check the status code.  client.send() can throw an exception,
        // but it can also *return* an error code (4xx, 5xx)
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
            return response.body();
        } else {
            //Handle the error appropriately
            throw new IOException("HTTP request failed with status code: " + response.statusCode() +
                    ", body: " + response.body()); // Include the body in the error, which is useful.
        }
    }

    public static boolean isValidYaml(String yamlString) {
        try {
            Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
            yaml.load(yamlString); // Attempt to load the YAML
            return true; // Parsing succeeded, so it's valid YAML
        } catch (YAMLException e) {
            // Parsing failed, so it's not valid YAML
            System.err.println("Invalid YAML: " + e.getMessage());
            return false;
        }
    }

    public static Map<String, Object> yamlToMap(String yamlString) {
        // Use SafeConstructor for security and LoaderOptions for safety defaults.
        LoaderOptions options = new LoaderOptions();
        Yaml yaml = new Yaml(new SafeConstructor(options));
        Object obj = yaml.load(yamlString);
        if (obj instanceof Map<?, ?>) {
            return (Map<String, Object>) obj;
        } else {
            // If the YAML is not a map, return an empty map.
            return new LinkedHashMap<>();
        }
    }

    public static Number parseNumber(String str) throws NumberFormatException {
        if (str == null || str.trim().isEmpty()) {
            throw new NumberFormatException("Input string is null or empty");
        }
        String trimmed = str.trim();

        // Check if the string represents a floating-point number
        if (trimmed.matches(".*[.eE].*")) {
            try {
                return parseFloatingPoint(trimmed);
            } catch (NumberFormatException e) {
                throw new NumberFormatException("Invalid floating-point number: " + trimmed);
            }
        } else {
            // Handle integer types (Integer, Long, or BigInteger)
            try {
                return parseInteger(trimmed);
            } catch (NumberFormatException e) {
                throw new NumberFormatException("Invalid integer number: " + trimmed);
            }
        }
    }

    private static Number parseFloatingPoint(String str) {
        try {
            return Double.parseDouble(str); // Try Double first
        } catch (NumberFormatException e) {
            return new BigDecimal(str); // Fallback to BigDecimal for precision
        }
    }

    private static Number parseInteger(String str) {
        try {
            return Integer.parseInt(str); // Try Integer first
        } catch (NumberFormatException e1) {
            try {
                return Long.parseLong(str); // Then Long
            } catch (NumberFormatException e2) {
                return new BigInteger(str); // Fallback to BigInteger for large values
            }
        }
    }

}

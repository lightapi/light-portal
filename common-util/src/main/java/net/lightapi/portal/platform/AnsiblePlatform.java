package net.lightapi.portal.platform;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Ansible implementation for starting deployments and querying job status.
 */
public class AnsiblePlatform implements DeploymentPlatformHandler {
    private static final Logger logger = LoggerFactory.getLogger(AnsiblePlatform.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Map<String, Object> startDeployment(Map<String, Object> input) throws Exception {
        // Extract deployment parameters
        String instanceId = (String) input.get("instanceId");
        String platformUrl = (String) input.get("url");
        String token = (String) input.get("token");

        // Build Ansible job launch request
        URL url = new URL(platformUrl + "/api/v2/job_templates/1/launch/");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Authorization", "Bearer " + token);
        conn.setRequestProperty("Content-Type", "application/json");

        // Send payload
        String payload = mapper.writeValueAsString(
            Map.of("extra_vars", Map.of("instance_id", instanceId, "action", "deploy"))
        );
        conn.getOutputStream().write(payload.getBytes(StandardCharsets.UTF_8));

        // Read response to extract jobId
        Map<String, Object> response = mapper.readValue(conn.getInputStream(), Map.class);
        String jobId = String.valueOf(response.get("job"));

        Map<String, Object> result = new HashMap<>();
        result.put("jobId", jobId);
        return result;
    }

    @Override
    public Map<String, Object> queryStatus(Map<String, Object> input) throws Exception {
        String jobId = (String) input.get("jobId");
        String deploymentId = (String) input.get("deploymentId");
        String token = (String) input.get("token");
        String platformUrl = (String) input.get("url");

        // Build Ansible job status query
        URL url = new URL(platformUrl + "/api/v2/jobs/" + jobId + "/");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", "Bearer " + token);

        // Read status response
        Map<String, Object> response = mapper.readValue(conn.getInputStream(), Map.class);
        String status = (String) response.get("status");

        logger.info("Ansible job status for job {} = {}", jobId, status);

        /*
        // Emit Avro DeploymentStatusEvent
        DeploymentStatusEvent event = DeploymentStatusEvent.newBuilder()
                .setDeploymentId(deploymentId)
                .setJobId(jobId)
                .setStatus(status.toUpperCase())
                .setTimestamp(System.currentTimeMillis())
                .build();

        byte[] serialized = serializer.serialize(event);
        HybridCommandStartup.producer.send(
            new ProducerRecord<>("deployment-status", deploymentId.getBytes(StandardCharsets.UTF_8), serialized)
        );
        */

        Map<String, Object> result = new HashMap<>();
        result.put("status", status);
        return result;
    }
}

package net.lightapi.portal.platform;

import java.util.Map;

/**
 * Common interface for deployment platforms (e.g., Ansible, Jenkins).
 * Defines how to start deployments and query job status.
 */
public interface DeploymentPlatformHandler {
    Map<String, Object> startDeployment(Map<String, Object> input) throws Exception;
    Map<String, Object> queryStatus(Map<String, Object> input) throws Exception;
}

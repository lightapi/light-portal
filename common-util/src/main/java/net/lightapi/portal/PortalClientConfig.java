package net.lightapi.portal;

/**
 * This is a centralized configuration for the portal services. The config file can be copied to all
 * hybrid services test resource if it is needed for test cases.
 *
 * This refactor will avoid duplications if we give each hybrid service a config file.
 *
 * @author Steve Hu
 */
public class PortalClientConfig {
    /** Config Name */
    public static final String CONFIG_NAME = "portal-client";

    boolean portalByServiceUrl;
    String portalQueryServiceUrl;
    String portalCommandServiceUrl;
    String portalQueryServiceId;
    String portalCommandServiceId;
    String portalReferenceServiceId;
    String bootstrapToken; // this token will be used to call hybrid-query services if incoming request doesn't have a token.

    /**
     * Default constructor
     */
    public PortalClientConfig() {
    }

    /**
     * Check if portal is by service URL
     * @return true if by service URL
     */
    public boolean isPortalByServiceUrl() {
        return portalByServiceUrl;
    }

    /**
     * Set if portal is by service URL
     * @param portalByServiceUrl boolean
     */
    public void setPortalByServiceUrl(boolean portalByServiceUrl) {
        this.portalByServiceUrl = portalByServiceUrl;
    }

    /**
     * Get portal query service URL
     * @return portal query service URL
     */
    public String getPortalQueryServiceUrl() {
        return portalQueryServiceUrl;
    }

    /**
     * Set portal query service URL
     * @param portalQueryServiceUrl String
     */
    public void setPortalQueryServiceUrl(String portalQueryServiceUrl) {
        this.portalQueryServiceUrl = portalQueryServiceUrl;
    }

    /**
     * Get portal command service URL
     * @return portal command service URL
     */
    public String getPortalCommandServiceUrl() {
        return portalCommandServiceUrl;
    }

    /**
     * Set portal command service URL
     * @param portalCommandServiceUrl String
     */
    public void setPortalCommandServiceUrl(String portalCommandServiceUrl) {
        this.portalCommandServiceUrl = portalCommandServiceUrl;
    }

    /**
     * Get portal query service ID
     * @return portal query service ID
     */
    public String getPortalQueryServiceId() {
        return portalQueryServiceId;
    }

    /**
     * Set portal query service ID
     * @param portalQueryServiceId String
     */
    public void setPortalQueryServiceId(String portalQueryServiceId) {
        this.portalQueryServiceId = portalQueryServiceId;
    }

    /**
     * Get portal command service ID
     * @return portal command service ID
     */
    public String getPortalCommandServiceId() {
        return portalCommandServiceId;
    }

    /**
     * Set portal command service ID
     * @param portalCommandServiceId String
     */
    public void setPortalCommandServiceId(String portalCommandServiceId) {
        this.portalCommandServiceId = portalCommandServiceId;
    }

    /**
     * Get portal reference service ID
     * @return portal reference service ID
     */
    public String getPortalReferenceServiceId() {
        return portalReferenceServiceId;
    }

    /**
     * Set portal reference service ID
     * @param portalReferenceServiceId String
     */
    public void setPortalReferenceServiceId(String portalReferenceServiceId) {
        this.portalReferenceServiceId = portalReferenceServiceId;
    }

    /**
     * Get bootstrap token
     * @return bootstrap token
     */
    public String getBootstrapToken() {
        return bootstrapToken;
    }

    /**
     * Set bootstrap token
     * @param bootstrapToken String
     */
    public void setBootstrapToken(String bootstrapToken) {
        this.bootstrapToken = bootstrapToken;
    }
}

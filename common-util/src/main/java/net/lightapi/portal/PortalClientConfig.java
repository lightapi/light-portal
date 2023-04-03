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
    public static final String CONFIG_NAME = "portal-client";

    boolean portalByServiceUrl;
    String portalQueryServiceUrl;
    String portalCommandServiceUrl;
    String portalQueryServiceId;
    String portalCommandServiceId;
    String portalReferenceServiceId;
    String bootstrapToken; // this token will be used to call hybrid-query services if incoming request doesn't have a token.

    public PortalClientConfig() {
    }

    public boolean isPortalByServiceUrl() {
        return portalByServiceUrl;
    }

    public void setPortalByServiceUrl(boolean portalByServiceUrl) {
        this.portalByServiceUrl = portalByServiceUrl;
    }

    public String getPortalQueryServiceUrl() {
        return portalQueryServiceUrl;
    }

    public void setPortalQueryServiceUrl(String portalQueryServiceUrl) {
        this.portalQueryServiceUrl = portalQueryServiceUrl;
    }

    public String getPortalCommandServiceUrl() {
        return portalCommandServiceUrl;
    }

    public void setPortalCommandServiceUrl(String portalCommandServiceUrl) {
        this.portalCommandServiceUrl = portalCommandServiceUrl;
    }

    public String getPortalQueryServiceId() {
        return portalQueryServiceId;
    }

    public void setPortalQueryServiceId(String portalQueryServiceId) {
        this.portalQueryServiceId = portalQueryServiceId;
    }

    public String getPortalCommandServiceId() {
        return portalCommandServiceId;
    }

    public void setPortalCommandServiceId(String portalCommandServiceId) {
        this.portalCommandServiceId = portalCommandServiceId;
    }

    public String getPortalReferenceServiceId() {
        return portalReferenceServiceId;
    }

    public void setPortalReferenceServiceId(String portalReferenceServiceId) {
        this.portalReferenceServiceId = portalReferenceServiceId;
    }

    public String getBootstrapToken() {
        return bootstrapToken;
    }

    public void setBootstrapToken(String bootstrapToken) {
        this.bootstrapToken = bootstrapToken;
    }
}

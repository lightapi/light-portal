package net.lightapi.portal;

import com.networknt.config.Config;

import java.util.Map;

public class PortalUtil {
    public static PortalConfig config = (PortalConfig) Config.getInstance().getJsonObjectConfig(PortalConfig.CONFIG_NAME, PortalConfig.class);
}

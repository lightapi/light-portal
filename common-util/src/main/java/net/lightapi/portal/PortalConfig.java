package net.lightapi.portal;

/**
 * This is a centralized configuration for the portal services. The config file can be copied to all
 * hybrid services test resource if it is needed for test cases.
 *
 * This refactor will avoid duplications if we give each hybrid service a config file.
 *
 * @author Steve Hu
 */
public class PortalConfig {
    public static final String CONFIG_NAME = "portal";

    String topic;
    boolean multitenancy;
    boolean sendEmail;
    String cmdHost;
    String cmdPath;
    String resetHost;
    String portalHost;
    String admin;
    String dataPath;
    String scheduleApplicationId;

    public PortalConfig() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isMultitenancy() {
        return multitenancy;
    }

    public void setMultitenancy(boolean multitenancy) {
        this.multitenancy = multitenancy;
    }

    public boolean isSendEmail() {
        return sendEmail;
    }

    public void setSendEmail(boolean sendEmail) {
        this.sendEmail = sendEmail;
    }

    public String getCmdHost() {
        return cmdHost;
    }

    public void setCmdHost(String cmdHost) {
        this.cmdHost = cmdHost;
    }

    public String getCmdPath() {
        return cmdPath;
    }

    public void setCmdPath(String cmdPath) {
        this.cmdPath = cmdPath;
    }

    public String getResetHost() {
        return resetHost;
    }

    public void setResetHost(String resetHost) {
        this.resetHost = resetHost;
    }

    public String getPortalHost() {
        return portalHost;
    }

    public void setPortalHost(String portalHost) {
        this.portalHost = portalHost;
    }

    public String getAdmin() {
        return admin;
    }

    public void setAdmin(String admin) {
        this.admin = admin;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public String getScheduleApplicationId() {
        return scheduleApplicationId;
    }

    public void setScheduleApplicationId(String scheduleApplicationId) {
        this.scheduleApplicationId = scheduleApplicationId;
    }
}

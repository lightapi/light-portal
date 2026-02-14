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
    /** Config Name */
    public static final String CONFIG_NAME = "portal";

    String topic;
    boolean sendEmail;
    String cmdHost;
    String cmdPath;
    String resetHost;
    String portalHost;
    String admin;
    String dataPath;
    String scheduleApplicationId;

    /**
     * Default constructor
     */
    public PortalConfig() {
    }

    /**
     * Get the topic
     * @return topic String
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Set the topic
     * @param topic String
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * Get the send email flag
     * @return sendEmail boolean
     */
    public boolean isSendEmail() {
        return sendEmail;
    }

    /**
     * Set the send email flag
     * @param sendEmail boolean
     */
    public void setSendEmail(boolean sendEmail) {
        this.sendEmail = sendEmail;
    }

    /**
     * Get the command host
     * @return cmdHost String
     */
    public String getCmdHost() {
        return cmdHost;
    }

    /**
     * Set the command host
     * @param cmdHost String
     */
    public void setCmdHost(String cmdHost) {
        this.cmdHost = cmdHost;
    }

    /**
     * Get the command path
     * @return cmdPath String
     */
    public String getCmdPath() {
        return cmdPath;
    }

    /**
     * Set the command path
     * @param cmdPath String
     */
    public void setCmdPath(String cmdPath) {
        this.cmdPath = cmdPath;
    }

    /**
     * Get the reset host
     * @return resetHost String
     */
    public String getResetHost() {
        return resetHost;
    }

    /**
     * Set the reset host
     * @param resetHost String
     */
    public void setResetHost(String resetHost) {
        this.resetHost = resetHost;
    }

    /**
     * Get the portal host
     * @return portalHost String
     */
    public String getPortalHost() {
        return portalHost;
    }

    /**
     * Set the portal host
     * @param portalHost String
     */
    public void setPortalHost(String portalHost) {
        this.portalHost = portalHost;
    }

    /**
     * Get the admin
     * @return admin String
     */
    public String getAdmin() {
        return admin;
    }

    /**
     * Set the admin
     * @param admin String
     */
    public void setAdmin(String admin) {
        this.admin = admin;
    }

    /**
     * Get the data path
     * @return dataPath String
     */
    public String getDataPath() {
        return dataPath;
    }

    /**
     * Set the data path
     * @param dataPath String
     */
    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    /**
     * Get the schedule application id
     * @return scheduleApplicationId String
     */
    public String getScheduleApplicationId() {
        return scheduleApplicationId;
    }

    /**
     * Set the schedule application id
     * @param scheduleApplicationId String
     */
    public void setScheduleApplicationId(String scheduleApplicationId) {
        this.scheduleApplicationId = scheduleApplicationId;
    }
}

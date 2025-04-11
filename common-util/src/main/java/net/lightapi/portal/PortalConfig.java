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
    String userApplicationId;
    String userIdTopic;
    String taijiTopic;
    String referenceTopic;
    String notificationApplicationId;
    String notificationTopic;
    String nonceApplicationId;
    String nonceTopic;
    String marketApplicationId;
    String maprootApplicationId;
    String scheduleApplicationId;
    String scheduleTopic;


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

    public String getUserApplicationId() {
        return userApplicationId;
    }

    public void setUserApplicationId(String userApplicationId) {
        this.userApplicationId = userApplicationId;
    }

    public String getUserIdTopic() {
        return userIdTopic;
    }

    public void setUserIdTopic(String userIdTopic) {
        this.userIdTopic = userIdTopic;
    }

    public String getTaijiTopic() {
        return taijiTopic;
    }

    public void setTaijiTopic(String taijiTopic) {
        this.taijiTopic = taijiTopic;
    }

    public String getReferenceTopic() {
        return referenceTopic;
    }

    public void setReferenceTopic(String referenceTopic) {
        this.referenceTopic = referenceTopic;
    }

    public String getNotificationApplicationId() {
        return notificationApplicationId;
    }

    public void setNotificationApplicationId(String notificationApplicationId) {
        this.notificationApplicationId = notificationApplicationId;
    }

    public String getNotificationTopic() {
        return notificationTopic;
    }

    public void setNotificationTopic(String notificationTopic) {
        this.notificationTopic = notificationTopic;
    }

    public String getNonceApplicationId() {
        return nonceApplicationId;
    }

    public void setNonceApplicationId(String nonceApplicationId) {
        this.nonceApplicationId = nonceApplicationId;
    }

    public String getNonceTopic() {
        return nonceTopic;
    }

    public void setNonceTopic(String nonceTopic) {
        this.nonceTopic = nonceTopic;
    }

    public String getMarketApplicationId() {
        return marketApplicationId;
    }

    public void setMarketApplicationId(String marketApplicationId) {
        this.marketApplicationId = marketApplicationId;
    }

    public String getMaprootApplicationId() {
        return maprootApplicationId;
    }

    public void setMaprootApplicationId(String maprootApplicationId) {
        this.maprootApplicationId = maprootApplicationId;
    }

    public String getScheduleApplicationId() {
        return scheduleApplicationId;
    }

    public void setScheduleApplicationId(String scheduleApplicationId) {
        this.scheduleApplicationId = scheduleApplicationId;
    }

    public String getScheduleTopic() {
        return scheduleTopic;
    }

    public void setScheduleTopic(String scheduleTopic) {
        this.scheduleTopic = scheduleTopic;
    }
}

package net.lightapi.portal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class PortalUtilTest {
    @Test
    @Disabled
    public void testYamlToMap() {
        String s = "---\n" +
                "host: \"lightapi.net\"\n" +
                "service: \"user\"\n" +
                "schemas:\n" +
                "  queryUserByEmailRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      email:\n" +
                "        type: \"string\"\n" +
                "        description: \"user email address\"\n" +
                "    required:\n" +
                "    - \"email\"\n" +
                "  queryUserByIdRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      userId:\n" +
                "        type: \"string\"\n" +
                "        description: \"A unique user id\"\n" +
                "    required:\n" +
                "    - \"userId\"\n" +
                "  queryUserByTypeEntityIdRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      userType:\n" +
                "        type: \"string\"\n" +
                "        description: \"User Type\"\n" +
                "      entityId:\n" +
                "        type: \"string\"\n" +
                "        description: \"A unique user id for the type\"\n" +
                "    required:\n" +
                "    - \"userType\"\n" +
                "    - \"entityId\"\n" +
                "  getNonceByUserIdRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      userId:\n" +
                "        type: \"string\"\n" +
                "        description: \"User Id\"\n" +
                "    required:\n" +
                "    - \"userId\"\n" +
                "  getUserLabelRequest:\n" +
                "    title: \"User Label\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      hostId:\n" +
                "        type: \"string\"\n" +
                "        description: \"Host Id\"\n" +
                "    required:\n" +
                "    - \"hostId\"\n" +
                "  queryUserByWalletRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      taijiWallet:\n" +
                "        type: \"string\"\n" +
                "        description: \"user taiji wallet address\"\n" +
                "    required:\n" +
                "    - \"taijiWallet\"\n" +
                "  loginUserRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      email:\n" +
                "        type: \"string\"\n" +
                "      password:\n" +
                "        type: \"string\"\n" +
                "    required:\n" +
                "    - \"email\"\n" +
                "    - \"password\"\n" +
                "  getPrivateMessageRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      email:\n" +
                "        type: \"string\"\n" +
                "    required:\n" +
                "    - \"email\"\n" +
                "  getNotificationRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      hostId:\n" +
                "        type: \"string\"\n" +
                "      userId:\n" +
                "        type: \"string\"\n" +
                "    required:\n" +
                "    - \"hostId\"\n" +
                "    - \"userId\"\n" +
                "  listUserByHostIdRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      hostId:\n" +
                "        type: \"string\"\n" +
                "        description: \"user hostId\"\n" +
                "      offset:\n" +
                "        type: \"integer\"\n" +
                "        description: \"Record Offset\"\n" +
                "      limit:\n" +
                "        type: \"integer\"\n" +
                "        description: \"Record Limit\"\n" +
                "      email:\n" +
                "        type: \"string\"\n" +
                "        description: \"Email\"\n" +
                "      language:\n" +
                "        type: \"string\"\n" +
                "        description: \"Language\"\n" +
                "      userType:\n" +
                "        type: \"string\"\n" +
                "        description: \"User Type\"\n" +
                "      entityId:\n" +
                "        type: \"string\"\n" +
                "        description: \"Entity Id\"\n" +
                "      referralId:\n" +
                "        type: \"string\"\n" +
                "        description: \"Referral Id\"\n" +
                "      managerId:\n" +
                "        type: \"string\"\n" +
                "        description: \"Manager Id\"\n" +
                "      firstName:\n" +
                "        type: \"string\"\n" +
                "        description: \"First Name\"\n" +
                "      lastName:\n" +
                "        type: \"string\"\n" +
                "        description: \"Last Name\"\n" +
                "      phoneNumber:\n" +
                "        type: \"string\"\n" +
                "        description: \"Phone Number\"\n" +
                "      gender:\n" +
                "        type: \"string\"\n" +
                "        description: \"Gender\"\n" +
                "      birthday:\n" +
                "        type: \"string\"\n" +
                "        description: \"Birthday\"\n" +
                "      country:\n" +
                "        type: \"string\"\n" +
                "        description: \"Country\"\n" +
                "      province:\n" +
                "        type: \"string\"\n" +
                "        description: \"Province\"\n" +
                "      city:\n" +
                "        type: \"string\"\n" +
                "        description: \"City\"\n" +
                "      address:\n" +
                "        type: \"string\"\n" +
                "        description: \"Address\"\n" +
                "      postCode:\n" +
                "        type: \"string\"\n" +
                "        description: \"Post Code\"\n" +
                "      verified:\n" +
                "        type: \"boolean\"\n" +
                "        description: \"Verified\"\n" +
                "      locked:\n" +
                "        type: \"boolean\"\n" +
                "        description: \"Locked\"\n" +
                "    required:\n" +
                "    - \"hostId\"\n" +
                "    - \"offset\"\n" +
                "    - \"limit\"\n" +
                "  getPaymentRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      email:\n" +
                "        type: \"string\"\n" +
                "        description: \"Merchant user email address\"\n" +
                "    required:\n" +
                "    - \"email\"\n" +
                "  getPaymentByIdRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      userId:\n" +
                "        type: \"string\"\n" +
                "        description: \"A unique user id\"\n" +
                "    required:\n" +
                "    - \"userId\"\n" +
                "  getClientTokenRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      userId:\n" +
                "        type: \"string\"\n" +
                "        description: \"Merchant userId\"\n" +
                "      merchantId:\n" +
                "        type: \"string\"\n" +
                "        description: \"Merchant Id\"\n" +
                "    required:\n" +
                "    - \"userId\"\n" +
                "    - \"merchantId\"\n" +
                "  getCustomerOrderRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      email:\n" +
                "        type: \"string\"\n" +
                "        description: \"Customer Email\"\n" +
                "      offset:\n" +
                "        type: \"integer\"\n" +
                "        description: \"Record Offset\"\n" +
                "      limit:\n" +
                "        type: \"integer\"\n" +
                "        description: \"Record Limit\"\n" +
                "    required:\n" +
                "    - \"email\"\n" +
                "    - \"offset\"\n" +
                "    - \"limit\"\n" +
                "  getMerchantOrderRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      email:\n" +
                "        type: \"string\"\n" +
                "        description: \"Merchant Email\"\n" +
                "      status:\n" +
                "        type: \"string\"\n" +
                "        description: \"Order Status\"\n" +
                "      offset:\n" +
                "        type: \"integer\"\n" +
                "        description: \"Record Offset\"\n" +
                "      limit:\n" +
                "        type: \"integer\"\n" +
                "        description: \"Record Limit\"\n" +
                "    required:\n" +
                "    - \"email\"\n" +
                "    - \"status\"\n" +
                "    - \"offset\"\n" +
                "    - \"limit\"\n" +
                "  getReferenceRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      name:\n" +
                "        type: \"string\"\n" +
                "        description: \"Table name\"\n" +
                "      lang:\n" +
                "        type: \"string\"\n" +
                "        description: \"Label language\"\n" +
                "      rela:\n" +
                "        type: \"string\"\n" +
                "        description: \"Relationship name\"\n" +
                "      from:\n" +
                "        type: \"string\"\n" +
                "        description: \"From value in rela\"\n" +
                "    required:\n" +
                "    - \"name\"\n" +
                "  getHostRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      offset:\n" +
                "        type: \"integer\"\n" +
                "        description: \"Record Offset\"\n" +
                "      limit:\n" +
                "        type: \"integer\"\n" +
                "        description: \"Record Limit\"\n" +
                "    required:\n" +
                "    - \"offset\"\n" +
                "    - \"limit\"\n" +
                "  getRolesByEmailRequest:\n" +
                "    title: \"Service\"\n" +
                "    type: \"object\"\n" +
                "    properties:\n" +
                "      email:\n" +
                "        type: \"string\"\n" +
                "        description: \"Merchant Email\"\n" +
                "    required:\n" +
                "    - \"email\"\n" +
                "action:\n" +
                "- name: \"queryUserByEmail\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"QueryUserByEmail\"\n" +
                "  scope: \"portal.r\"\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/queryUserByEmailRequest\"\n" +
                "    example:\n" +
                "      email: \"test@example.com\"\n" +
                "- name: \"queryUserById\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"QueryUserById\"\n" +
                "  scope: \"portal.r\"\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/queryUserByIdRequest\"\n" +
                "    example:\n" +
                "      userId: \"user123\"\n" +
                "- name: \"queryUserByTypeEntityId\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"QueryUserByTypeEntityId\"\n" +
                "  scope: \"portal.r\"\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/queryUserByTypeEntityIdRequest\"\n" +
                "    example:\n" +
                "      userType: \"C\"\n" +
                "      entityId: \"cust123\"\n" +
                "- name: \"getNonceByUserId\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"GetNonceByUserId\"\n" +
                "  skipAuth: true\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/getNonceByUserIdRequest\"\n" +
                "    example:\n" +
                "      userId: \"user123\"\n" +
                "- name: \"getUserLabel\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"GetUserLabel\"\n" +
                "  skipAuth: true\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/getUserLabelRequest\"\n" +
                "    example:\n" +
                "      hostId: \"host123\"\n" +
                "- name: \"queryUserByWallet\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"QueryUserByWallet\"\n" +
                "  scope: \"portal.r\"\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/queryUserByWalletRequest\"\n" +
                "    example:\n" +
                "      taijiWallet: \"0x1234567890abcdef\"\n" +
                "- name: \"loginUser\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"LoginUser\"\n" +
                "  skipAuth: true\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/loginUserRequest\"\n" +
                "    example:\n" +
                "      email: \"test@example.com\"\n" +
                "      password: \"password123\"\n" +
                "- name: \"getPrivateMessage\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"GetPrivateMessage\"\n" +
                "  scope: \"portal.r\"\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/getPrivateMessageRequest\"\n" +
                "    example:\n" +
                "      email: \"test@example.com\"\n" +
                "- name: \"getNotification\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"GetNotification\"\n" +
                "  scope: \"portal.r\"\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/getNotificationRequest\"\n" +
                "    example:\n" +
                "      email: \"test@example.com\"\n" +
                "- name: \"listUserByHostId\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"ListUserByHostId\"\n" +
                "  scope: \"portal.r\"\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/listUserByHostIdRequest\"\n" +
                "    example:\n" +
                "      hostId: \"host123\"\n" +
                "      offset: 0\n" +
                "      limit: 10\n" +
                "- name: \"getPayment\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"GetPayment\"\n" +
                "  scope: \"portal.r\"\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/getPaymentRequest\"\n" +
                "    example:\n" +
                "      email: \"test@example.com\"\n" +
                "- name: \"getPaymentById\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"GetPaymentById\"\n" +
                "  scope: \"portal.r\"\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/getPaymentByIdRequest\"\n" +
                "    example:\n" +
                "      userId: \"user123\"\n" +
                "- name: \"getClientToken\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"GetClientToken\"\n" +
                "  scope: \"portal.r\"\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/getClientTokenRequest\"\n" +
                "    example:\n" +
                "      userId: \"user123\"\n" +
                "      merchantId: \"merchant123\"\n" +
                "- name: \"getCustomerOrder\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"GetCustomerOrder\"\n" +
                "  scope: \"portal.r\"\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/getCustomerOrderRequest\"\n" +
                "    example:\n" +
                "      email: \"test@example.com\"\n" +
                "      offset: 0\n" +
                "      limit: 10\n" +
                "- name: \"getMerchantOrder\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"GetMerchantOrder\"\n" +
                "  scope: \"portal.r\"\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/getMerchantOrderRequest\"\n" +
                "    example:\n" +
                "      email: \"test@example.com\"\n" +
                "      status: \"pending\"\n" +
                "      offset: 0\n" +
                "      limit: 10\n" +
                "- name: \"getReference\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"GetReference\"\n" +
                "  skipAuth: true\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/getReferenceRequest\"\n" +
                "    example:\n" +
                "      name: \"countries\"\n" +
                "      lang: \"en\"\n" +
                "      rela: \"region\"\n" +
                "      from: \"US\"\n" +
                "- name: \"listHost\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"ListHost\"\n" +
                "  skipAuth: true\n" +
                "- name: \"getHost\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"GetHost\"\n" +
                "  scope: \"portal.r\"\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/getHostRequest\"\n" +
                "    example:\n" +
                "      offset: 0\n" +
                "      limit: 10\n" +
                "- name: \"getRolesByEmail\"\n" +
                "  version: \"0.1.0\"\n" +
                "  handler: \"GetRolesByEmail\"\n" +
                "  scope: \"portal.r\"\n" +
                "  request:\n" +
                "    schema:\n" +
                "      $ref: \"#/schemas/getRolesByEmailRequest\"\n" +
                "    example:\n" +
                "      email: \"test@example.com\"\n";
        Map<String, Object> map = PortalUtil.yamlToMap(s);
        // System.out.println(map);
    }
}

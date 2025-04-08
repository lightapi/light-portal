package net.lightapi.portal;

public class PortalConstants {

    // order event handling
    public static final String ORDER_CONFIRMED = "Confirmed";
    public static final String ORDER_CANCELLED = "Cancelled";
    public static final String ORDER_DELIVERED = "Delivered";

    // fields
    public static final String GLOBAL_FLAG = "globalFlag";
    public static final String HOST_ID = "hostId";

    // roles
    public static final String ADMIN_ROLE = "admin";
    public static final String USER_ROLE = "user";
    public static final String GUEST_ROLE = "guest";
    public static final String ANONYMOUS_ROLE = "anonymous";
    public static final String HOST_ADMIN_ROLE = "hostAdmin";

    // event types
    public static final String DATA = "data";
    public static final String NONCE = "nonce";

    // --- User Events ---
    public static final String USER_CREATED_EVENT = "UserCreatedEvent";
    public static final String SOCIAL_USER_CREATED_EVENT = "SocialUserCreatedEvent";
    public static final String USER_CONFIRMED_EVENT = "UserConfirmedEvent";
    public static final String USER_VERIFIED_EVENT = "UserVerifiedEvent";
    public static final String USER_UPDATED_EVENT = "UserUpdatedEvent";
    public static final String PASSWORD_FORGOT_EVENT = "PasswordForgotEvent";
    public static final String PASSWORD_RESET_EVENT = "PasswordResetEvent";
    public static final String PASSWORD_CHANGED_EVENT = "PasswordChangedEvent";
    public static final String USER_DELETED_EVENT = "UserDeletedEvent";
    public static final String USER_ROLES_UPDATED_EVENT = "UserRolesUpdatedEvent";
    public static final String USER_LOCKED_EVENT = "UserLockedEvent";
    public static final String USER_UNLOCKED_EVENT = "UserUnlockedEvent";
    public static final String PRIVATE_MESSAGE_SENT_EVENT = "PrivateMessageSentEvent";
    public static final String PAYMENT_UPDATED_EVENT = "PaymentUpdatedEvent";
    public static final String PAYMENT_DELETED_EVENT = "PaymentDeletedEvent";
    public static final String ORDER_CREATED_EVENT = "OrderCreatedEvent";
    public static final String ORDER_CANCELLED_EVENT = "OrderCancelledEvent";
    public static final String ORDER_DELIVERED_EVENT = "OrderDeliveredEvent";

    // --- Host Events ---
    public static final String ORG_CREATED_EVENT = "OrgCreatedEvent";
    public static final String ORG_UPDATED_EVENT = "OrgUpdatedEvent";
    public static final String ORG_DELETED_EVENT = "OrgDeletedEvent";
    public static final String HOST_CREATED_EVENT = "HostCreatedEvent";
    public static final String HOST_UPDATED_EVENT = "HostUpdatedEvent";
    public static final String HOST_DELETED_EVENT = "HostDeletedEvent";
    public static final String HOST_SWITCHED_EVENT = "HostSwitchedEvent";


    // --- Reference Table Events ---
    public static final String REF_TABLE_CREATED_EVENT = "RefTableCreatedEvent";
    public static final String REF_TABLE_UPDATED_EVENT = "RefTableUpdatedEvent";
    public static final String REF_TABLE_DELETED_EVENT = "RefTableDeletedEvent";
    public static final String REF_VALUE_CREATED_EVENT = "RefValueCreatedEvent";
    public static final String REF_VALUE_UPDATED_EVENT = "RefValueUpdatedEvent";
    public static final String REF_VALUE_DELETED_EVENT = "RefValueDeletedEvent";
    public static final String REF_LOCALE_CREATED_EVENT = "RefLocaleCreatedEvent";
    public static final String REF_LOCALE_UPDATED_EVENT = "RefLocaleUpdatedEvent";
    public static final String REF_LOCALE_DELETED_EVENT = "RefLocaleDeletedEvent";
    public static final String REF_RELATION_TYPE_CREATED_EVENT = "RefRelationTypeCreatedEvent";
    public static final String REF_RELATION_TYPE_UPDATED_EVENT = "RefRelationTypeUpdatedEvent";
    public static final String REF_RELATION_TYPE_DELETED_EVENT = "RefRelationTypeDeletedEvent";
    public static final String REF_RELATION_CREATED_EVENT = "RefRelationCreatedEvent";
    public static final String REF_RELATION_UPDATED_EVENT = "RefRelationUpdatedEvent";
    public static final String REF_RELATION_DELETED_EVENT = "RefRelationDeletedEvent";


    // --- Attribute Events ---
    public static final String ATTRIBUTE_CREATED_EVENT = "AttributeCreatedEvent";
    public static final String ATTRIBUTE_UPDATED_EVENT = "AttributeUpdatedEvent";
    public static final String ATTRIBUTE_DELETED_EVENT = "AttributeDeletedEvent";
    public static final String ATTRIBUTE_PERMISSION_CREATED_EVENT = "AttributePermissionCreatedEvent";
    public static final String ATTRIBUTE_PERMISSION_UPDATED_EVENT = "AttributePermissionUpdatedEvent";
    public static final String ATTRIBUTE_PERMISSION_DELETED_EVENT = "AttributePermissionDeletedEvent";
    public static final String ATTRIBUTE_USER_CREATED_EVENT = "AttributeUserCreatedEvent";
    public static final String ATTRIBUTE_USER_UPDATED_EVENT = "AttributeUserUpdatedEvent";
    public static final String ATTRIBUTE_USER_DELETED_EVENT = "AttributeUserDeletedEvent";
    public static final String ATTRIBUTE_ROW_FILTER_CREATED_EVENT = "AttributeRowFilterCreatedEvent";
    public static final String ATTRIBUTE_ROW_FILTER_UPDATED_EVENT = "AttributeRowFilterUpdatedEvent";
    public static final String ATTRIBUTE_ROW_FILTER_DELETED_EVENT = "AttributeRowFilterDeletedEvent";
    public static final String ATTRIBUTE_COL_FILTER_CREATED_EVENT = "AttributeColFilterCreatedEvent";
    public static final String ATTRIBUTE_COL_FILTER_UPDATED_EVENT = "AttributeColFilterUpdatedEvent";
    public static final String ATTRIBUTE_COL_FILTER_DELETED_EVENT = "AttributeColFilterDeletedEvent";
    // --- Group Events ---
    public static final String GROUP_CREATED_EVENT = "GroupCreatedEvent";
    public static final String GROUP_UPDATED_EVENT = "GroupUpdatedEvent";
    public static final String GROUP_DELETED_EVENT = "GroupDeletedEvent";
    public static final String GROUP_PERMISSION_CREATED_EVENT = "GroupPermissionCreatedEvent";
    public static final String GROUP_PERMISSION_DELETED_EVENT = "GroupPermissionDeletedEvent";
    public static final String GROUP_USER_CREATED_EVENT = "GroupUserCreatedEvent";
    public static final String GROUP_USER_UPDATED_EVENT = "GroupUserUpdatedEvent";
    public static final String GROUP_USER_DELETED_EVENT = "GroupUserDeletedEvent";
    public static final String GROUP_ROW_FILTER_CREATED_EVENT = "GroupRowFilterCreatedEvent";
    public static final String GROUP_ROW_FILTER_UPDATED_EVENT = "GroupRowFilterUpdatedEvent";
    public static final String GROUP_ROW_FILTER_DELETED_EVENT = "GroupRowFilterDeletedEvent";
    public static final String GROUP_COL_FILTER_CREATED_EVENT = "GroupColFilterCreatedEvent";
    public static final String GROUP_COL_FILTER_UPDATED_EVENT = "GroupColFilterUpdatedEvent";
    public static final String GROUP_COL_FILTER_DELETED_EVENT = "GroupColFilterDeletedEvent";
    // --- Role Events ---
    public static final String ROLE_CREATED_EVENT = "RoleCreatedEvent";
    public static final String ROLE_UPDATED_EVENT = "RoleUpdatedEvent";
    public static final String ROLE_DELETED_EVENT = "RoleDeletedEvent";
    public static final String ROLE_PERMISSION_CREATED_EVENT = "RolePermissionCreatedEvent";
    public static final String ROLE_PERMISSION_DELETED_EVENT = "RolePermissionDeletedEvent";
    public static final String ROLE_USER_CREATED_EVENT = "RoleUserCreatedEvent";
    public static final String ROLE_USER_UPDATED_EVENT = "RoleUserUpdatedEvent";
    public static final String ROLE_USER_DELETED_EVENT = "RoleUserDeletedEvent";
    public static final String ROLE_ROW_FILTER_CREATED_EVENT = "RoleRowFilterCreatedEvent";
    public static final String ROLE_ROW_FILTER_UPDATED_EVENT = "RoleRowFilterUpdatedEvent";
    public static final String ROLE_ROW_FILTER_DELETED_EVENT = "RoleRowFilterDeletedEvent";
    public static final String ROLE_COL_FILTER_CREATED_EVENT = "RoleColFilterCreatedEvent";
    public static final String ROLE_COL_FILTER_UPDATED_EVENT = "RoleColFilterUpdatedEvent";
    public static final String ROLE_COL_FILTER_DELETED_EVENT = "RoleColFilterDeletedEvent";
    // --- Position Events ---
    public static final String POSITION_CREATED_EVENT = "PositionCreatedEvent";
    public static final String POSITION_UPDATED_EVENT = "PositionUpdatedEvent";
    public static final String POSITION_DELETED_EVENT = "PositionDeletedEvent";
    public static final String POSITION_PERMISSION_CREATED_EVENT = "PositionPermissionCreatedEvent";
    public static final String POSITION_PERMISSION_DELETED_EVENT = "PositionPermissionDeletedEvent";
    public static final String POSITION_USER_CREATED_EVENT = "PositionUserCreatedEvent";
    public static final String POSITION_USER_UPDATED_EVENT = "PositionUserUpdatedEvent";
    public static final String POSITION_USER_DELETED_EVENT = "PositionUserDeletedEvent";
    public static final String POSITION_ROW_FILTER_CREATED_EVENT = "PositionRowFilterCreatedEvent";
    public static final String POSITION_ROW_FILTER_UPDATED_EVENT = "PositionRowFilterUpdatedEvent";
    public static final String POSITION_ROW_FILTER_DELETED_EVENT = "PositionRowFilterDeletedEvent";
    public static final String POSITION_COL_FILTER_CREATED_EVENT = "PositionColFilterCreatedEvent";
    public static final String POSITION_COL_FILTER_UPDATED_EVENT = "PositionColFilterUpdatedEvent";
    public static final String POSITION_COL_FILTER_DELETED_EVENT = "PositionColFilterDeletedEvent";

    // --- Rule Events ---
    public static final String RULE_CREATED_EVENT = "RuleCreatedEvent";
    public static final String RULE_UPDATED_EVENT = "RuleUpdatedEvent";
    public static final String RULE_DELETED_EVENT = "RuleDeletedEvent";

    // --- Category Events ---
    public static final String CATEGORY_CREATED_EVENT = "CategoryCreatedEvent";
    public static final String CATEGORY_UPDATED_EVENT = "CategoryUpdatedEvent";
    public static final String CATEGORY_DELETED_EVENT = "CategoryDeletedEvent";

    // --- Tag Events ---
    public static final String TAG_CREATED_EVENT = "TagCreatedEvent";
    public static final String TAG_UPDATED_EVENT = "TagUpdatedEvent";
    public static final String TAG_DELETED_EVENT = "TagDeletedEvent";

    // --- Schema Events ---
    public static final String SCHEMA_CREATED_EVENT = "SchemaCreatedEvent";
    public static final String SCHEMA_UPDATED_EVENT = "SchemaUpdatedEvent";
    public static final String SCHEMA_DELETED_EVENT = "SchemaDeletedEvent";

    // --- Service Events ---
    public static final String SERVICE_CREATED_EVENT = "ServiceCreatedEvent";
    public static final String SERVICE_UPDATED_EVENT = "ServiceUpdatedEvent";
    public static final String SERVICE_SPEC_UPDATED_EVENT = "ServiceSpecUpdatedEvent";
    public static final String SERVICE_DELETED_EVENT = "ServiceDeletedEvent";
    public static final String SERVICE_VERSION_CREATED_EVENT = "ServiceVersionCreatedEvent";
    public static final String SERVICE_VERSION_UPDATED_EVENT = "ServiceVersionUpdatedEvent";
    public static final String SERVICE_VERSION_DELETED_EVENT = "ServiceVersionDeletedEvent";
    public static final String ENDPOINT_RULE_CREATED_EVENT = "EndpointRuleCreatedEvent";
    public static final String ENDPOINT_RULE_DELETED_EVENT = "EndpointRuleDeletedEvent";
    // --- Auth Events ---
    public static final String AUTH_REFRESH_TOKEN_CREATED_EVENT = "AuthRefreshTokenCreatedEvent";
    public static final String AUTH_REFRESH_TOKEN_DELETED_EVENT = "AuthRefreshTokenDeletedEvent";
    public static final String AUTH_CODE_CREATED_EVENT = "AuthCodeCreatedEvent";
    public static final String AUTH_CODE_DELETED_EVENT = "AuthCodeDeletedEvent";
    public static final String AUTH_REF_TOKEN_CREATED_EVENT = "AuthRefTokenCreatedEvent";
    public static final String AUTH_REF_TOKEN_DELETED_EVENT = "AuthRefTokenDeletedEvent";
    public static final String AUTH_PROVIDER_CREATED_EVENT = "AuthProviderCreatedEvent";
    public static final String AUTH_PROVIDER_ROTATED_EVENT = "AuthProviderRotatedEvent";
    public static final String AUTH_PROVIDER_UPDATED_EVENT = "AuthProviderUpdatedEvent";
    public static final String AUTH_PROVIDER_DELETED_EVENT = "AuthProviderDeletedEvent";

    // --- Product Events ---
    public static final String PRODUCT_CREATED_EVENT = "ProductCreatedEvent";
    public static final String PRODUCT_UPDATED_EVENT = "ProductUpdatedEvent";
    public static final String PRODUCT_DELETED_EVENT = "ProductDeletedEvent";

    // --- Pipeline Events ---
    public static final String PIPELINE_CREATED_EVENT = "PipelineCreatedEvent";
    public static final String PIPELINE_UPDATED_EVENT = "PipelineUpdatedEvent";
    public static final String PIPELINE_DELETED_EVENT = "PipelineDeletedEvent";

    // --- Platform Events ---
    public static final String PLATFORM_CREATED_EVENT = "PlatformCreatedEvent";
    public static final String PLATFORM_UPDATED_EVENT = "PlatformUpdatedEvent";
    public static final String PLATFORM_DELETED_EVENT = "PlatformDeletedEvent";

    // --- Instance Events ---
    public static final String INSTANCE_CREATED_EVENT = "InstanceCreatedEvent";
    public static final String INSTANCE_UPDATED_EVENT = "InstanceUpdatedEvent";
    public static final String INSTANCE_DELETED_EVENT = "InstanceDeletedEvent";
    public static final String INSTANCE_API_CREATED_EVENT = "InstanceApiCreatedEvent";
    public static final String INSTANCE_API_UPDATED_EVENT = "InstanceApiUpdatedEvent";
    public static final String INSTANCE_API_DELETED_EVENT = "InstanceApiDeletedEvent";
    public static final String INSTANCE_APP_CREATED_EVENT = "InstanceAppCreatedEvent";
    public static final String INSTANCE_APP_UPDATED_EVENT = "InstanceAppUpdatedEvent";
    public static final String INSTANCE_APP_DELETED_EVENT = "InstanceAppDeletedEvent";

    // --- Deployment Events ---
    public static final String DEPLOYMENT_CREATED_EVENT = "DeploymentCreatedEvent";
    public static final String DEPLOYMENT_UPDATED_EVENT = "DeploymentUpdatedEvent";
    public static final String DEPLOYMENT_DELETED_EVENT = "DeploymentDeletedEvent";

    // --- Config Events ---
    public static final String CONFIG_CREATED_EVENT = "ConfigCreatedEvent";
    public static final String CONFIG_UPDATED_EVENT = "ConfigUpdatedEvent";
    public static final String CONFIG_DELETED_EVENT = "ConfigDeletedEvent";
    public static final String CONFIG_PROPERTY_CREATED_EVENT = "ConfigPropertyCreatedEvent";
    public static final String CONFIG_PROPERTY_UPDATED_EVENT = "ConfigPropertyUpdatedEvent";
    public static final String CONFIG_PROPERTY_DELETED_EVENT = "ConfigPropertyDeletedEvent";
    public static final String CONFIG_ENVIRONMENT_CREATED_EVENT = "ConfigEnvironmentCreatedEvent";
    public static final String CONFIG_ENVIRONMENT_UPDATED_EVENT = "ConfigEnvironmentUpdatedEvent";
    public static final String CONFIG_ENVIRONMENT_DELETED_EVENT = "ConfigEnvironmentDeletedEvent";
    public static final String CONFIG_INSTANCE_API_CREATED_EVENT = "ConfigInstanceApiCreatedEvent";
    public static final String CONFIG_INSTANCE_API_UPDATED_EVENT = "ConfigInstanceApiUpdatedEvent";
    public static final String CONFIG_INSTANCE_API_DELETED_EVENT = "ConfigInstanceApiDeletedEvent";
    public static final String CONFIG_INSTANCE_APP_CREATED_EVENT = "ConfigInstanceAppCreatedEvent";
    public static final String CONFIG_INSTANCE_APP_UPDATED_EVENT = "ConfigInstanceAppUpdatedEvent";
    public static final String CONFIG_INSTANCE_APP_DELETED_EVENT = "ConfigInstanceAppDeletedEvent";
    public static final String CONFIG_INSTANCE_CREATED_EVENT = "ConfigInstanceCreatedEvent";
    public static final String CONFIG_INSTANCE_UPDATED_EVENT = "ConfigInstanceUpdatedEvent";
    public static final String CONFIG_INSTANCE_DELETED_EVENT = "ConfigInstanceDeletedEvent";
    public static final String CONFIG_PRODUCT_CREATED_EVENT = "ConfigProductCreatedEvent";
    public static final String CONFIG_PRODUCT_UPDATED_EVENT = "ConfigProductUpdatedEvent";
    public static final String CONFIG_PRODUCT_DELETED_EVENT = "ConfigProductDeletedEvent";
    public static final String CONFIG_PRODUCT_VERSION_CREATED_EVENT = "ConfigProductVersionCreatedEvent";
    public static final String CONFIG_PRODUCT_VERSION_UPDATED_EVENT = "ConfigProductVersionUpdatedEvent";
    public static final String CONFIG_PRODUCT_VERSION_DELETED_EVENT = "ConfigProductVersionDeletedEvent";

    // --- App Events ---
    public static final String APP_CREATED_EVENT = "AppCreatedEvent";
    public static final String APP_UPDATED_EVENT = "AppUpdatedEvent";
    public static final String APP_DELETED_EVENT = "AppDeletedEvent";

    // --- Client Events ---
    public static final String CLIENT_CREATED_EVENT = "ClientCreatedEvent";
    public static final String CLIENT_UPDATED_EVENT = "ClientUpdatedEvent";
    public static final String CLIENT_DELETED_EVENT = "ClientDeletedEvent";


}

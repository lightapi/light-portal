package net.lightapi.portal;

import java.net.URI;

/**
 * Constants used throughout the portal application.
 */
public class PortalConstants {

    /** Event Source URI */
    public static final URI EVENT_SOURCE = URI.create("https://github.com/lightapi/light-portal");

    // order event handling
    /** Order Confirmed */
    public static final String ORDER_CONFIRMED = "Confirmed";
    /** Order Cancelled */
    public static final String ORDER_CANCELLED = "Cancelled";
    /** Order Delivered */
    public static final String ORDER_DELIVERED = "Delivered";

    private PortalConstants() {
        // Prevent instantiation
    }

    // fields
    /** global flag */
    public static final String GLOBAL_FLAG = "globalFlag";
    /** host id */
    public static final String HOST_ID = "hostId";

    // roles
    /** admin role */
    public static final String ADMIN_ROLE = "admin";
    /** user role */
    public static final String USER_ROLE = "user";
    /** guest role */
    public static final String GUEST_ROLE = "guest";
    /** anonymous role */
    public static final String ANONYMOUS_ROLE = "anonymous";
    /** host admin role */
    public static final String HOST_ADMIN_ROLE = "hostAdmin";

    // event types

    // Cloud Event
    /** Cloud Event */
    public static final String DATA = "data";
    /** nonce to prevent replay attack */
    public static final String NONCE = "nonce";
    /** target topic name as an CloudEvent extension */
    public static final String TOPIC = "topic"; // target topic name as an CloudEvent extension.

    // --- User Events ---
    /** UserCreatedEvent */
    public static final String USER_CREATED_EVENT = "UserCreatedEvent";
    /** UserOnboardedEvent */
    public static final String USER_ONBOARDED_EVENT = "UserOnboardedEvent";
    /** SocialUserCreatedEvent */
    public static final String SOCIAL_USER_CREATED_EVENT = "SocialUserCreatedEvent";
    /** UserConfirmedEvent */
    public static final String USER_CONFIRMED_EVENT = "UserConfirmedEvent";
    /** UserVerifiedEvent */
    public static final String USER_VERIFIED_EVENT = "UserVerifiedEvent";
    /** UserUpdatedEvent */
    public static final String USER_UPDATED_EVENT = "UserUpdatedEvent";
    /** PasswordForgotEvent */
    public static final String PASSWORD_FORGOT_EVENT = "PasswordForgotEvent";
    /** PasswordResetEvent */
    public static final String PASSWORD_RESET_EVENT = "PasswordResetEvent";
    /** PasswordChangedEvent */
    public static final String PASSWORD_CHANGED_EVENT = "PasswordChangedEvent";
    /** UserDeletedEvent */
    public static final String USER_DELETED_EVENT = "UserDeletedEvent";
    /** UserRolesUpdatedEvent */
    public static final String USER_ROLES_UPDATED_EVENT = "UserRolesUpdatedEvent";
    /** UserLockedEvent */
    public static final String USER_LOCKED_EVENT = "UserLockedEvent";
    /** UserUnlockedEvent */
    public static final String USER_UNLOCKED_EVENT = "UserUnlockedEvent";
    /** PrivateMessageSentEvent */
    public static final String PRIVATE_MESSAGE_SENT_EVENT = "PrivateMessageSentEvent";
    /** PaymentUpdatedEvent */
    public static final String PAYMENT_UPDATED_EVENT = "PaymentUpdatedEvent";
    /** PaymentDeletedEvent */
    public static final String PAYMENT_DELETED_EVENT = "PaymentDeletedEvent";
    /** OrderCreatedEvent */
    public static final String ORDER_CREATED_EVENT = "OrderCreatedEvent";
    /** OrderCancelledEvent */
    public static final String ORDER_CANCELLED_EVENT = "OrderCancelledEvent";
    /** OrderDeliveredEvent */
    public static final String ORDER_DELIVERED_EVENT = "OrderDeliveredEvent";

    // --- Host Events ---
    /** OrgCreatedEvent */
    public static final String ORG_CREATED_EVENT = "OrgCreatedEvent";
    /** OrgUpdatedEvent */
    public static final String ORG_UPDATED_EVENT = "OrgUpdatedEvent";
    /** OrgDeletedEvent */
    public static final String ORG_DELETED_EVENT = "OrgDeletedEvent";
    /** HostCreatedEvent */
    public static final String HOST_CREATED_EVENT = "HostCreatedEvent";
    /** HostUpdatedEvent */
    public static final String HOST_UPDATED_EVENT = "HostUpdatedEvent";
    /** HostDeletedEvent */
    public static final String HOST_DELETED_EVENT = "HostDeletedEvent";
    /** UserHostSwitchedEvent */
    public static final String USER_HOST_SWITCHED_EVENT = "UserHostSwitchedEvent";
    /** UserHostCreatedEvent */
    public static final String USER_HOST_CREATED_EVENT = "UserHostCreatedEvent";
    /** UserHostDeletedEvent */
    public static final String USER_HOST_DELETED_EVENT = "UserHostDeletedEvent";

    // --- Reference Table Events ---
    /** RefTableCreatedEvent */
    public static final String REF_TABLE_CREATED_EVENT = "RefTableCreatedEvent";
    /** RefTableUpdatedEvent */
    public static final String REF_TABLE_UPDATED_EVENT = "RefTableUpdatedEvent";
    /** RefTableDeletedEvent */
    public static final String REF_TABLE_DELETED_EVENT = "RefTableDeletedEvent";
    /** RefValueCreatedEvent */
    public static final String REF_VALUE_CREATED_EVENT = "RefValueCreatedEvent";
    /** RefValueUpdatedEvent */
    public static final String REF_VALUE_UPDATED_EVENT = "RefValueUpdatedEvent";
    /** RefValueDeletedEvent */
    public static final String REF_VALUE_DELETED_EVENT = "RefValueDeletedEvent";
    /** RefLocaleCreatedEvent */
    public static final String REF_LOCALE_CREATED_EVENT = "RefLocaleCreatedEvent";
    /** RefLocaleUpdatedEvent */
    public static final String REF_LOCALE_UPDATED_EVENT = "RefLocaleUpdatedEvent";
    /** RefLocaleDeletedEvent */
    public static final String REF_LOCALE_DELETED_EVENT = "RefLocaleDeletedEvent";
    /** RefRelationTypeCreatedEvent */
    public static final String REF_RELATION_TYPE_CREATED_EVENT = "RefRelationTypeCreatedEvent";
    /** RefRelationTypeUpdatedEvent */
    public static final String REF_RELATION_TYPE_UPDATED_EVENT = "RefRelationTypeUpdatedEvent";
    /** RefRelationTypeDeletedEvent */
    public static final String REF_RELATION_TYPE_DELETED_EVENT = "RefRelationTypeDeletedEvent";
    /** RefRelationCreatedEvent */
    public static final String REF_RELATION_CREATED_EVENT = "RefRelationCreatedEvent";
    /** RefRelationUpdatedEvent */
    public static final String REF_RELATION_UPDATED_EVENT = "RefRelationUpdatedEvent";
    /** RefRelationDeletedEvent */
    public static final String REF_RELATION_DELETED_EVENT = "RefRelationDeletedEvent";


    // --- Attribute Events ---
    /** AttributeCreatedEvent */
    public static final String ATTRIBUTE_CREATED_EVENT = "AttributeCreatedEvent";
    /** AttributeUpdatedEvent */
    public static final String ATTRIBUTE_UPDATED_EVENT = "AttributeUpdatedEvent";
    /** AttributeDeletedEvent */
    public static final String ATTRIBUTE_DELETED_EVENT = "AttributeDeletedEvent";
    /** AttributePermissionCreatedEvent */
    public static final String ATTRIBUTE_PERMISSION_CREATED_EVENT = "AttributePermissionCreatedEvent";
    /** AttributePermissionUpdatedEvent */
    public static final String ATTRIBUTE_PERMISSION_UPDATED_EVENT = "AttributePermissionUpdatedEvent";
    /** AttributePermissionDeletedEvent */
    public static final String ATTRIBUTE_PERMISSION_DELETED_EVENT = "AttributePermissionDeletedEvent";
    /** AttributeUserCreatedEvent */
    public static final String ATTRIBUTE_USER_CREATED_EVENT = "AttributeUserCreatedEvent";
    /** AttributeUserUpdatedEvent */
    public static final String ATTRIBUTE_USER_UPDATED_EVENT = "AttributeUserUpdatedEvent";
    /** AttributeUserDeletedEvent */
    public static final String ATTRIBUTE_USER_DELETED_EVENT = "AttributeUserDeletedEvent";
    /** AttributeRowFilterCreatedEvent */
    public static final String ATTRIBUTE_ROW_FILTER_CREATED_EVENT = "AttributeRowFilterCreatedEvent";
    /** AttributeRowFilterUpdatedEvent */
    public static final String ATTRIBUTE_ROW_FILTER_UPDATED_EVENT = "AttributeRowFilterUpdatedEvent";
    /** AttributeRowFilterDeletedEvent */
    public static final String ATTRIBUTE_ROW_FILTER_DELETED_EVENT = "AttributeRowFilterDeletedEvent";
    /** AttributeColFilterCreatedEvent */
    public static final String ATTRIBUTE_COL_FILTER_CREATED_EVENT = "AttributeColFilterCreatedEvent";
    /** AttributeColFilterUpdatedEvent */
    public static final String ATTRIBUTE_COL_FILTER_UPDATED_EVENT = "AttributeColFilterUpdatedEvent";
    /** AttributeColFilterDeletedEvent */
    public static final String ATTRIBUTE_COL_FILTER_DELETED_EVENT = "AttributeColFilterDeletedEvent";
    // --- Group Events ---
    /** GroupCreatedEvent */
    public static final String GROUP_CREATED_EVENT = "GroupCreatedEvent";
    /** GroupUpdatedEvent */
    public static final String GROUP_UPDATED_EVENT = "GroupUpdatedEvent";
    /** GroupDeletedEvent */
    public static final String GROUP_DELETED_EVENT = "GroupDeletedEvent";
    /** GroupPermissionCreatedEvent */
    public static final String GROUP_PERMISSION_CREATED_EVENT = "GroupPermissionCreatedEvent";
    /** GroupPermissionDeletedEvent */
    public static final String GROUP_PERMISSION_DELETED_EVENT = "GroupPermissionDeletedEvent";
    /** GroupUserCreatedEvent */
    public static final String GROUP_USER_CREATED_EVENT = "GroupUserCreatedEvent";
    /** GroupUserUpdatedEvent */
    public static final String GROUP_USER_UPDATED_EVENT = "GroupUserUpdatedEvent";
    /** GroupUserDeletedEvent */
    public static final String GROUP_USER_DELETED_EVENT = "GroupUserDeletedEvent";
    /** GroupRowFilterCreatedEvent */
    public static final String GROUP_ROW_FILTER_CREATED_EVENT = "GroupRowFilterCreatedEvent";
    /** GroupRowFilterUpdatedEvent */
    public static final String GROUP_ROW_FILTER_UPDATED_EVENT = "GroupRowFilterUpdatedEvent";
    /** GroupRowFilterDeletedEvent */
    public static final String GROUP_ROW_FILTER_DELETED_EVENT = "GroupRowFilterDeletedEvent";
    /** GroupColFilterCreatedEvent */
    public static final String GROUP_COL_FILTER_CREATED_EVENT = "GroupColFilterCreatedEvent";
    /** GroupColFilterUpdatedEvent */
    public static final String GROUP_COL_FILTER_UPDATED_EVENT = "GroupColFilterUpdatedEvent";
    /** GroupColFilterDeletedEvent */
    public static final String GROUP_COL_FILTER_DELETED_EVENT = "GroupColFilterDeletedEvent";
    // --- Role Events ---
    /** RoleCreatedEvent */
    public static final String ROLE_CREATED_EVENT = "RoleCreatedEvent";
    /** RoleUpdatedEvent */
    public static final String ROLE_UPDATED_EVENT = "RoleUpdatedEvent";
    /** RoleDeletedEvent */
    public static final String ROLE_DELETED_EVENT = "RoleDeletedEvent";
    /** RolePermissionCreatedEvent */
    public static final String ROLE_PERMISSION_CREATED_EVENT = "RolePermissionCreatedEvent";
    /** RolePermissionDeletedEvent */
    public static final String ROLE_PERMISSION_DELETED_EVENT = "RolePermissionDeletedEvent";
    /** RoleUserCreatedEvent */
    public static final String ROLE_USER_CREATED_EVENT = "RoleUserCreatedEvent";
    /** RoleUserUpdatedEvent */
    public static final String ROLE_USER_UPDATED_EVENT = "RoleUserUpdatedEvent";
    /** RoleUserDeletedEvent */
    public static final String ROLE_USER_DELETED_EVENT = "RoleUserDeletedEvent";
    /** RoleRowFilterCreatedEvent */
    public static final String ROLE_ROW_FILTER_CREATED_EVENT = "RoleRowFilterCreatedEvent";
    /** RoleRowFilterUpdatedEvent */
    public static final String ROLE_ROW_FILTER_UPDATED_EVENT = "RoleRowFilterUpdatedEvent";
    /** RoleRowFilterDeletedEvent */
    public static final String ROLE_ROW_FILTER_DELETED_EVENT = "RoleRowFilterDeletedEvent";
    /** RoleColFilterCreatedEvent */
    public static final String ROLE_COL_FILTER_CREATED_EVENT = "RoleColFilterCreatedEvent";
    /** RoleColFilterUpdatedEvent */
    public static final String ROLE_COL_FILTER_UPDATED_EVENT = "RoleColFilterUpdatedEvent";
    /** RoleColFilterDeletedEvent */
    public static final String ROLE_COL_FILTER_DELETED_EVENT = "RoleColFilterDeletedEvent";
    // --- Position Events ---
    /** PositionCreatedEvent */
    public static final String POSITION_CREATED_EVENT = "PositionCreatedEvent";
    /** PositionUpdatedEvent */
    public static final String POSITION_UPDATED_EVENT = "PositionUpdatedEvent";
    /** PositionDeletedEvent */
    public static final String POSITION_DELETED_EVENT = "PositionDeletedEvent";
    /** PositionPermissionCreatedEvent */
    public static final String POSITION_PERMISSION_CREATED_EVENT = "PositionPermissionCreatedEvent";
    /** PositionPermissionDeletedEvent */
    public static final String POSITION_PERMISSION_DELETED_EVENT = "PositionPermissionDeletedEvent";
    /** PositionUserCreatedEvent */
    public static final String POSITION_USER_CREATED_EVENT = "PositionUserCreatedEvent";
    /** PositionUserUpdatedEvent */
    public static final String POSITION_USER_UPDATED_EVENT = "PositionUserUpdatedEvent";
    /** PositionUserDeletedEvent */
    public static final String POSITION_USER_DELETED_EVENT = "PositionUserDeletedEvent";
    /** PositionRowFilterCreatedEvent */
    public static final String POSITION_ROW_FILTER_CREATED_EVENT = "PositionRowFilterCreatedEvent";
    /** PositionRowFilterUpdatedEvent */
    public static final String POSITION_ROW_FILTER_UPDATED_EVENT = "PositionRowFilterUpdatedEvent";
    /** PositionRowFilterDeletedEvent */
    public static final String POSITION_ROW_FILTER_DELETED_EVENT = "PositionRowFilterDeletedEvent";
    /** PositionColFilterCreatedEvent */
    public static final String POSITION_COL_FILTER_CREATED_EVENT = "PositionColFilterCreatedEvent";
    /** PositionColFilterUpdatedEvent */
    public static final String POSITION_COL_FILTER_UPDATED_EVENT = "PositionColFilterUpdatedEvent";
    /** PositionColFilterDeletedEvent */
    public static final String POSITION_COL_FILTER_DELETED_EVENT = "PositionColFilterDeletedEvent";

    // --- Rule Events ---
    /** RuleCreatedEvent */
    public static final String RULE_CREATED_EVENT = "RuleCreatedEvent";
    /** RuleUpdatedEvent */
    public static final String RULE_UPDATED_EVENT = "RuleUpdatedEvent";
    /** RuleDeletedEvent */
    public static final String RULE_DELETED_EVENT = "RuleDeletedEvent";

    // --- Category Events ---
    /** CategoryCreatedEvent */
    public static final String CATEGORY_CREATED_EVENT = "CategoryCreatedEvent";
    /** CategoryUpdatedEvent */
    public static final String CATEGORY_UPDATED_EVENT = "CategoryUpdatedEvent";
    /** CategoryDeletedEvent */
    public static final String CATEGORY_DELETED_EVENT = "CategoryDeletedEvent";

    // --- Tag Events ---
    /** TagCreatedEvent */
    public static final String TAG_CREATED_EVENT = "TagCreatedEvent";
    /** TagUpdatedEvent */
    public static final String TAG_UPDATED_EVENT = "TagUpdatedEvent";
    /** TagDeletedEvent */
    public static final String TAG_DELETED_EVENT = "TagDeletedEvent";

    // --- Schema Events ---
    /** SchemaCreatedEvent */
    public static final String SCHEMA_CREATED_EVENT = "SchemaCreatedEvent";
    /** SchemaUpdatedEvent */
    public static final String SCHEMA_UPDATED_EVENT = "SchemaUpdatedEvent";
    /** SchemaDeletedEvent */
    public static final String SCHEMA_DELETED_EVENT = "SchemaDeletedEvent";

    // --- Service Events ---
    /** ApiCreatedEvent */
    public static final String API_CREATED_EVENT = "ApiCreatedEvent";
    /** ApiUpdatedEvent */
    public static final String API_UPDATED_EVENT = "ApiUpdatedEvent";
    /** ApiDeletedEvent */
    public static final String API_DELETED_EVENT = "ApiDeletedEvent";
    /** ApiVersionCreatedEvent */
    public static final String API_VERSION_CREATED_EVENT = "ApiVersionCreatedEvent";
    /** ApiVersionUpdatedEvent */
    public static final String API_VERSION_UPDATED_EVENT = "ApiVersionUpdatedEvent";
    /** ApiVersionSpecUpdatedEvent */
    public static final String API_VERSION_SPEC_UPDATED_EVENT = "ApiVersionSpecUpdatedEvent";
    /** ApiVersionDeletedEvent */
    public static final String API_VERSION_DELETED_EVENT = "ApiVersionDeletedEvent";
    /** ApiEndpointRuleCreatedEvent */
    public static final String API_ENDPOINT_RULE_CREATED_EVENT = "ApiEndpointRuleCreatedEvent";
    /** ApiEndpointRuleDeletedEvent */
    public static final String API_ENDPOINT_RULE_DELETED_EVENT = "ApiEndpointRuleDeletedEvent";

    // --- Auth Events ---
    /** AuthRefreshTokenCreatedEvent */
    public static final String AUTH_REFRESH_TOKEN_CREATED_EVENT = "AuthRefreshTokenCreatedEvent";
    /** AuthRefreshTokenDeletedEvent */
    public static final String AUTH_REFRESH_TOKEN_DELETED_EVENT = "AuthRefreshTokenDeletedEvent";
    /** AuthCodeCreatedEvent */
    public static final String AUTH_CODE_CREATED_EVENT = "AuthCodeCreatedEvent";
    /** AuthCodeDeletedEvent */
    public static final String AUTH_CODE_DELETED_EVENT = "AuthCodeDeletedEvent";
    /** AuthRefTokenCreatedEvent */
    public static final String AUTH_REF_TOKEN_CREATED_EVENT = "AuthRefTokenCreatedEvent";
    /** AuthRefTokenDeletedEvent */
    public static final String AUTH_REF_TOKEN_DELETED_EVENT = "AuthRefTokenDeletedEvent";
    /** AuthProviderCreatedEvent */
    public static final String AUTH_PROVIDER_CREATED_EVENT = "AuthProviderCreatedEvent";
    /** AuthProviderRotatedEvent */
    public static final String AUTH_PROVIDER_ROTATED_EVENT = "AuthProviderRotatedEvent";
    /** AuthProviderUpdatedEvent */
    public static final String AUTH_PROVIDER_UPDATED_EVENT = "AuthProviderUpdatedEvent";
    /** AuthProviderDeletedEvent */
    public static final String AUTH_PROVIDER_DELETED_EVENT = "AuthProviderDeletedEvent";
    /** AuthProviderApiCreatedEvent */
    public static final String AUTH_PROVIDER_API_CREATED_EVENT = "AuthProviderApiCreatedEvent";
    /** AuthProviderApiDeletedEvent */
    public static final String AUTH_PROVIDER_API_DELETED_EVENT = "AuthProviderApiDeletedEvent";
    /** AuthProviderClientCreatedEvent */
    public static final String AUTH_PROVIDER_CLIENT_CREATED_EVENT = "AuthProviderClientCreatedEvent";
    /** AuthProviderClientDeletedEvent */
    public static final String AUTH_PROVIDER_CLIENT_DELETED_EVENT = "AuthProviderClientDeletedEvent";

    // --- Product Version Events ---
    /** ProductVersionCreatedEvent */
    public static final String PRODUCT_VERSION_CREATED_EVENT = "ProductVersionCreatedEvent";
    /** ProductVersionUpdatedEvent */
    public static final String PRODUCT_VERSION_UPDATED_EVENT = "ProductVersionUpdatedEvent";
    /** ProductVersionDeletedEvent */
    public static final String PRODUCT_VERSION_DELETED_EVENT = "ProductVersionDeletedEvent";

    // --- Product Version Environment Events ---
    /** ProductVersionEnvironmentCreatedEvent */
    public static final String PRODUCT_VERSION_ENVIRONMENT_CREATED_EVENT = "ProductVersionEnvironmentCreatedEvent";
    /** ProductVersionEnvironmentUpdatedEvent */
    public static final String PRODUCT_VERSION_ENVIRONMENT_UPDATED_EVENT = "ProductVersionEnvironmentUpdatedEvent";
    /** ProductVersionEnvironmentDeletedEvent */
    public static final String PRODUCT_VERSION_ENVIRONMENT_DELETED_EVENT = "ProductVersionEnvironmentDeletedEvent";

    // --- Product Version Pipeline Events ---
    /** ProductVersionPipelineCreatedEvent */
    public static final String PRODUCT_VERSION_PIPELINE_CREATED_EVENT = "ProductVersionPipelineCreatedEvent";
    /** ProductVersionPipelineDeletedEvent */
    public static final String PRODUCT_VERSION_PIPELINE_DELETED_EVENT = "ProductVersionPipelineDeletedEvent";

    // --- Product Version Config Events ---
    /** ProductVersionConfigCreatedEvent */
    public static final String PRODUCT_VERSION_CONFIG_CREATED_EVENT = "ProductVersionConfigCreatedEvent";
    /** ProductVersionConfigDeletedEvent */
    public static final String PRODUCT_VERSION_CONFIG_DELETED_EVENT = "ProductVersionConfigDeletedEvent";

    // --- Product Version Config Property Events ---
    /** ProductVersionConfigPropertyCreatedEvent */
    public static final String PRODUCT_VERSION_CONFIG_PROPERTY_CREATED_EVENT = "ProductVersionConfigPropertyCreatedEvent";
    /** ProductVersionConfigPropertyDeletedEvent */
    public static final String PRODUCT_VERSION_CONFIG_PROPERTY_DELETED_EVENT = "ProductVersionConfigPropertyDeletedEvent";

    // --- Pipeline Events ---
    /** PipelineCreatedEvent */
    public static final String PIPELINE_CREATED_EVENT = "PipelineCreatedEvent";
    /** PipelineUpdatedEvent */
    public static final String PIPELINE_UPDATED_EVENT = "PipelineUpdatedEvent";
    /** PipelineDeletedEvent */
    public static final String PIPELINE_DELETED_EVENT = "PipelineDeletedEvent";

    // --- Platform Events ---
    /** PlatformCreatedEvent */
    public static final String PLATFORM_CREATED_EVENT = "PlatformCreatedEvent";
    /** PlatformUpdatedEvent */
    public static final String PLATFORM_UPDATED_EVENT = "PlatformUpdatedEvent";
    /** PlatformDeletedEvent */
    public static final String PLATFORM_DELETED_EVENT = "PlatformDeletedEvent";

    // --- Instance Events ---
    /** InstanceCreatedEvent */
    public static final String INSTANCE_CREATED_EVENT = "InstanceCreatedEvent";
    /** InstanceUpdatedEvent */
    public static final String INSTANCE_UPDATED_EVENT = "InstanceUpdatedEvent";
    /** InstanceDeletedEvent */
    public static final String INSTANCE_DELETED_EVENT = "InstanceDeletedEvent";
    /** InstanceLockedEvent */
    public static final String INSTANCE_LOCKED_EVENT = "InstanceLockedEvent";
    /** InstanceUnlockedEvent */
    public static final String INSTANCE_UNLOCKED_EVENT = "InstanceUnlockedEvent";
    /** InstanceClonedEvent */
    public static final String INSTANCE_CLONED_EVENT = "InstanceClonedEvent";
    /** InstancePromotedEvent */
    public static final String INSTANCE_PROMOTED_EVENT = "InstancePromotedEvent";
    /** InstanceApiCreatedEvent */
    public static final String INSTANCE_API_CREATED_EVENT = "InstanceApiCreatedEvent";
    /** InstanceApiUpdatedEvent */
    public static final String INSTANCE_API_UPDATED_EVENT = "InstanceApiUpdatedEvent";
    /** InstanceApiDeletedEvent */
    public static final String INSTANCE_API_DELETED_EVENT = "InstanceApiDeletedEvent";
    /** InstanceAppCreatedEvent */
    public static final String INSTANCE_APP_CREATED_EVENT = "InstanceAppCreatedEvent";
    /** InstanceAppUpdatedEvent */
    public static final String INSTANCE_APP_UPDATED_EVENT = "InstanceAppUpdatedEvent";
    /** InstanceAppDeletedEvent */
    public static final String INSTANCE_APP_DELETED_EVENT = "InstanceAppDeletedEvent";
    /** InstancePipelineCreatedEvent */
    public static final String INSTANCE_PIPELINE_CREATED_EVENT = "InstancePipelineCreatedEvent";
    /** InstancePipelineUpdatedEvent */
    public static final String INSTANCE_PIPELINE_UPDATED_EVENT = "InstancePipelineUpdatedEvent";
    /** InstancePipelineDeletedEvent */
    public static final String INSTANCE_PIPELINE_DELETED_EVENT = "InstancePipelineDeletedEvent";
    /** InstanceAppApiCreatedEvent */
    public static final String INSTANCE_APP_API_CREATED_EVENT = "InstanceAppApiCreatedEvent";
    /** InstanceAppApiUpdatedEvent */
    public static final String INSTANCE_APP_API_UPDATED_EVENT = "InstanceAppApiUpdatedEvent";
    /** InstanceAppApiDeletedEvent */
    public static final String INSTANCE_APP_API_DELETED_EVENT = "InstanceAppApiDeletedEvent";
    /** InstanceApiPathPrefixCreatedEvent */
    public static final String INSTANCE_API_PATH_PREFIX_CREATED_EVENT = "InstanceApiPathPrefixCreatedEvent";
    /** InstanceApiPathPrefixUpdatedEvent */
    public static final String INSTANCE_API_PATH_PREFIX_UPDATED_EVENT = "InstanceApiPathPrefixUpdatedEvent";
    /** InstanceApiPathPrefixDeletedEvent */
    public static final String INSTANCE_API_PATH_PREFIX_DELETED_EVENT = "InstanceApiPathPrefixDeletedEvent";
    /** CompositeInstanceAppCreatedEvent */
    public static final String COMPOSITE_INSTANCE_APP_CREATED_EVENT = "CompositeInstanceAppCreatedEvent";
    /** CompositeInstanceApiCreatedEvent */
    public static final String COMPOSITE_INSTANCE_API_CREATED_EVENT = "CompositeInstanceApiCreatedEvent";


    // --- Deployment Events ---
    /** DeploymentCreatedEvent */
    public static final String DEPLOYMENT_CREATED_EVENT = "DeploymentCreatedEvent";
    /** DeploymentUpdatedEvent */
    public static final String DEPLOYMENT_UPDATED_EVENT = "DeploymentUpdatedEvent";
    /** DeploymentJobIdUpdatedEvent */
    public static final String DEPLOYMENT_JOB_ID_UPDATED_EVENT = "DeploymentJobIdUpdatedEvent";
    /** DeploymentStatusUpdatedEvent */
    public static final String DEPLOYMENT_STATUS_UPDATED_EVENT = "DeploymentStatusUpdatedEvent";
    /** DeploymentDeletedEvent */
    public static final String DEPLOYMENT_DELETED_EVENT = "DeploymentDeletedEvent";

    // --- Deployment Instance Events ---
    /** DeploymentInstanceCreatedEvent */
    public static final String DEPLOYMENT_INSTANCE_CREATED_EVENT = "DeploymentInstanceCreatedEvent";
    /** DeploymentInstanceUpdatedEvent */
    public static final String DEPLOYMENT_INSTANCE_UPDATED_EVENT = "DeploymentInstanceUpdatedEvent";
    /** DeploymentInstanceDeletedEvent */
    public static final String DEPLOYMENT_INSTANCE_DELETED_EVENT = "DeploymentInstanceDeletedEvent";

    // --- Config Events ---
    /** ConfigCreatedEvent */
    public static final String CONFIG_CREATED_EVENT = "ConfigCreatedEvent";
    /** ConfigUpdatedEvent */
    public static final String CONFIG_UPDATED_EVENT = "ConfigUpdatedEvent";
    /** ConfigDeletedEvent */
    public static final String CONFIG_DELETED_EVENT = "ConfigDeletedEvent";
    /** ConfigPropertyCreatedEvent */
    public static final String CONFIG_PROPERTY_CREATED_EVENT = "ConfigPropertyCreatedEvent";
    /** ConfigPropertyUpdatedEvent */
    public static final String CONFIG_PROPERTY_UPDATED_EVENT = "ConfigPropertyUpdatedEvent";
    /** ConfigPropertyDeletedEvent */
    public static final String CONFIG_PROPERTY_DELETED_EVENT = "ConfigPropertyDeletedEvent";
    /** ConfigEnvironmentCreatedEvent */
    public static final String CONFIG_ENVIRONMENT_CREATED_EVENT = "ConfigEnvironmentCreatedEvent";
    /** ConfigEnvironmentUpdatedEvent */
    public static final String CONFIG_ENVIRONMENT_UPDATED_EVENT = "ConfigEnvironmentUpdatedEvent";
    /** ConfigEnvironmentDeletedEvent */
    public static final String CONFIG_ENVIRONMENT_DELETED_EVENT = "ConfigEnvironmentDeletedEvent";
    /** ConfigInstanceApiCreatedEvent */
    public static final String CONFIG_INSTANCE_API_CREATED_EVENT = "ConfigInstanceApiCreatedEvent";
    /** ConfigInstanceApiUpdatedEvent */
    public static final String CONFIG_INSTANCE_API_UPDATED_EVENT = "ConfigInstanceApiUpdatedEvent";
    /** ConfigInstanceApiDeletedEvent */
    public static final String CONFIG_INSTANCE_API_DELETED_EVENT = "ConfigInstanceApiDeletedEvent";
    /** ConfigInstanceAppCreatedEvent */
    public static final String CONFIG_INSTANCE_APP_CREATED_EVENT = "ConfigInstanceAppCreatedEvent";
    /** ConfigInstanceAppUpdatedEvent */
    public static final String CONFIG_INSTANCE_APP_UPDATED_EVENT = "ConfigInstanceAppUpdatedEvent";
    /** ConfigInstanceAppDeletedEvent */
    public static final String CONFIG_INSTANCE_APP_DELETED_EVENT = "ConfigInstanceAppDeletedEvent";
    /** ConfigInstanceAppApiCreatedEvent */
    public static final String CONFIG_INSTANCE_APP_API_CREATED_EVENT = "ConfigInstanceAppApiCreatedEvent";
    /** ConfigInstanceAppApiUpdatedEvent */
    public static final String CONFIG_INSTANCE_APP_API_UPDATED_EVENT = "ConfigInstanceAppApiUpdatedEvent";
    /** ConfigInstanceAppApiDeletedEvent */
    public static final String CONFIG_INSTANCE_APP_API_DELETED_EVENT = "ConfigInstanceAppApiDeletedEvent";
    /** ConfigInstanceFileCreatedEvent */
    public static final String CONFIG_INSTANCE_FILE_CREATED_EVENT = "ConfigInstanceFileCreatedEvent";
    /** ConfigInstanceFileUpdatedEvent */
    public static final String CONFIG_INSTANCE_FILE_UPDATED_EVENT = "ConfigInstanceFileUpdatedEvent";
    /** ConfigInstanceFileDeletedEvent */
    public static final String CONFIG_INSTANCE_FILE_DELETED_EVENT = "ConfigInstanceFileDeletedEvent";
    /** ConfigDeploymentInstanceCreatedEvent */
    public static final String CONFIG_DEPLOYMENT_INSTANCE_CREATED_EVENT = "ConfigDeploymentInstanceCreatedEvent";
    /** ConfigDeploymentInstanceUpdatedEvent */
    public static final String CONFIG_DEPLOYMENT_INSTANCE_UPDATED_EVENT = "ConfigDeploymentInstanceUpdatedEvent";
    /** ConfigDeploymentInstanceDeletedEvent */
    public static final String CONFIG_DEPLOYMENT_INSTANCE_DELETED_EVENT = "ConfigDeploymentInstanceDeletedEvent";

    /** ConfigInstanceCreatedEvent */
    public static final String CONFIG_INSTANCE_CREATED_EVENT = "ConfigInstanceCreatedEvent";
    /** ConfigInstanceUpdatedEvent */
    public static final String CONFIG_INSTANCE_UPDATED_EVENT = "ConfigInstanceUpdatedEvent";
    /** ConfigInstanceDeletedEvent */
    public static final String CONFIG_INSTANCE_DELETED_EVENT = "ConfigInstanceDeletedEvent";
    /** ConfigProductCreatedEvent */
    public static final String CONFIG_PRODUCT_CREATED_EVENT = "ConfigProductCreatedEvent";
    /** ConfigProductUpdatedEvent */
    public static final String CONFIG_PRODUCT_UPDATED_EVENT = "ConfigProductUpdatedEvent";
    /** ConfigProductDeletedEvent */
    public static final String CONFIG_PRODUCT_DELETED_EVENT = "ConfigProductDeletedEvent";
    /** ConfigProductVersionCreatedEvent */
    public static final String CONFIG_PRODUCT_VERSION_CREATED_EVENT = "ConfigProductVersionCreatedEvent";
    /** ConfigProductVersionUpdatedEvent */
    public static final String CONFIG_PRODUCT_VERSION_UPDATED_EVENT = "ConfigProductVersionUpdatedEvent";
    /** ConfigProductVersionDeletedEvent */
    public static final String CONFIG_PRODUCT_VERSION_DELETED_EVENT = "ConfigProductVersionDeletedEvent";

    // --- App Events ---
    /** AppCreatedEvent */
    public static final String APP_CREATED_EVENT = "AppCreatedEvent";
    /** AppUpdatedEvent */
    public static final String APP_UPDATED_EVENT = "AppUpdatedEvent";
    /** AppDeletedEvent */
    public static final String APP_DELETED_EVENT = "AppDeletedEvent";

    // --- Client Events ---
    /** ClientCreatedEvent */
    public static final String CLIENT_CREATED_EVENT = "ClientCreatedEvent";
    /** ClientUpdatedEvent */
    public static final String CLIENT_UPDATED_EVENT = "ClientUpdatedEvent";
    /** ClientDeletedEvent */
    public static final String CLIENT_DELETED_EVENT = "ClientDeletedEvent";

    // --- Schedule Events ---
    /** ScheduleCreatedEvent */
    public static final String SCHEDULE_CREATED_EVENT = "ScheduleCreatedEvent";
    /** ScheduleUpdatedEvent */
    public static final String SCHEDULE_UPDATED_EVENT = "ScheduleUpdatedEvent";
    /** ScheduleDeletedEvent */
    public static final String SCHEDULE_DELETED_EVENT = "ScheduleDeletedEvent";

    // --- Platform Queried Events ---
    /** PlatformQueriedEvent */
    public static final String PLATFORM_QUERIED_EVENT = "PlatformQueriedEvent";

    // --- GenAI Events ---
    /** AgentDefinitionCreatedEvent */
    public static final String AGENT_DEFINITION_CREATED_EVENT = "AgentDefinitionCreatedEvent";
    /** AgentDefinitionUpdatedEvent */
    public static final String AGENT_DEFINITION_UPDATED_EVENT = "AgentDefinitionUpdatedEvent";
    /** AgentDefinitionDeletedEvent */
    public static final String AGENT_DEFINITION_DELETED_EVENT = "AgentDefinitionDeletedEvent";
    /** WorkflowDefinitionCreatedEvent */
    public static final String WORKFLOW_DEFINITION_CREATED_EVENT = "WorkflowDefinitionCreatedEvent";
    /** WorkflowDefinitionUpdatedEvent */
    public static final String WORKFLOW_DEFINITION_UPDATED_EVENT = "WorkflowDefinitionUpdatedEvent";
    /** WorkflowDefinitionDeletedEvent */
    public static final String WORKFLOW_DEFINITION_DELETED_EVENT = "WorkflowDefinitionDeletedEvent";
    /** WorklistCreatedEvent */
    public static final String WORKLIST_CREATED_EVENT = "WorklistCreatedEvent";
    /** WorklistUpdatedEvent */
    public static final String WORKLIST_UPDATED_EVENT = "WorklistUpdatedEvent";
    /** WorklistDeletedEvent */
    public static final String WORKLIST_DELETED_EVENT = "WorklistDeletedEvent";
    /** WorklistColumnCreatedEvent */
    public static final String WORKLIST_COLUMN_CREATED_EVENT = "WorklistColumnCreatedEvent";
    /** WorklistColumnUpdatedEvent */
    public static final String WORKLIST_COLUMN_UPDATED_EVENT = "WorklistColumnUpdatedEvent";
    /** WorklistColumnDeletedEvent */
    public static final String WORKLIST_COLUMN_DELETED_EVENT = "WorklistColumnDeletedEvent";
    /** ProcessInfoCreatedEvent */
    public static final String PROCESS_INFO_CREATED_EVENT = "ProcessInfoCreatedEvent";
    /** ProcessInfoUpdatedEvent */
    public static final String PROCESS_INFO_UPDATED_EVENT = "ProcessInfoUpdatedEvent";
    /** ProcessInfoDeletedEvent */
    public static final String PROCESS_INFO_DELETED_EVENT = "ProcessInfoDeletedEvent";
    /** TaskInfoCreatedEvent */
    public static final String TASK_INFO_CREATED_EVENT = "TaskInfoCreatedEvent";
    /** TaskInfoUpdatedEvent */
    public static final String TASK_INFO_UPDATED_EVENT = "TaskInfoUpdatedEvent";
    /** TaskInfoDeletedEvent */
    public static final String TASK_INFO_DELETED_EVENT = "TaskInfoDeletedEvent";
    /** TaskAssignmentCreatedEvent */
    public static final String TASK_ASSIGNMENT_CREATED_EVENT = "TaskAssignmentCreatedEvent";
    /** TaskAssignmentUpdatedEvent */
    public static final String TASK_ASSIGNMENT_UPDATED_EVENT = "TaskAssignmentUpdatedEvent";
    /** TaskAssignmentDeletedEvent */
    public static final String TASK_ASSIGNMENT_DELETED_EVENT = "TaskAssignmentDeletedEvent";
    /** AuditLogCreatedEvent */
    public static final String AUDIT_LOG_CREATED_EVENT = "AuditLogCreatedEvent";
    /** SkillCreatedEvent */
    public static final String SKILL_CREATED_EVENT = "SkillCreatedEvent";
    /** SkillUpdatedEvent */
    public static final String SKILL_UPDATED_EVENT = "SkillUpdatedEvent";
    /** SkillDeletedEvent */
    public static final String SKILL_DELETED_EVENT = "SkillDeletedEvent";
    /** ToolCreatedEvent */
    public static final String TOOL_CREATED_EVENT = "ToolCreatedEvent";
    /** ToolUpdatedEvent */
    public static final String TOOL_UPDATED_EVENT = "ToolUpdatedEvent";
    /** ToolDeletedEvent */
    public static final String TOOL_DELETED_EVENT = "ToolDeletedEvent";
    /** ToolParamCreatedEvent */
    public static final String TOOL_PARAM_CREATED_EVENT = "ToolParamCreatedEvent";
    /** ToolParamUpdatedEvent */
    public static final String TOOL_PARAM_UPDATED_EVENT = "ToolParamUpdatedEvent";
    /** ToolParamDeletedEvent */
    public static final String TOOL_PARAM_DELETED_EVENT = "ToolParamDeletedEvent";
    /** SkillToolCreatedEvent */
    public static final String SKILL_TOOL_CREATED_EVENT = "SkillToolCreatedEvent";
    /** SkillToolUpdatedEvent */
    public static final String SKILL_TOOL_UPDATED_EVENT = "SkillToolUpdatedEvent";
    /** SkillToolDeletedEvent */
    public static final String SKILL_TOOL_DELETED_EVENT = "SkillToolDeletedEvent";
    /** SkillDependencyCreatedEvent */
    public static final String SKILL_DEPENDENCY_CREATED_EVENT = "SkillDependencyCreatedEvent";
    /** SkillDependencyUpdatedEvent */
    public static final String SKILL_DEPENDENCY_UPDATED_EVENT = "SkillDependencyUpdatedEvent";
    /** SkillDependencyDeletedEvent */
    public static final String SKILL_DEPENDENCY_DELETED_EVENT = "SkillDependencyDeletedEvent";
    /** AgentSkillCreatedEvent */
    public static final String AGENT_SKILL_CREATED_EVENT = "AgentSkillCreatedEvent";
    /** AgentSkillUpdatedEvent */
    public static final String AGENT_SKILL_UPDATED_EVENT = "AgentSkillUpdatedEvent";
    /** AgentSkillDeletedEvent */
    public static final String AGENT_SKILL_DELETED_EVENT = "AgentSkillDeletedEvent";
    /** AgentSessionHistoryCreatedEvent */
    public static final String AGENT_SESSION_HISTORY_CREATED_EVENT = "AgentSessionHistoryCreatedEvent";
    /** AgentSessionHistoryDeletedEvent */
    public static final String AGENT_SESSION_HISTORY_DELETED_EVENT = "AgentSessionHistoryDeletedEvent";
    /** SessionMemoryCreatedEvent */
    public static final String SESSION_MEMORY_CREATED_EVENT = "SessionMemoryCreatedEvent";
    /** SessionMemoryUpdatedEvent */
    public static final String SESSION_MEMORY_UPDATED_EVENT = "SessionMemoryUpdatedEvent";
    /** SessionMemoryDeletedEvent */
    public static final String SESSION_MEMORY_DELETED_EVENT = "SessionMemoryDeletedEvent";
    /** UserMemoryCreatedEvent */
    public static final String USER_MEMORY_CREATED_EVENT = "UserMemoryCreatedEvent";
    /** UserMemoryUpdatedEvent */
    public static final String USER_MEMORY_UPDATED_EVENT = "UserMemoryUpdatedEvent";
    /** UserMemoryDeletedEvent */
    public static final String USER_MEMORY_DELETED_EVENT = "UserMemoryDeletedEvent";
    /** AgentMemoryCreatedEvent */
    public static final String AGENT_MEMORY_CREATED_EVENT = "AgentMemoryCreatedEvent";
    /** AgentMemoryUpdatedEvent */
    public static final String AGENT_MEMORY_UPDATED_EVENT = "AgentMemoryUpdatedEvent";
    /** AgentMemoryDeletedEvent */
    public static final String AGENT_MEMORY_DELETED_EVENT = "AgentMemoryDeletedEvent";
    /** OrgMemoryCreatedEvent */
    public static final String ORG_MEMORY_CREATED_EVENT = "OrgMemoryCreatedEvent";
    /** OrgMemoryUpdatedEvent */
    public static final String ORG_MEMORY_UPDATED_EVENT = "OrgMemoryUpdatedEvent";
    /** OrgMemoryDeletedEvent */
    public static final String ORG_MEMORY_DELETED_EVENT = "OrgMemoryDeletedEvent";

    // aggregate

    // aggregate id
    /** aggregate id */
    public static final String AGGREGATE_ID = "aggregateid";
    /** event aggregate version */
    public static final String EVENT_AGGREGATE_VERSION = "aggregateversion";
    /** aggregate Version */
    public static final String AGGREGATE_VERSION = "aggregateVersion";
    /** new Aggregate Version */
    public static final String NEW_AGGREGATE_VERSION = "newAggregateVersion";
    // aggregate type
    /** aggregate type */
    public static final String AGGREGATE_TYPE = "aggregatetype";

    // -- Auth --
    /** AuthCode aggregate */
    public static final String AGGREGATE_AUTH_CODE = "AuthCode";
    /** Client aggregate */
    public static final String AGGREGATE_CLIENT = "Client";
    /** AuthProvider aggregate */
    public static final String AGGREGATE_AUTH_PROVIDER = "AuthProvider";
    /** AuthProviderApi aggregate */
    public static final String AGGREGATE_AUTH_PROVIDER_API = "AuthProviderApi";
    /** AuthProviderClient aggregate */
    public static final String AGGREGATE_AUTH_PROVIDER_CLIENT = "AuthProviderClient";
    /** AuthRefreshToken aggregate */
    public static final String AGGREGATE_AUTH_REFRESH_TOKEN = "AuthRefreshToken";
    /** AuthRefToken aggregate */
    public static final String AGGREGATE_AUTH_REF_TOKEN = "AuthRefToken";

    // -- Category --
    /** Category aggregate */
    public static final String AGGREGATE_CATEGORY = "Category";

    // -- App --
    /** App aggregate */
    public static final String AGGREGATE_APP = "App";

    // -- Config --
    /** Config aggregate */
    public static final String AGGREGATE_CONFIG = "Config";
    /** ConfigProperty aggregate */
    public static final String AGGREGATE_CONFIG_PROPERTY = "ConfigProperty";
    /** ConfigEnvironment aggregate */
    public static final String AGGREGATE_CONFIG_ENVIRONMENT = "ConfigEnvironment";
    /** ConfigInstanceApi aggregate */
    public static final String AGGREGATE_CONFIG_INSTANCE_API = "ConfigInstanceApi";
    /** ConfigInstanceApp aggregate */
    public static final String AGGREGATE_CONFIG_INSTANCE_APP = "ConfigInstanceApp";
    /** ConfigInstanceAppApi aggregate */
    public static final String AGGREGATE_CONFIG_INSTANCE_APP_API = "ConfigInstanceAppApi";
    /** ConfigInstanceFile aggregate */
    public static final String AGGREGATE_CONFIG_INSTANCE_FILE = "ConfigInstanceFile";
    /** ConfigDeploymentInstance aggregate */
    public static final String AGGREGATE_CONFIG_DEPLOYMENT_INSTANCE = "ConfigDeploymentInstance";
    /** ConfigInstance aggregate */
    public static final String AGGREGATE_CONFIG_INSTANCE = "ConfigInstance";
    /** ConfigProduct aggregate */
    public static final String AGGREGATE_CONFIG_PRODUCT = "ConfigProduct";
    /** ConfigProductVersion aggregate */
    public static final String AGGREGATE_CONFIG_PRODUCT_VERSION = "ConfigProductVersion";

    // -- Deployment --
    /** Deployment aggregate */
    public static final String AGGREGATE_DEPLOYMENT = "Deployment";
    /** DeploymentInstance aggregate */
    public static final String AGGREGATE_DEPLOYMENT_INSTANCE = "DeploymentInstance";
    /** Pipeline aggregate */
    public static final String AGGREGATE_PIPELINE = "Pipeline";
    /** Platform aggregate */
    public static final String AGGREGATE_PLATFORM = "Platform";

    // -- Host --
    /** Host aggregate */
    public static final String AGGREGATE_HOST = "Host";
    /** UserHost aggregate */
    public static final String AGGREGATE_USER_HOST = "UserHost";
    /** Org aggregate */
    public static final String AGGREGATE_ORG = "Org";

    // -- Instance --
    /** Instance aggregate */
    public static final String AGGREGATE_INSTANCE = "Instance";
    /** InstanceApi aggregate */
    public static final String AGGREGATE_INSTANCE_API = "InstanceApi";
    /** CompositeInstanceApi aggregate */
    public static final String AGGREGATE_COMPOSITE_INSTANCE_API = "CompositeInstanceApi";
    /** InstanceApiPathPrefix aggregate */
    public static final String AGGREGATE_INSTANCE_API_PATH_PREFIX = "InstanceApiPathPrefix";
    /** InstanceApp aggregate */
    public static final String AGGREGATE_INSTANCE_APP = "InstanceApp";
    /** CompositeInstanceApp aggregate */
    public static final String AGGREGATE_COMPOSITE_INSTANCE_APP = "CompositeInstanceApp";
    /** InstanceAppApi aggregate */
    public static final String AGGREGATE_INSTANCE_APP_API = "InstanceAppApi";

    // -- Ref --
    /** RefTable aggregate */
    public static final String AGGREGATE_REF_TABLE = "RefTable";
    /** RefValue aggregate */
    public static final String AGGREGATE_REF_VALUE = "RefValue";
    /** RefLocale aggregate */
    public static final String AGGREGATE_REF_LOCALE = "RefLocale";
    /** RefRelationType aggregate */
    public static final String AGGREGATE_REF_RELATION_TYPE = "RefRelationType";
    /** RefRelation aggregate */
    public static final String AGGREGATE_REF_RELATION = "RefRelation";

    // -- Role --
    /** Role aggregate */
    public static final String AGGREGATE_ROLE = "Role";
    /** RolePermission aggregate */
    public static final String AGGREGATE_ROLE_PERMISSION = "RolePermission";
    /** RoleUser aggregate */
    public static final String AGGREGATE_ROLE_USER = "RoleUser";
    /** RoleRowFilter aggregate */
    public static final String AGGREGATE_ROLE_ROW_FILTER = "RoleRowFilter";
    /** RoleColFilter aggregate */
    public static final String AGGREGATE_ROLE_COL_FILTER = "RoleColFilter";

    // -- Group --
    /** Group aggregate */
    public static final String AGGREGATE_GROUP = "Group";
    /** GroupPermission aggregate */
    public static final String AGGREGATE_GROUP_PERMISSION = "GroupPermission";
    /** GroupUser aggregate */
    public static final String AGGREGATE_GROUP_USER = "GroupUser";
    /** GroupRowFilter aggregate */
    public static final String AGGREGATE_GROUP_ROW_FILTER = "GroupRowFilter";
    /** GroupColFilter aggregate */
    public static final String AGGREGATE_GROUP_COL_FILTER = "GroupColFilter";

    // -- Position --
    /** Position aggregate */
    public static final String AGGREGATE_POSITION = "Position";
    /** PositionPermission aggregate */
    public static final String AGGREGATE_POSITION_PERMISSION = "PositionPermission";
    /** PositionUser aggregate */
    public static final String AGGREGATE_POSITION_USER = "PositionUser";
    /** PositionRowFilter aggregate */
    public static final String AGGREGATE_POSITION_ROW_FILTER = "PositionRowFilter";
    /** PositionColFilter aggregate */
    public static final String AGGREGATE_POSITION_COL_FILTER = "PositionColFilter";

    // -- Attribute --
    /** Attribute aggregate */
    public static final String AGGREGATE_ATTRIBUTE = "Attribute";
    /** AttributePermission aggregate */
    public static final String AGGREGATE_ATTRIBUTE_PERMISSION = "AttributePermission";
    /** AttributeUser aggregate */
    public static final String AGGREGATE_ATTRIBUTE_USER = "AttributeUser";
    /** AttributeRowFilter aggregate */
    public static final String AGGREGATE_ATTRIBUTE_ROW_FILTER = "AttributeRowFilter";
    /** AttributeColFilter aggregate */
    public static final String AGGREGATE_ATTRIBUTE_COL_FILTER = "AttributeColFilter";

    // -- Rule --
    /** Rule aggregate */
    public static final String AGGREGATE_RULE = "Rule";

    // -- Schema --
    /** Schema aggregate */
    public static final String AGGREGATE_SCHEMA = "Schema";

    // -- Schedule --
    /** Schedule aggregate */
    public static final String AGGREGATE_SCHEDULE = "Schedule";

    // -- Tag --
    /** Tag aggregate */
    public static final String AGGREGATE_TAG = "Tag";

    // -- User --
    /** User aggregate */
    public static final String AGGREGATE_USER = "User";

    // -- Product --
    /** ProductVersion aggregate */
    public static final String AGGREGATE_PRODUCT_VERSION = "ProductVersion";
    /** ProductVersionEnvironment aggregate */
    public static final String AGGREGATE_PRODUCT_VERSION_ENVIRONMENT = "ProductVersionEnvironment";
    /** ProductVersionPipeline aggregate */
    public static final String AGGREGATE_PRODUCT_VERSION_PIPELINE = "ProductVersionPipeline";
    /** ProductVersionConfig aggregate */
    public static final String AGGREGATE_PRODUCT_VERSION_CONFIG = "ProductVersionConfig";
    /** ProductVersionConfigProperty aggregate */
    public static final String AGGREGATE_PRODUCT_VERSION_CONFIG_PROPERTY = "ProductVersionConfigProperty";

    // -- Api --
    /** Api aggregate */
    public static final String AGGREGATE_API = "Api";
    /** ApiVersion aggregate */
    public static final String AGGREGATE_API_VERSION = "ApiVersion";
    /** ApiVersionSpec aggregate */
    public static final String AGGREGATE_API_VERSION_SPEC = "ApiVersionSpec";
    /** ApiEndpoint aggregate */
    public static final String AGGREGATE_API_ENDPOINT = "ApiEndpoint";
    /** ApiEndpointScope aggregate */
    public static final String AGGREGATE_API_ENDPOINT_SCOPE = "ApiEndpointScope";
    /** ApiEndpointRule aggregate */
    public static final String AGGREGATE_API_ENDPOINT_RULE = "ApiEndpointRule";

    // -- GenAI --
    /** AgentDefinition aggregate */
    public static final String AGGREGATE_AGENT_DEFINITION = "AgentDefinition";
    /** WorkflowDefinition aggregate */
    public static final String AGGREGATE_WORKFLOW_DEFINITION = "WorkflowDefinition";
    /** Worklist aggregate */
    public static final String AGGREGATE_WORKLIST = "Worklist";
    /** WorklistColumn aggregate */
    public static final String AGGREGATE_WORKLIST_COLUMN = "WorklistColumn";
    /** ProcessInfo aggregate */
    public static final String AGGREGATE_PROCESS_INFO = "ProcessInfo";
    /** TaskInfo aggregate */
    public static final String AGGREGATE_TASK_INFO = "TaskInfo";
    /** TaskAssignment aggregate */
    public static final String AGGREGATE_TASK_ASSIGNMENT = "TaskAssignment";
    /** AuditLog aggregate */
    public static final String AGGREGATE_AUDIT_LOG = "AuditLog";
    /** Skill aggregate */
    public static final String AGGREGATE_SKILL = "Skill";
    /** ToolParam aggregate */
    public static final String AGGREGATE_TOOL_PARAM = "ToolParam";
    /** SkillTool aggregate */
    public static final String AGGREGATE_SKILL_TOOL = "SkillTool";
    /** SkillDependency aggregate */
    public static final String AGGREGATE_SKILL_DEPENDENCY = "SkillDependency";
    /** AgentSkill aggregate */
    public static final String AGGREGATE_AGENT_SKILL = "AgentSkill";
    /** AgentSessionHistory aggregate */
    public static final String AGGREGATE_AGENT_SESSION_HISTORY = "AgentSessionHistory";
    /** SessionMemory aggregate */
    public static final String AGGREGATE_SESSION_MEMORY = "SessionMemory";
    /** UserMemory aggregate */
    public static final String AGGREGATE_USER_MEMORY = "UserMemory";
    /** AgentMemory aggregate */
    public static final String AGGREGATE_AGENT_MEMORY = "AgentMemory";
    /** OrgMemory aggregate */
    public static final String AGGREGATE_ORG_MEMORY = "OrgMemory";

}

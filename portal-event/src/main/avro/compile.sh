#!/bin/bash

# -string is very necessary, otherwise the key of map will be Utf8 internal class. You cannot find object with string.

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema UserCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema UserUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema UserConfirmedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema UserVerifiedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema UserDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PasswordResetEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PasswordForgotEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PasswordChangedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PrivateMessageSentEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema UserLockedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema UserUnlockedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema UserRolesUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema SocialUserCreatedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ReferenceCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ReferenceUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ReferenceDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RefRelaCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RefRelaDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CityMapCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CityMapUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CityMapDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema EntityCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema EntityUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema EntityDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MapMovedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema StatusUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PeerStatusUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema WebsiteUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema WebsiteDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema StatusDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PaymentUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PaymentDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema OrderCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema OrderCancelledEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema OrderDeliveredEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ClientCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ClientUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ClientDeletedEvent.avsc .


java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ServiceCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ServiceUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ServiceDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ServiceSpecUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ServiceVersionCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ServiceVersionUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ServiceVersionDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema EndpointRuleCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema EndpointRuleDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AuthRefreshTokenCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AuthRefreshTokenDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AuthCodeCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AuthCodeDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AuthRefTokenCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AuthRefTokenDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AuthProviderCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AuthProviderUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AuthProviderDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CategoryCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CategoryCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CategoryUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CategoryDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema TaxonomyCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema TaxonomyDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema BlogCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema BlogUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema BlogDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema DocumentCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema DocumentUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema DocumentDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PageCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PageUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PageDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema TemplateCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema TemplateUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema TemplateDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema NewsCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema NewsUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema NewsDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ErrorCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ErrorUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ErrorDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema JsonSchemaCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema JsonSchemaUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema JsonSchemaDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RuleCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RuleUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RuleDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema HostCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema HostUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema HostDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ControllerRegisteredEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ControllerDeregisteredEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ConfigCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ConfigUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ConfigDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RoleCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RoleUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RoleDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RolePermissionCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RolePermissionDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RoleUserCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RoleUserDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RoleUserUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RoleRowFilterCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RoleRowFilterUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RoleRowFilterDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RoleColFilterCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RoleColFilterUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema RoleColFilterDeletedEvent.avsc .


java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema GroupCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema GroupUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema GroupDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema GroupPermissionCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema GroupPermissionDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema GroupUserCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema GroupUserDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema GroupUserUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema GroupRowFilterCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema GroupRowFilterUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema GroupRowFilterDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema GroupColFilterCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema GroupColFilterUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema GroupColFilterDeletedEvent.avsc .


java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributeCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributeUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributeDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributePermissionCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributePermissionDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributePermissionUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributeUserCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributeUserDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributeUserUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributeRowFilterCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributeRowFilterUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributeRowFilterDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributeColFilterCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributeColFilterUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema AttributeColFilterDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PositionCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PositionUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PositionDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PositionPermissionCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PositionPermissionDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PositionUserCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PositionUserDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PositionUserUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PositionRowFilterCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PositionRowFilterUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PositionRowFilterDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PositionColFilterCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PositionColFilterUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PositionColFilterDeletedEvent.avsc .

# Update events to implement from KycEvent interface in order to group these events in streams processing.

# find . -name '*Event.java' -exec sed -i "s/implements org.apache.avro.specific.SpecificRecord/implements UserEvent/g" {} +

# move to the right directory and remove the generated folder.

mv net/lightapi/portal/user/UserCreatedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/UserUpdatedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/UserConfirmedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/UserVerifiedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/UserDeletedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/PasswordResetEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/PasswordForgotEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/PasswordChangedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/PrivateMessageSentEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/UserLockedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/UserUnlockedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/UserRolesUpdatedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/SocialUserCreatedEvent.java ../java/net/lightapi/portal/user

mv net/lightapi/portal/user/ReferenceCreatedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/ReferenceUpdatedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/ReferenceDeletedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/RefRelaCreatedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/RefRelaDeletedEvent.java ../java/net/lightapi/portal/user

mv net/lightapi/portal/map/CityMapCreatedEvent.java ../java/net/lightapi/portal/map
mv net/lightapi/portal/map/CityMapUpdatedEvent.java ../java/net/lightapi/portal/map
mv net/lightapi/portal/map/CityMapDeletedEvent.java ../java/net/lightapi/portal/map
mv net/lightapi/portal/entity/EntityCreatedEvent.java ../java/net/lightapi/portal/entity
mv net/lightapi/portal/entity/EntityUpdatedEvent.java ../java/net/lightapi/portal/entity
mv net/lightapi/portal/entity/EntityDeletedEvent.java ../java/net/lightapi/portal/entity
mv net/lightapi/portal/map/MapMovedEvent.java ../java/net/lightapi/portal/map
mv net/lightapi/portal/status/StatusUpdatedEvent.java ../java/net/lightapi/portal/status
mv net/lightapi/portal/status/PeerStatusUpdatedEvent.java ../java/net/lightapi/portal/status
mv net/lightapi/portal/website/WebsiteUpdatedEvent.java ../java/net/lightapi/portal/website
mv net/lightapi/portal/website/WebsiteDeletedEvent.java ../java/net/lightapi/portal/website
mv net/lightapi/portal/status/StatusDeletedEvent.java ../java/net/lightapi/portal/status

mv net/lightapi/portal/user/PaymentUpdatedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/PaymentDeletedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/OrderCreatedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/OrderCancelledEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/OrderDeliveredEvent.java ../java/net/lightapi/portal/user

mv net/lightapi/portal/user/HostCreatedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/HostUpdatedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/HostDeletedEvent.java ../java/net/lightapi/portal/user

mv net/lightapi/portal/client/ClientCreatedEvent.java ../java/net/lightapi/portal/client
mv net/lightapi/portal/client/ClientUpdatedEvent.java ../java/net/lightapi/portal/client
mv net/lightapi/portal/client/ClientDeletedEvent.java ../java/net/lightapi/portal/client

mv net/lightapi/portal/service/ServiceCreatedEvent.java ../java/net/lightapi/portal/service
mv net/lightapi/portal/service/ServiceUpdatedEvent.java ../java/net/lightapi/portal/service
mv net/lightapi/portal/service/ServiceDeletedEvent.java ../java/net/lightapi/portal/service
mv net/lightapi/portal/service/ServiceSpecUpdatedEvent.java ../java/net/lightapi/portal/service
mv net/lightapi/portal/service/ServiceVersionCreatedEvent.java ../java/net/lightapi/portal/service
mv net/lightapi/portal/service/ServiceVersionUpdatedEvent.java ../java/net/lightapi/portal/service
mv net/lightapi/portal/service/ServiceVersionDeletedEvent.java ../java/net/lightapi/portal/service
mv net/lightapi/portal/service/EndpointRuleCreatedEvent.java ../java/net/lightapi/portal/service
mv net/lightapi/portal/service/EndpointRuleDeletedEvent.java ../java/net/lightapi/portal/service

mv net/lightapi/portal/oauth/AuthRefreshTokenCreatedEvent.java ../java/net/lightapi/portal/oauth
mv net/lightapi/portal/oauth/AuthRefreshTokenDeletedEvent.java ../java/net/lightapi/portal/oauth
mv net/lightapi/portal/oauth/AuthCodeCreatedEvent.java ../java/net/lightapi/portal/oauth
mv net/lightapi/portal/oauth/AuthCodeDeletedEvent.java ../java/net/lightapi/portal/oauth
mv net/lightapi/portal/oauth/AuthRefTokenCreatedEvent.java ../java/net/lightapi/portal/oauth
mv net/lightapi/portal/oauth/AuthRefTokenDeletedEvent.java ../java/net/lightapi/portal/oauth
mv net/lightapi/portal/oauth/AuthProviderCreatedEvent.java ../java/net/lightapi/portal/oauth
mv net/lightapi/portal/oauth/AuthProviderUpdatedEvent.java ../java/net/lightapi/portal/oauth
mv net/lightapi/portal/oauth/AuthProviderDeletedEvent.java ../java/net/lightapi/portal/oauth

mv net/lightapi/portal/market/CategoryCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/CategoryUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/CategoryDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/TaxonomyCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/TaxonomyDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/BlogCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/BlogUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/BlogDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/DocumentCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/DocumentUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/DocumentDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PageCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PageUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PageDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/TemplateCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/TemplateUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/TemplateDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/NewsCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/NewsUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/NewsDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ErrorCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ErrorUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ErrorDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/JsonSchemaCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/JsonSchemaUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/JsonSchemaDeletedEvent.java ../java/net/lightapi/portal/market

mv net/lightapi/portal/rule/RuleCreatedEvent.java ../java/net/lightapi/portal/rule
mv net/lightapi/portal/rule/RuleUpdatedEvent.java ../java/net/lightapi/portal/rule
mv net/lightapi/portal/rule/RuleDeletedEvent.java ../java/net/lightapi/portal/rule

mv net/lightapi/portal/market/ConfigCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ConfigUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ConfigDeletedEvent.java ../java/net/lightapi/portal/market

mv net/lightapi/portal/role/RoleCreatedEvent.java ../java/net/lightapi/portal/role
mv net/lightapi/portal/role/RoleUpdatedEvent.java ../java/net/lightapi/portal/role
mv net/lightapi/portal/role/RoleDeletedEvent.java ../java/net/lightapi/portal/role
mv net/lightapi/portal/role/RolePermissionCreatedEvent.java ../java/net/lightapi/portal/role
mv net/lightapi/portal/role/RolePermissionDeletedEvent.java ../java/net/lightapi/portal/role
mv net/lightapi/portal/role/RoleUserCreatedEvent.java ../java/net/lightapi/portal/role
mv net/lightapi/portal/role/RoleUserDeletedEvent.java ../java/net/lightapi/portal/role
mv net/lightapi/portal/role/RoleUserUpdatedEvent.java ../java/net/lightapi/portal/role
mv net/lightapi/portal/role/RoleRowFilterCreatedEvent.java ../java/net/lightapi/portal/role
mv net/lightapi/portal/role/RoleRowFilterUpdatedEvent.java ../java/net/lightapi/portal/role
mv net/lightapi/portal/role/RoleRowFilterDeletedEvent.java ../java/net/lightapi/portal/role
mv net/lightapi/portal/role/RoleColFilterCreatedEvent.java ../java/net/lightapi/portal/role
mv net/lightapi/portal/role/RoleColFilterUpdatedEvent.java ../java/net/lightapi/portal/role
mv net/lightapi/portal/role/RoleColFilterDeletedEvent.java ../java/net/lightapi/portal/role


mv net/lightapi/portal/group/GroupCreatedEvent.java ../java/net/lightapi/portal/group
mv net/lightapi/portal/group/GroupUpdatedEvent.java ../java/net/lightapi/portal/group
mv net/lightapi/portal/group/GroupDeletedEvent.java ../java/net/lightapi/portal/group
mv net/lightapi/portal/group/GroupPermissionCreatedEvent.java ../java/net/lightapi/portal/group
mv net/lightapi/portal/group/GroupPermissionDeletedEvent.java ../java/net/lightapi/portal/group
mv net/lightapi/portal/group/GroupUserCreatedEvent.java ../java/net/lightapi/portal/group
mv net/lightapi/portal/group/GroupUserDeletedEvent.java ../java/net/lightapi/portal/group
mv net/lightapi/portal/group/GroupUserUpdatedEvent.java ../java/net/lightapi/portal/group
mv net/lightapi/portal/group/GroupRowFilterCreatedEvent.java ../java/net/lightapi/portal/group
mv net/lightapi/portal/group/GroupRowFilterUpdatedEvent.java ../java/net/lightapi/portal/group
mv net/lightapi/portal/group/GroupRowFilterDeletedEvent.java ../java/net/lightapi/portal/group
mv net/lightapi/portal/group/GroupColFilterCreatedEvent.java ../java/net/lightapi/portal/group
mv net/lightapi/portal/group/GroupColFilterUpdatedEvent.java ../java/net/lightapi/portal/group
mv net/lightapi/portal/group/GroupColFilterDeletedEvent.java ../java/net/lightapi/portal/group


mv net/lightapi/portal/attribute/AttributeCreatedEvent.java ../java/net/lightapi/portal/attribute
mv net/lightapi/portal/attribute/AttributeUpdatedEvent.java ../java/net/lightapi/portal/attribute
mv net/lightapi/portal/attribute/AttributeDeletedEvent.java ../java/net/lightapi/portal/attribute
mv net/lightapi/portal/attribute/AttributePermissionCreatedEvent.java ../java/net/lightapi/portal/attribute
mv net/lightapi/portal/attribute/AttributePermissionDeletedEvent.java ../java/net/lightapi/portal/attribute
mv net/lightapi/portal/attribute/AttributePermissionUpdatedEvent.java ../java/net/lightapi/portal/attribute
mv net/lightapi/portal/attribute/AttributeUserCreatedEvent.java ../java/net/lightapi/portal/attribute
mv net/lightapi/portal/attribute/AttributeUserDeletedEvent.java ../java/net/lightapi/portal/attribute
mv net/lightapi/portal/attribute/AttributeUserUpdatedEvent.java ../java/net/lightapi/portal/attribute
mv net/lightapi/portal/attribute/AttributeRowFilterCreatedEvent.java ../java/net/lightapi/portal/attribute
mv net/lightapi/portal/attribute/AttributeRowFilterUpdatedEvent.java ../java/net/lightapi/portal/attribute
mv net/lightapi/portal/attribute/AttributeRowFilterDeletedEvent.java ../java/net/lightapi/portal/attribute
mv net/lightapi/portal/attribute/AttributeColFilterCreatedEvent.java ../java/net/lightapi/portal/attribute
mv net/lightapi/portal/attribute/AttributeColFilterUpdatedEvent.java ../java/net/lightapi/portal/attribute
mv net/lightapi/portal/attribute/AttributeColFilterDeletedEvent.java ../java/net/lightapi/portal/attribute


mv net/lightapi/portal/position/PositionCreatedEvent.java ../java/net/lightapi/portal/position
mv net/lightapi/portal/position/PositionUpdatedEvent.java ../java/net/lightapi/portal/position
mv net/lightapi/portal/position/PositionDeletedEvent.java ../java/net/lightapi/portal/position
mv net/lightapi/portal/position/PositionPermissionCreatedEvent.java ../java/net/lightapi/portal/position
mv net/lightapi/portal/position/PositionPermissionDeletedEvent.java ../java/net/lightapi/portal/position
mv net/lightapi/portal/position/PositionUserCreatedEvent.java ../java/net/lightapi/portal/position
mv net/lightapi/portal/position/PositionUserDeletedEvent.java ../java/net/lightapi/portal/position
mv net/lightapi/portal/position/PositionUserUpdatedEvent.java ../java/net/lightapi/portal/position
mv net/lightapi/portal/position/PositionRowFilterCreatedEvent.java ../java/net/lightapi/portal/position
mv net/lightapi/portal/position/PositionRowFilterUpdatedEvent.java ../java/net/lightapi/portal/position
mv net/lightapi/portal/position/PositionRowFilterDeletedEvent.java ../java/net/lightapi/portal/position
mv net/lightapi/portal/position/PositionColFilterCreatedEvent.java ../java/net/lightapi/portal/position
mv net/lightapi/portal/position/PositionColFilterUpdatedEvent.java ../java/net/lightapi/portal/position
mv net/lightapi/portal/position/PositionColFilterDeletedEvent.java ../java/net/lightapi/portal/position

mv net/lightapi/portal/controller/ControllerRegisteredEvent.java ../java/net/lightapi/portal/controller
mv net/lightapi/portal/controller/ControllerDeregisteredEvent.java ../java/net/lightapi/portal/controller

rm -rf net
rm -rf com

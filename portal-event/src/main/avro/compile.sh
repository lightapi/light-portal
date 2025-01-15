#!/bin/bash

# -string is very necessary, otherwise the key of map will be Utf8 internal class. You cannot find object with string.

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema UserCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema UserUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema UserConfirmedEvent.avsc .
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

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CovidEntityCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CovidEntityUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CovidEntityDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CovidMapMovedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CovidStatusUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PeerStatusUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CovidWebsiteUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CovidWebsiteDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema CovidStatusDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PaymentUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema PaymentDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema OrderCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema OrderCancelledEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema OrderDeliveredEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketClientCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketClientUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketClientDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketServiceCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketServiceUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketServiceDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ServiceSpecUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ServiceRuleUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ServiceVersionCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ServiceVersionUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ServiceVersionDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema EndpointRuleCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema EndpointRuleDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketTokenCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketTokenDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketCodeCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketCodeDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketRefCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketRefDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketProviderCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketProviderUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema MarketProviderDeletedEvent.avsc .
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
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ApiRuleCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.12.0.jar compile -string schema ApiRuleDeletedEvent.avsc .

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

mv net/lightapi/portal/covid/CityMapCreatedEvent.java ../java/net/lightapi/portal/covid
mv net/lightapi/portal/covid/CityMapUpdatedEvent.java ../java/net/lightapi/portal/covid
mv net/lightapi/portal/covid/CityMapDeletedEvent.java ../java/net/lightapi/portal/covid
mv net/lightapi/portal/covid/CovidEntityCreatedEvent.java ../java/net/lightapi/portal/covid
mv net/lightapi/portal/covid/CovidEntityUpdatedEvent.java ../java/net/lightapi/portal/covid
mv net/lightapi/portal/covid/CovidEntityDeletedEvent.java ../java/net/lightapi/portal/covid
mv net/lightapi/portal/covid/CovidMapMovedEvent.java ../java/net/lightapi/portal/covid
mv net/lightapi/portal/covid/CovidStatusUpdatedEvent.java ../java/net/lightapi/portal/covid
mv net/lightapi/portal/covid/PeerStatusUpdatedEvent.java ../java/net/lightapi/portal/covid
mv net/lightapi/portal/covid/CovidWebsiteUpdatedEvent.java ../java/net/lightapi/portal/covid
mv net/lightapi/portal/covid/CovidWebsiteDeletedEvent.java ../java/net/lightapi/portal/covid
mv net/lightapi/portal/covid/CovidStatusDeletedEvent.java ../java/net/lightapi/portal/covid

mv net/lightapi/portal/user/PaymentUpdatedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/PaymentDeletedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/OrderCreatedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/OrderCancelledEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/OrderDeliveredEvent.java ../java/net/lightapi/portal/user

mv net/lightapi/portal/user/HostCreatedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/HostUpdatedEvent.java ../java/net/lightapi/portal/user
mv net/lightapi/portal/user/HostDeletedEvent.java ../java/net/lightapi/portal/user

mv net/lightapi/portal/market/MarketClientCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketClientUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketClientDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketServiceCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketServiceUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketServiceDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ServiceSpecUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ServiceRuleUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ServiceVersionCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ServiceVersionUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ServiceVersionDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/EndpointRuleCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/EndpointRuleDeletedEvent.java ../java/net/lightapi/portal/market

mv net/lightapi/portal/market/ServiceRule.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketTokenCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketTokenDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketCodeCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketCodeDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketRefCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketRefDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketProviderCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketProviderUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketProviderDeletedEvent.java ../java/net/lightapi/portal/market
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
mv net/lightapi/portal/market/RuleCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RuleUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RuleDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ApiRuleCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ApiRuleDeletedEvent.java ../java/net/lightapi/portal/market

mv net/lightapi/portal/market/ConfigCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ConfigUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ConfigDeletedEvent.java ../java/net/lightapi/portal/market

mv net/lightapi/portal/market/RoleCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RoleUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RoleDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RolePermissionCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RolePermissionDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RoleUserCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RoleUserDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RoleUserUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RoleRowFilterCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RoleRowFilterUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RoleRowFilterDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RoleColFilterCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RoleColFilterUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/RoleColFilterDeletedEvent.java ../java/net/lightapi/portal/market


mv net/lightapi/portal/market/GroupCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/GroupUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/GroupDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/GroupPermissionCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/GroupPermissionDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/GroupUserCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/GroupUserDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/GroupUserUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/GroupRowFilterCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/GroupRowFilterUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/GroupRowFilterDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/GroupColFilterCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/GroupColFilterUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/GroupColFilterDeletedEvent.java ../java/net/lightapi/portal/market


mv net/lightapi/portal/market/AttributeCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/AttributeUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/AttributeDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/AttributePermissionCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/AttributePermissionDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/AttributePermissionUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/AttributeUserCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/AttributeUserDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/AttributeUserUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/AttributeRowFilterCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/AttributeRowFilterUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/AttributeRowFilterDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/AttributeColFilterCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/AttributeColFilterUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/AttributeColFilterDeletedEvent.java ../java/net/lightapi/portal/market


mv net/lightapi/portal/market/PositionCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PositionUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PositionDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PositionPermissionCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PositionPermissionDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PositionUserCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PositionUserDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PositionUserUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PositionRowFilterCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PositionRowFilterUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PositionRowFilterDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PositionColFilterCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PositionColFilterUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/PositionColFilterDeletedEvent.java ../java/net/lightapi/portal/market

mv net/lightapi/portal/controller/ControllerRegisteredEvent.java ../java/net/lightapi/portal/controller
mv net/lightapi/portal/controller/ControllerDeregisteredEvent.java ../java/net/lightapi/portal/controller

rm -rf net
rm -rf com

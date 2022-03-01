#!/bin/bash

# -string is very necessary, otherwise the key of map will be Utf8 internal class. You cannot find object with string.

java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema UserCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema UserUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema UserConfirmedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema UserDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema PasswordResetEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema PasswordForgotEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema PasswordChangedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema PrivateMessageSentEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema UserLockedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema UserUnlockedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema UserRolesUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema SocialUserCreatedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema ReferenceCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema ReferenceUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema ReferenceDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema RefRelaCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema RefRelaDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema CityMapCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema CityMapUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema CityMapDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema CovidEntityCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema CovidEntityUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema CovidEntityDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema CovidMapMovedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema CovidStatusUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema PeerStatusUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema CovidWebsiteUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema CovidWebsiteDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema CovidStatusDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema PaymentUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema PaymentDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema OrderCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema OrderCancelledEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema OrderDeliveredEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketClientCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketClientUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketClientDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketServiceCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketServiceUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketServiceDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema ServiceSpecUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema ServiceRuleUpdatedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketTokenCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketTokenDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketCodeCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketCodeDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketRefCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketRefDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketProviderCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketProviderUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema MarketProviderDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema CategoryCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema CategoryUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema CategoryDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema TaxonomyCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema TaxonomyDeletedEvent.avsc .

java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema BlogCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema BlogUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema BlogDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema DocumentCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema DocumentUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema DocumentDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema PageCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema PageUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema PageDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema TemplateCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema TemplateUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema TemplateDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema NewsCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema NewsUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema NewsDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema ErrorCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema ErrorUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema ErrorDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema JsonSchemaCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema JsonSchemaUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema JsonSchemaDeletedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema RuleCreatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema RuleUpdatedEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema RuleDeletedEvent.avsc .


java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema ControllerRegisteredEvent.avsc .
java -jar /home/steve/tool/avro-tools-1.11.0.jar compile -string schema ControllerDeregisteredEvent.avsc .

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

mv net/lightapi/portal/market/MarketClientCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketClientUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketClientDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketServiceCreatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketServiceUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/MarketServiceDeletedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ServiceSpecUpdatedEvent.java ../java/net/lightapi/portal/market
mv net/lightapi/portal/market/ServiceRuleUpdatedEvent.java ../java/net/lightapi/portal/market
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

mv net/lightapi/portal/controller/ControllerRegisteredEvent.java ../java/net/lightapi/portal/controller
mv net/lightapi/portal/controller/ControllerDeregisteredEvent.java ../java/net/lightapi/portal/controller

rm -rf net
rm -rf com

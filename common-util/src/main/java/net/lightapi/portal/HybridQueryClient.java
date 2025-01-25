package net.lightapi.portal;

import com.networknt.client.simplepool.SimpleConnectionHolder;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.server.Server;
import com.networknt.server.ServerConfig;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.status.Status;
import com.networknt.client.Http2Client;
import com.networknt.cluster.Cluster;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.OptionMap;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a client utility that is shared by all command side services to query data from
 * the query side. It creates a HTTP/2 connection to the query side and cache that connection
 * until the connection is closed. Then it create another connection and cache it. This will
 * ensure the highest efficiency.
 *
 * @author Steve Hu
 */
public class HybridQueryClient {

    static final Logger logger = LoggerFactory.getLogger(HybridQueryClient.class);
    static final String FAILED_TO_POPULATE_HEADER = "ERR12050";
    public static final PortalClientConfig config = (PortalClientConfig) Config.getInstance().getJsonObjectConfig(PortalClientConfig.CONFIG_NAME, PortalClientConfig.class);

    //static final String queryServiceId = "com.networknt.portal.hybrid.query-1.0.0";
    static String tag = ServerConfig.getInstance().getEnvironment();
    // Get the singleton Cluster instance
    static Cluster cluster = SingletonServiceFactory.getBean(Cluster.class);
    // Get the singleton Http2Client instance
    static Http2Client client = Http2Client.getInstance();
    static final String GENERIC_EXCEPTION = "ERR10014";

    static Map<String, ClientConnection> connCache = new ConcurrentHashMap<>();

    public static Result<String> callQueryWithToken(String command, String token) {
        Result<String> result = null;
        SimpleConnectionHolder.ConnectionToken connectionToken = null;
        try {
            String host = cluster.serviceToUrl("https", config.getPortalQueryServiceId(), tag, null);
            if(logger.isTraceEnabled()) logger.trace("serviceId " + config.getPortalQueryServiceId() + " with result " + host);
            URI uri = new URI(host);
            connectionToken = client.borrow(uri, Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true));
            ClientConnection connection = (ClientConnection) connectionToken.getRawConnection();
            // Create one CountDownLatch that will be reset in the callback function
            final CountDownLatch latch = new CountDownLatch(1);
            // Create an AtomicReference object to receive ClientResponse from callback function
            final AtomicReference<ClientResponse> reference = new AtomicReference<>();
            String message = "/portal/query?cmd=" + URLEncoder.encode(command, StandardCharsets.UTF_8);
            final ClientRequest request = new ClientRequest().setMethod(Methods.GET).setPath(message);
            request.getRequestHeaders().put(Headers.AUTHORIZATION, "Bearer " + token);
            request.getRequestHeaders().put(Headers.HOST, "localhost");
            connection.sendRequest(request, client.createClientCallback(reference, latch));
            latch.await();
            int statusCode = reference.get().getResponseCode();
            String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
            if(statusCode != 200) {
                Status status = Config.getInstance().getMapper().readValue(body, Status.class);
                result = Failure.of(status);
            } else result = Success.of(body);
        } catch (Exception e) {
            logger.error("Exception:", e);
            Status status = new Status(GENERIC_EXCEPTION, e.getMessage());
            result = Failure.of(status);
        } finally {
            client.restore(connectionToken);
        }
        return result;
    }

    public static Result<String> callQueryExchangeUrl(String command, HttpServerExchange exchange, String url) {
        Result<String> result = null;
        SimpleConnectionHolder.ConnectionToken connectionToken = null;
        try {
            URI uri = new URI(url);
            connectionToken = client.borrow(uri, Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true));
            ClientConnection connection = (ClientConnection) connectionToken.getRawConnection();
            // Create one CountDownLatch that will be reset in the callback function
            final CountDownLatch latch = new CountDownLatch(1);
            // Create an AtomicReference object to receive ClientResponse from callback function
            final AtomicReference<ClientResponse> reference = new AtomicReference<>();
            String message = "/portal/query?cmd=" + URLEncoder.encode(command, "UTF-8");
            final ClientRequest request = new ClientRequest().setMethod(Methods.GET).setPath(message);
            String token = exchange.getRequestHeaders().getFirst(Headers.AUTHORIZATION);
            if(token != null) request.getRequestHeaders().put(Headers.AUTHORIZATION, token);
            request.getRequestHeaders().put(Headers.HOST, "localhost");
            connection.sendRequest(request, client.createClientCallback(reference, latch));
            latch.await();
            int statusCode = reference.get().getResponseCode();
            String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
            if(logger.isTraceEnabled()) logger.trace("statusCode = " + statusCode + " body = " + body);
            if(statusCode != 200) {
                Status status = Config.getInstance().getMapper().readValue(body, Status.class);
                result = Failure.of(status);
            } else result = Success.of(body);
        } catch (Exception e) {
            logger.error("Exception:", e);
            Status status = new Status(GENERIC_EXCEPTION, e.getMessage());
            result = Failure.of(status);
        } finally {
            client.restore(connectionToken);
        }
        return result;
    }

    public static Result<String> callQueryExchange(String command, HttpServerExchange exchange) {
        Result<String> result = null;
        SimpleConnectionHolder.ConnectionToken connectionToken = null;
        try {
            String host = cluster.serviceToUrl("https", config.getPortalQueryServiceId(), tag, null);
            if(logger.isTraceEnabled()) logger.trace("serviceId " + config.getPortalQueryServiceId() + " with result " + host);
            URI uri = new URI(host);
            connectionToken = client.borrow(uri, Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true));
            ClientConnection connection = (ClientConnection) connectionToken.getRawConnection();
            // Create one CountDownLatch that will be reset in the callback function
            final CountDownLatch latch = new CountDownLatch(1);
            // Create an AtomicReference object to receive ClientResponse from callback function
            final AtomicReference<ClientResponse> reference = new AtomicReference<>();
            String message = "/portal/query?cmd=" + URLEncoder.encode(command, "UTF-8");
            final ClientRequest request = new ClientRequest().setMethod(Methods.GET).setPath(message);
            String token = exchange.getRequestHeaders().getFirst(Headers.AUTHORIZATION);
            if(token != null)
                request.getRequestHeaders().put(Headers.AUTHORIZATION, token);
            else
                request.getRequestHeaders().put(Headers.AUTHORIZATION, "Bearer " + config.getBootstrapToken());
            request.getRequestHeaders().put(Headers.HOST, "localhost");
            connection.sendRequest(request, client.createClientCallback(reference, latch));
            latch.await();
            int statusCode = reference.get().getResponseCode();
            String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
            if(statusCode != 200) {
                Status status = Config.getInstance().getMapper().readValue(body, Status.class);
                result = Failure.of(status);
            } else result = Success.of(body);
        } catch (Exception e) {
            logger.error("Exception:", e);
            Status status = new Status(GENERIC_EXCEPTION, e.getMessage());
            result = Failure.of(status);
        } finally {
            client.restore(connectionToken);
        }
        return result;
    }

    public static Result<String> callQueryUrl(String command, String url) {
        Result<String> result = null;
        SimpleConnectionHolder.ConnectionToken connectionToken = null;
        try {
            URI uri = new URI(url);
            connectionToken = client.borrow(uri, Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true));
            ClientConnection connection = (ClientConnection) connectionToken.getRawConnection();
            // Create one CountDownLatch that will be reset in the callback function
            final CountDownLatch latch = new CountDownLatch(1);
            // Create an AtomicReference object to receive ClientResponse from callback function
            final AtomicReference<ClientResponse> reference = new AtomicReference<>();
            String message = "/portal/query?cmd=" + URLEncoder.encode(command, "UTF-8");
            final ClientRequest request = new ClientRequest().setMethod(Methods.GET).setPath(message);
            request.getRequestHeaders().put(Headers.HOST, "localhost");
            connection.sendRequest(request, client.createClientCallback(reference, latch));
            latch.await();
            int statusCode = reference.get().getResponseCode();
            String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
            if(statusCode != 200) {
                Status status = Config.getInstance().getMapper().readValue(body, Status.class);
                result = Failure.of(status);
            } else result = Success.of(body);
        } catch (Exception e) {
            logger.error("Exception:", e);
            Status status = new Status(GENERIC_EXCEPTION, e.getMessage());
            result = Failure.of(status);
        } finally {
            client.restore(connectionToken);
        }
        return result;
    }

    public static Result<String> callQueryTokenUrl(String command, String token, String url) {
        Result<String> result = null;
        SimpleConnectionHolder.ConnectionToken connectionToken = null;
        try {
            URI uri = new URI(url);
            connectionToken = client.borrow(uri, Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true));
            ClientConnection connection = (ClientConnection) connectionToken.getRawConnection();
            // Create one CountDownLatch that will be reset in the callback function
            final CountDownLatch latch = new CountDownLatch(1);
            // Create an AtomicReference object to receive ClientResponse from callback function
            final AtomicReference<ClientResponse> reference = new AtomicReference<>();
            String message = "/portal/query?cmd=" + URLEncoder.encode(command, "UTF-8");
            final ClientRequest request = new ClientRequest().setMethod(Methods.GET).setPath(message);
            request.getRequestHeaders().put(Headers.AUTHORIZATION, "Bearer " + token);
            request.getRequestHeaders().put(Headers.HOST, "localhost");
            connection.sendRequest(request, client.createClientCallback(reference, latch));
            latch.await();
            int statusCode = reference.get().getResponseCode();
            String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
            if(statusCode != 200) {
                Status status = Config.getInstance().getMapper().readValue(body, Status.class);
                result = Failure.of(status);
            } else result = Success.of(body);
        } catch (Exception e) {
            logger.error("Exception:", e);
            Status status = new Status(GENERIC_EXCEPTION, e.getMessage());
            result = Failure.of(status);
        } finally {
            client.restore(connectionToken);
        }
        return result;
    }

    /*
    public static Result<String> callQueryTokenUrl(String command, String token, String url) {
        Result<String> result = null;
        try {
            ClientConnection conn = connCache.get(url);
            if(conn == null || !conn.isOpen()) {
                conn = client.connect(new URI(url), Http2Client.WORKER, Http2Client.SSL, Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true)).get();
                connCache.put(url, conn);
            }
            // Create one CountDownLatch that will be reset in the callback function
            final CountDownLatch latch = new CountDownLatch(1);
            // Create an AtomicReference object to receive ClientResponse from callback function
            final AtomicReference<ClientResponse> reference = new AtomicReference<>();
            String message = "/portal/query?cmd=" + URLEncoder.encode(command, "UTF-8");
            final ClientRequest request = new ClientRequest().setMethod(Methods.GET).setPath(message);
            request.getRequestHeaders().put(Headers.AUTHORIZATION, token.startsWith("Bearer") ? token : "Bearer " + token);
            request.getRequestHeaders().put(Headers.HOST, "localhost");
            conn.sendRequest(request, client.createClientCallback(reference, latch));
            latch.await();
            int statusCode = reference.get().getResponseCode();
            String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
            if(statusCode != 200) {
                Status status = Config.getInstance().getMapper().readValue(body, Status.class);
                result = Failure.of(status);
            } else result = Success.of(body);
        } catch (Exception e) {
            logger.error("Exception:", e);
            Status status = new Status(GENERIC_EXCEPTION, e.getMessage());
            result = Failure.of(status);
        }
        return result;
    }
    */

    /**
     * Get User by email with exchange and email
     *
     * @param exchange HttpServerExchange
     * @param email user email
     * @return Result of user object in JSON
     */
    public static Result<String> getUserByEmail(HttpServerExchange exchange, String email) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"queryUserByEmail\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\"}}", email);
        if (config.isPortalByServiceUrl()) {
            return callQueryExchangeUrl(command, exchange, config.getPortalQueryServiceUrl());
        } else {
            return callQueryExchange(command, exchange);
        }

    }

    /**
     * Get User by email with exchange, a specific url and email
     *
     * @param exchange HttpServerExchange
     * @param url url to a specific host
     * @param email user email
     * @return Result of user object in JSON
     * @deprecated Please use portal.yml config PortalByServiceUrl indicator
     */
    public static Result<String> getUserByEmail(HttpServerExchange exchange, String url, String email) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"queryUserByEmail\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\"}}", email);
        return callQueryExchangeUrl(command, exchange, url);
    }

    /**
     * Get User by email between service to service invocation. It is mainly called from light-spa-4j social login handlers.
     * The token will be a bootstrap client credential token so that there is no user_id in the JWT to bypass the match
     * verification. This is an internal method that is called between portal services and a client credential token must
     * be provided.
     *
     * @param email email
     * @param token a client credential JWT token
     * @return Result of user
     */
    public static Result<String> getUserByEmail(String email, String token) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"queryUserByEmail\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\"}}", email);
        if (config.isPortalByServiceUrl()) {
            return callQueryTokenUrl(command, token, config.getPortalQueryServiceUrl());
        } else {
            return callQueryWithToken(command, token);
        }

    }

    /**
     * Get User email by id with the current credentials in the exchange. This means the same user is
     * trying to get its email from userId. Or an Admin user is doing so with an authorization code
     * token.
     *
     * @param exchange HttpServerExchange
     * @param userId userId
     * @return Result of user email
     */
    public static Result<String> getUserById(HttpServerExchange exchange, String userId) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"queryUserById\",\"version\":\"0.1.0\",\"data\":{\"userId\":\"%s\"}}", userId);
        if (config.isPortalByServiceUrl()) {
            return callQueryExchangeUrl(command, exchange, config.getPortalQueryServiceUrl());
        } else {
            return callQueryExchange(command, exchange);
        }
    }

    /**
     * Get User email by id with the same credentials token. It must be the same user or an admin user. Given there is
     * a url in the parameter, that means this is coming from the Kafka lookup already so the exact url is specified.
     *
     * @param exchange HttpServerExchange
     * @param url to a specific host
     * @param userId userId
     * @return Result of user email
     */
    public static Result<String> getUserById(HttpServerExchange exchange, String url, String userId) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"queryUserById\",\"version\":\"0.1.0\",\"data\":{\"userId\":\"%s\"}}", userId);
        return callQueryExchangeUrl(command, exchange, url);
    }

    /**
     * Get User email by userId between service to service invocation. The token will be a client credential
     * token so that there is no user_id in the JWT to bypass the match verification. This is an internal
     * method that is called between portal services and a client credential token must be provided.
     *
     * @param userId userId
     * @param token a client credential JWT token
     * @return Result of user email
     */
     public static Result<String> getUserById(String userId, String token) {
         final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"queryUserById\",\"version\":\"0.1.0\",\"data\":{\"userId\":\"%s\"}}", userId);
         if (config.isPortalByServiceUrl()) {
             return callQueryTokenUrl(command, token, config.getPortalQueryServiceUrl());
         } else {
             return callQueryWithToken(command, token);
         }
     }

    /**
     * Get User by userType and entityId with the current credentials in the exchange. This means the same user is
     * trying to get its user object from userType and entityId. Or an Admin user is doing so with an authorization code
     * token.
     *
     * @param exchange HttpServerExchange
     * @param userType userType
     * @param entityId entityId
     * @return Result of user email
     */
    public static Result<String> getUserByTypeEntityId(HttpServerExchange exchange, String userType, String entityId) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"queryUserByTypeEntityId\",\"version\":\"0.1.0\",\"data\":{\"userType\":\"%s\",\"entityId\":\"%s\"}}", userType, entityId);
        if (config.isPortalByServiceUrl()) {
            return callQueryExchangeUrl(command, exchange, config.getPortalQueryServiceUrl());
        } else {
            return callQueryExchange(command, exchange);
        }
    }

    /**
     * Get User by userType and entityId with the same credentials token. It must be the same user or an admin user. Given there is
     * an url in the parameter, that means this is coming from the Kafka lookup already so the exact url is specified.
     *
     * @param exchange HttpServerExchange
     * @param url to a specific host
     * @param userType userType
     * @param entityId entityId
     * @return Result of user email
     */
    public static Result<String> getUserByTypeEntityId(HttpServerExchange exchange, String url, String userType, String entityId) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"queryUserByTypeEntityId\",\"version\":\"0.1.0\",\"data\":{\"userType\":\"%s\",\"entityId\":\"%s\"}}", userType, entityId);
        return callQueryExchangeUrl(command, exchange, url);
    }

    /**
     * Get User by userType and entityId between service to service invocation. The token will be a client credential
     * token so that there is no user_id in the JWT to bypass the match verification. This is an internal
     * method that is called between portal services and a client credential token must be provided.
     *
     * @param userType user type
     * @param entityId entity id
     * @param token a client credential JWT token
     * @return Result of user email
     */
    public static Result<String> getUserByTypeEntityId(String userType, String entityId, String token) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"queryUserByTypeEntityId\",\"version\":\"0.1.0\",\"data\":{\"userType\":\"%s\",\"entityId\":\"%s\"}}", userType, entityId);
        if (config.isPortalByServiceUrl()) {
            return callQueryTokenUrl(command, token, config.getPortalQueryServiceUrl());
        } else {
            return callQueryWithToken(command, token);
        }
    }


    /**
     * Get Nonce for the user by email. The result also indicates the user exists in the system.
     *
     * @param exchange HttpServerExchange
     * @param userId user id
     * @return Result of user object in JSON
     */
    public static Result<String> getNonceByUserId(HttpServerExchange exchange, String userId) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"getNonceByUserId\",\"version\":\"0.1.0\",\"data\":{\"userId\":\"%s\"}}", userId);
        if (config.isPortalByServiceUrl()) {
            return callQueryExchangeUrl(command, exchange, config.getPortalQueryServiceUrl());
        } else {
            return callQueryExchange(command, exchange);
        }
    }

    /**
     * Get Nonce for the user by email. The result also indicates the user exists in the system.
     *
     * @param exchange HttpServerExchange
     * @param url url to a specific host
     * @param userId user id
     * @return Result of user object in JSON
     */
    public static Result<String> getNonceByUserId(HttpServerExchange exchange, String url, String userId) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"getNonceByUserId\",\"version\":\"0.1.0\",\"data\":{\"userId\":\"%s\"}}", userId);
        return callQueryExchangeUrl(command, exchange, url);
    }

    /**
     * Get city object from a combination of country, province and city. The result also indicates the city exists in the system.
     *
     * @param exchange HttpServerExchange
     * @param country of the city
     * @param province of the city
     * @param city name of the city
     * @return Result of city object in JSON
     */
    public static Result<String> getCity(HttpServerExchange exchange, String country, String province, String city) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"covid\",\"action\":\"getCity\",\"version\":\"0.1.0\",\"data\":{\"country\":\"%s\",\"province\":\"%s\",\"city\":\"%s\"}}", country, province, city);
        if (config.isPortalByServiceUrl()) {
            return callQueryExchangeUrl(command, exchange, config.getPortalQueryServiceUrl());
        } else {
            return callQueryExchange(command, exchange);
        }
    }

    /**
     * Get password by email for login.
     *
     * @param url url to a specific host
     * @param email email of the user
     * @param password password of the user
     * @return Result of user password
     */
    public static Result<String> loginUser(String url, String email, String password) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"loginUser\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\",\"password\":\"%s\"}}", email, password);
        return callQueryUrl(command, url);
    }

    /**
     * Get private messages for the user by email. The result contains a list of messages.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param email user email
     * @return Result of message list in JSON
     */
    public static Result<String> getMessageByEmail(HttpServerExchange exchange, String url, String email) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"getPrivateMessage\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\"}}", email);
        return callQueryExchangeUrl(command, exchange, url);
    }

    /**
     * Get notification for the user by email. The result contains a list of notifications.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param email user email
     * @return Result of message list in JSON
     */
    public static Result<String> getNotificationByEmail(HttpServerExchange exchange, String url, String email) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"getNotification\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\"}}", email);
        return callQueryExchangeUrl(command, exchange, url);
    }

    /**
     * Get covid entity for the user by key. The result contains an entity object.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param email entity email
     * @return Result of message list in JSON
     */
    public static Result<String> getEntity(HttpServerExchange exchange, String url, String email) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"covid\",\"action\":\"getEntity\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\"}}", email);
        return callQueryExchangeUrl(command, exchange, url);
    }

    /**
     * Get status by email. The result contains an status object.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param email entity email
     * @return Result of status object in JSON
     */
    public static Result<String> getStatusByEmail(HttpServerExchange exchange, String url, String email) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"covid\",\"action\":\"getStatusByEmail\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\"}}", email);
        return callQueryExchangeUrl(command, exchange, url);
    }

    /**
     * Get status by email with a client credentials token. The result contains an status object.
     *
     * @param token Jwt token
     * @param url host of instance that contains the state store
     * @param email entity email
     * @return Result of status object in JSON
     */
    public static Result<String> getStatusByEmail(String url, String email, String token) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"covid\",\"action\":\"getStatusByEmail\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\"}}", email);
        return callQueryTokenUrl(command, token, url);
    }

    /**
     * Get website by email. The result contains an website object.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param email entity email
     * @return Result of website object in JSON
     */
    public static Result<String> getWebsiteByEmail(HttpServerExchange exchange, String url, String email) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"covid\",\"action\":\"getWebsiteByEmail\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\"}}", email);
        return callQueryExchangeUrl(command, exchange, url);
    }

    /**
     * Get website by email with a client credentials token. The result contains a website object.
     *
     * @param token Jwt token
     * @param url host of instance that contains the state store
     * @param email entity email
     * @return Result of website object in JSON
     */
    public static Result<String> getWebsiteByEmail(String url, String email, String token) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"covid\",\"action\":\"getWebsiteByEmail\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\"}}", email);
        return callQueryTokenUrl(command, token, url);
    }

    /**
     * Get payment by email. The result contains a list of payments.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param email entity email
     * @return Result of payments object in JSON
     */
    public static Result<String> getPaymentByEmail(HttpServerExchange exchange, String url, String email) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"getPayment\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\"}}", email);
        return callQueryExchangeUrl(command, exchange, url);
    }

    /**
     * Get payment by email. The result contains a list of payments. This is for API to API
     * invocation with client credentials token.
     *
     * @param url host of instance that contains the state store
     * @param email entity email
     * @param token JWT token
     * @return Result of payments object in JSON
     */
    public static Result<String> getPaymentByEmail(String url, String email, String token) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"getPayment\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\"}}", email);
        return callQueryTokenUrl(command, token, url);
    }

    /**
     * Get customer orders by email. The result contains a list of orders for the customer email.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param email entity email
     * @param offset entry offset
     * @param limit entry limit
     * @return Result of orders object in JSON
     */
    public static Result<String> getCustomerOrderByEmail(HttpServerExchange exchange, String url, String email, int offset, int limit) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"getCustomerOrder\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\",\"offset\":%d,\"limit\":%d}}", email, offset, limit);
        return callQueryExchangeUrl(command, exchange, url);
    }

    /**
     * Get merchant orders by email. The result contains a list of orders for the merchant email.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param email entity email
     * @param orderStatus order status
     * @param offset order offset
     * @param limit order limit
     * @return Result of orders object in JSON
     */
    public static Result<String> getMerchantOrderByEmail(HttpServerExchange exchange, String url, String email, String orderStatus, int offset, int limit) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"user\",\"action\":\"getMerchantOrder\",\"version\":\"0.1.0\",\"data\":{\"email\":\"%s\",\"status\":\"%s\",\"offset\":%d,\"limit\":%d}}", email, orderStatus, offset, limit);
        return callQueryExchangeUrl(command, exchange, url);
    }

    /**
     * Get client detail by clientId. The result contains a map of properties.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param clientId client id
     * @return Result of client object in JSON
     */
    public static Result<String> getClientById(HttpServerExchange exchange, String url, String clientId) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"client\",\"action\":\"getClientById\",\"version\":\"0.1.0\",\"data\":{\"clientId\":\"%s\"}}", clientId);
        return callQueryExchangeUrl(command, exchange, url);
    }

    /**
     * Get client detail by clientId. The result contains a map of properties.
     *
     * @param token access token
     * @param url host of instance that contains the state store
     * @param clientId client id
     * @return Result of client object in JSON
     */
    public static Result<String> getClientById(String token, String url, String clientId) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"client\",\"action\":\"getClientById\",\"version\":\"0.1.0\",\"data\":{\"clientId\":\"%s\"}}", clientId);
        return callQueryTokenUrl(s, token, url);
    }

    /**
     * Get client by clientId with client credentials token from oauth-kafka. The token will be a client credential
     * token so that there is no user_id in the JWT to bypass the match verification. This is an internal method
     * that is called between oauth and portal services and a client credential token must be provided.
     *
     * @param clientId client Id
     * @param token a client credential JWT token
     * @return Result of client
     */
    public static Result<String> getClientById(String token, String clientId) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"client\",\"action\":\"getClientById\",\"version\":\"0.1.0\",\"data\":{\"clientId\":\"%s\"}}", clientId);
        if (config.isPortalByServiceUrl()) {
            return callQueryTokenUrl(s, token, config.getPortalQueryServiceUrl());
        } else {
            return callQueryWithToken(s, token);
        }
    }

    /**
     * Get client by providerId and clientId with client credentials token from oauth-kafka. The token will be a client credential
     * token so that there is no user_id in the JWT to bypass the match verification. This is an internal method that is called
     * between oauth-kafka and portal services and a client credential token must be provided.
     *
     * @param providerId provider id
     * @param clientId client id
     * @param token a client credential JWT token
     * @return Result of client
     */
    public static Result<String> getClientByProviderClientId(String token, String providerId, String clientId) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"oauth\",\"action\":\"getClientByProviderClientId\",\"version\":\"0.1.0\",\"data\":{\"providerId\":\"%s\",\"clientId\":\"%s\"}}", providerId, clientId);
        if (config.isPortalByServiceUrl()) {
            return callQueryTokenUrl(s, token, config.getPortalQueryServiceUrl());
        } else {
            return callQueryWithToken(s, token);
        }
    }

    /**
     * Get client for host with exchange and url. The result contains a list of client.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param offset of the records
     * @param limit of the records
     * @return Result list of client in JSON
     */
    public static Result<String> getClient(HttpServerExchange exchange, String url, String host, int offset, int limit) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"client\",\"action\":\"getClient\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get service for host with exchange, url and optional style. The result contains a list of service.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param style service style
     * @param offset of the records
     * @param limit of the records
     * @return Result list of service in JSON
     */
    public static Result<String> getService(HttpServerExchange exchange, String url, String host, String style, int offset, int limit) {
        String s;
        if(style != null) {
            s = String.format("{\"host\":\"lightapi.net\",\"service\":\"service\",\"action\":\"getService\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"style\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, style, offset, limit);
        } else {
            s = String.format("{\"host\":\"lightapi.net\",\"service\":\"service\",\"action\":\"getService\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        }
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get service detail by serviceId. The result contains a map of properties.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param serviceId service id
     * @return Result of client object in JSON
     */
    public static Result<String> getServiceById(HttpServerExchange exchange, String url, String serviceId) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"service\",\"action\":\"getServiceById\",\"version\":\"0.1.0\",\"data\":{\"serviceId\":\"%s\"}}", serviceId);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get service roles by serviceId. The result contains a map of role to role for dropdown menu on the UI.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param serviceId service id
     * @return Result of role/role map in JSON
     */
    public static Result<String> getServiceRoleById(HttpServerExchange exchange, String url, String serviceId) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"service\",\"action\":\"getServiceRoleById\",\"version\":\"0.1.0\",\"data\":{\"serviceId\":\"%s\"}}", serviceId);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get refreshToken detail. The result contains a map of properties.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param refreshToken refreshToken
     * @return Result of refreshToken object in JSON
     */
    public static Result<String> getRefreshTokenDetail(HttpServerExchange exchange, String url, String refreshToken) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"oauth\",\"action\":\"getRefreshTokenDetail\",\"version\":\"0.1.0\",\"data\":{\"refreshToken\":\"%s\"}}", refreshToken);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get refresh tokens for host with exchange and url. The result contains a list of refresh tokens.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param offset of the records
     * @param limit of the records
     * @return Result list of refresh tokens in JSON
     */
    public static Result<String> getRefreshToken(HttpServerExchange exchange, String url, String host, int offset, int limit) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"oauth\",\"action\":\"getRefreshToken\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get authCode detail. The result contains a map of properties.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param authCode auth code
     * @param derived boolean flag to derive the auth code
     * @return Result of authCode object in JSON
     */
    public static Result<String> getAuthCodeDetail(HttpServerExchange exchange, String url, String hostId, String authCode, boolean derived) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"oauth\",\"action\":\"getAuthCodeDetail\",\"version\":\"0.1.0\",\"data\":{\"hostId\":\"%s\",\"authCode\":\"%s\",\"derived\":%b}}", hostId, authCode, derived);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get authCode detail by authCode with client credentials token from oauth-kafka. The token will be a client
     * credentials token so that there is no user_id in the JWT to bypass the match verification. This is an internal
     * method that is called between oauth and portal services and a client credential token must be provided.
     *
     * @param authCode Auth Code
     * @param token a client credential JWT token
     * @param derive boolean flag to derive the auth code
     * @return Result of authCode
     */
    public static Result<String> getAuthCodeDetail(String authCode, String token, boolean derive) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"oauth\",\"action\":\"getAuthCodeDetail\",\"version\":\"0.1.0\",\"data\":{\"authCode\":\"%s\",\"derived\":%b}}", authCode, derive);
        if (config.isPortalByServiceUrl()) {
            return callQueryTokenUrl(s, token, config.getPortalQueryServiceUrl());
        } else {
            return callQueryWithToken(s, token);
        }
    }

    /**
     * Get authCode for host. The result contains a list of auth code object.
     *
     * @param exchange HttpServerExchange
     * @param host host name
     * @param offset of the records
     * @param limit of the records
     * @return Result list of authCode object in JSON
     */
    public static Result<String> getAuthCode(HttpServerExchange exchange, String host, int offset, int limit) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"oauth\",\"action\":\"getAuthCode\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        if (config.isPortalByServiceUrl()) {
            return callQueryExchangeUrl(s, exchange, config.getPortalQueryServiceUrl());
        } else {
            return callQueryExchange(s, exchange);
        }
    }

    /**
     * Get authCode for host with exchange and url. The result contains a list of auth code object.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param offset of the records
     * @param limit of the records
     * @return Result list of authCode object in JSON
     */
    public static Result<String> getAuthCode(HttpServerExchange exchange, String url, String host, int offset, int limit) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"oauth\",\"action\":\"getAuthCode\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get refToken detail. The result contains a map of properties.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param refToken reference token
     * @return Result of refToken object in JSON
     */
    public static Result<String> getRefTokenDetail(HttpServerExchange exchange, String url, String refToken) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"oauth\",\"action\":\"getRefTokenDetail\",\"version\":\"0.1.0\",\"data\":{\"refToken\":\"%s\"}}", refToken);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get refToken detail by reference token with client credentials token from oauth-kafka. The token will be a client
     * credentials token so that there is no user_id in the JWT to bypass the match verification. This is an internal
     * method that is called between oauth and portal services and a client credential token must be provided.
     *
     * @param refToken Reference Token
     * @param token a client credential JWT token
     * @return Result of refToken
     */
    public static Result<String> getRefTokenDetail(String refToken, String token) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"oauth\",\"action\":\"getRefTokenDetail\",\"version\":\"0.1.0\",\"data\":{\"refToken\":\"%s\"}}", refToken);
        if (config.isPortalByServiceUrl()) {
            return callQueryTokenUrl(s, token, config.getPortalQueryServiceUrl());
        } else {
            return callQueryWithToken(s, token);
        }
    }

    /**
     * Get reference tokens for host with exchange and url. The result contains a list of reference tokens.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param offset of the records
     * @param limit of the records
     * @return Result list of reference tokens in JSON
     */
    public static Result<String> getRefToken(HttpServerExchange exchange, String url, String host, int offset, int limit) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"oauth\",\"action\":\"getRefToken\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get provider detail. The result contains a map of properties.
     *
     * @param exchange HttpServerExchange
     * @param url host of instance that contains the state store
     * @param providerId provider Id
     * @return Result of provider object in JSON
     */
    public static Result<String> getProviderDetail(HttpServerExchange exchange, String url, String providerId) {
        String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"oauth\",\"action\":\"getProviderDetail\",\"version\":\"0.1.0\",\"data\":{\"providerId\":\"%s\"}}", providerId);
        return callQueryExchangeUrl(command, exchange, url);
    }

    /**
     * Get provider for host with exchange and url. The result contains a list of providers.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param offset of the records
     * @param limit of the records
     * @return Result list of provider object in JSON
     */
    public static Result<String> getProvider(HttpServerExchange exchange, String url, String host, int offset, int limit) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"oauth\",\"action\":\"getProvider\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get a list of hosts from reference api for host dropdowns in the UI.
     *
     * @return Result of list of host object in JSON
     */
    public static Result<String> getHosts() {
        String path = "/r/data?name=host";
        Result<String> result = null;
        SimpleConnectionHolder.ConnectionToken connectionToken = null;
        try {
            String host = cluster.serviceToUrl(Http2Client.HTTPS, config.getPortalReferenceServiceId(), tag, null);
            URI uri = new URI(host);
            connectionToken = client.borrow(uri, Http2Client.WORKER, client.getDefaultXnioSsl(), Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true));
            ClientConnection connection = (ClientConnection) connectionToken.getRawConnection();
            // Create one CountDownLatch that will be reset in the callback function
            final CountDownLatch latch = new CountDownLatch(1);
            // Create an AtomicReference object to receive ClientResponse from callback function
            final AtomicReference<ClientResponse> reference = new AtomicReference<>();
            final ClientRequest request = new ClientRequest().setMethod(Methods.GET).setPath(path);
            request.getRequestHeaders().put(Headers.HOST, "localhost");
            connection.sendRequest(request, client.createClientCallback(reference, latch));
            latch.await();
            int statusCode = reference.get().getResponseCode();
            String body = reference.get().getAttachment(Http2Client.RESPONSE_BODY);
            if(statusCode != 200) {
                Status status = Config.getInstance().getMapper().readValue(body, Status.class);
                result = Failure.of(status);
            } else result = Success.of(body);
        } catch (Exception e) {
            logger.error("Exception:", e);
            Status status = new Status(GENERIC_EXCEPTION, e.getMessage());
            result = Failure.of(status);
        } finally {
            client.restore(connectionToken);
        }
        return result;
    }

    /**
     * Get all the entities associated with a particular host. This method is called from the main getHost
     * endpoint to get remote host if it is not on the local store.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @return Result map of entities in JSON
     */
    public static Result<String> getHost(HttpServerExchange exchange, String url, String host) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"host\",\"action\":\"getHost\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\"}}", host);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get all the entities associated with a particular host. This method is called by the createHost
     * or updateHost command side endpoints.
     *
     * @param exchange HttpServerExchange
     * @param host host name
     * @return Result map of entities in JSON
     */
    public static Result<String> getHost(HttpServerExchange exchange, String host) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"host\",\"action\":\"getHost\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\"}}", host);
        if (config.isPortalByServiceUrl()) {
            return callQueryExchangeUrl(s, exchange, config.getPortalQueryServiceUrl());
        } else {
            return callQueryExchange(s, exchange);
        }
    }

    /**
     * Get host domain associated with a particular hostId. This method is called from the main getHostDomainById
     * endpoint to get host domain based on the hostId.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param hostId host id
     * @return Result map of entities in JSON
     */
    public static Result<String> getHostDomainById(HttpServerExchange exchange, String url, String hostId) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"host\",\"action\":\"getHostDomainById\",\"version\":\"0.1.0\",\"data\":{\"hostId\":\"%s\"}}", hostId);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get host domain associated with a particular hostId. This method is called from the main getHostDomainById
     * endpoint to get host domain based on the hostId.
     *
     * @param exchange HttpServerExchange
     * @param hostId host id
     * @return Result map of entities in JSON
     */
    public static Result<String> getHostDomainById(HttpServerExchange exchange, String hostId) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"host\",\"action\":\"getHostDomainById\",\"version\":\"0.1.0\",\"data\":{\"hostId\":\"%s\"}}", hostId);
        if (config.isPortalByServiceUrl()) {
            return callQueryExchangeUrl(s, exchange, config.getPortalQueryServiceUrl());
        } else {
            return callQueryExchange(s, exchange);
        }
    }

    /**
     * Get category for host and name with exchange and url. The result contains a list of codes.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param name of the category
     * @return Result list of category objects in JSON
     */
    public static Result<String> getCategoryByName(HttpServerExchange exchange, String url, String host, String name) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"category\",\"action\":\"getCategoryByName\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"name\":\"%s\"}}", host, name);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get category list for host with exchange and url. The result contains a list of categories.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @return Result list of category objects in JSON
     */
    public static Result<String> getCategory(HttpServerExchange exchange, String url, String host) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"category\",\"action\":\"getCategory\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\"}}", host);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get blog for host and id with exchange and url. The result contains a blog object.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param id of the blog
     * @return Result the blog object in JSON
     */
    public static Result<String> getBlogById(HttpServerExchange exchange, String url, String host, String id) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"blog\",\"action\":\"getBlogById\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"id\":\"%s\"}}", host, id);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get blog for host and id with exchange. The result contains a blog object.
     *
     * @param exchange HttpServerExchange
     * @param host host name
     * @param id of the blog
     * @return Result the blog object in JSON
     */
    public static Result<String> getBlogById(HttpServerExchange exchange, String host, String id) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"blog\",\"action\":\"getBlogById\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"id\":\"%s\"}}", host, id);
        return callQueryExchange(s, exchange);
    }

    /**
     * Get blog list for host or categories or tags with exchange and url. The result contains a list of detailed blog objects.
     *
     * @param exchange HttpServerExchange
     * @param host host name
     * @param url url of target server
     * @param offset of the records
     * @param limit of the records
     * @param categories list of categories
     * @param tags list of tags
     * @return Result the blog object in JSON
     */
    public static Result<String> getBlogList(HttpServerExchange exchange, String url, String host, int offset, int limit, List<String> categories, List<String> tags) {
        Map<String, Object> map = new HashMap<>();
        map.put("host", "lightapi.net");
        map.put("service", "blog");
        map.put("action", "getBlogList");
        map.put("version", "0.1.0");
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("host", host);
        dataMap.put("offset", offset);
        dataMap.put("limit", limit);
        dataMap.put("categories", categories);
        dataMap.put("tags", tags);
        map.put("data", dataMap);
        return callQueryExchangeUrl(JsonMapper.toJson(map), exchange, url);
    }

    /**
     * Get blog list for host with exchange and url. The result contains a list of blogs.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param offset of the records
     * @param limit of the records
     * @return Result list of blog objects in JSON
     */
    public static Result<String> getBlog(HttpServerExchange exchange, String url, String host, int offset, int limit) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"blog\",\"action\":\"getBlog\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get document for host and id with exchange and url. The result contains a document object.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param id of the document
     * @return Result the document object in JSON
     */
    public static Result<String> getDocumentById(HttpServerExchange exchange, String url, String host, String id) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"document\",\"action\":\"getDocumentById\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"id\":\"%s\"}}", host, id);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get document list for host with exchange and url. The result contains a list of documents.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param offset of the records
     * @param limit of the records
     * @return Result list of document objects in JSON
     */
    public static Result<String> getDocument(HttpServerExchange exchange, String url, String host, int offset, int limit) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"document\",\"action\":\"getDocument\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get news for host and id with exchange and url. The result contains a news object.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param id of the document
     * @return Result the news object in JSON
     */
    public static Result<String> getNewsById(HttpServerExchange exchange, String url, String host, String id) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"news\",\"action\":\"getNewsById\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"id\":\"%s\"}}", host, id);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get news list for host with exchange and url. The result contains a list of news.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param offset of the records
     * @param limit of the records
     * @return Result list of news objects in JSON
     */
    public static Result<String> getNews(HttpServerExchange exchange, String url, String host, int offset, int limit) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"news\",\"action\":\"getNews\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get page for host and id with exchange and url. The result contains a page object.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param id of the page
     * @return Result the page object in JSON
     */
    public static Result<String> getPageById(HttpServerExchange exchange, String url, String host, String id) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"page\",\"action\":\"getPageById\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"id\":\"%s\"}}", host, id);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get page list for host with exchange and url. The result contains a list of page.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param offset of the records
     * @param limit of the records
     * @return Result list of page objects in JSON
     */
    public static Result<String> getPage(HttpServerExchange exchange, String url, String host, int offset, int limit) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"page\",\"action\":\"getPage\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get template for host and id with exchange and url. The result contains a template object.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param id of the template
     * @return Result the template object in JSON
     */
    public static Result<String> getTemplateById(HttpServerExchange exchange, String url, String host, String id) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"template\",\"action\":\"getTemplateById\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"id\":\"%s\"}}", host, id);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get template list for host with exchange and url. The result contains a list of template.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param offset of the records
     * @param limit of the records
     * @return Result list of template objects in JSON
     */
    public static Result<String> getTemplate(HttpServerExchange exchange, String url, String host, int offset, int limit) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"template\",\"action\":\"getTemplate\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get tags list for host and app with url. The result contains a list of tags for the host and app combination.
     *
     * @param url url of target server
     * @param host host name
     * @param app app name
     * @return Result list of tags objects in JSON
     */
    public static Result<String> getTags(String url, String host, String app) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"tag\",\"action\":\"getTags\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"app\":\"%s\",\"local\":true}}", host, app);
        return callQueryUrl(s, url);
    }

    /**
     * Get error list for host with exchange and url. The result contains a list of error codes.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param offset of the records
     * @param limit of the records
     * @return Result list of error objects in JSON
     */
    public static Result<String> getError(HttpServerExchange exchange, String url, String host, int offset, int limit) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"error\",\"action\":\"getError\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get error for host and errorCode with exchange. The result contains a error object.
     *
     * @param exchange HttpServerExchange
     * @param host host name
     * @param url of the target server
     * @param errorCode of the error
     * @return Result the error object in JSON
     */
    public static Result<String> getErrorByCode(HttpServerExchange exchange, String url, String host, String errorCode) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"error\",\"action\":\"getErrorByCode\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"errorCode\":\"%s\"}}", host, errorCode);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get schema list for host with exchange and url. The result contains a list of schema ids.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param offset of the records
     * @param limit of the records
     * @return Result list of schema objects in JSON
     */
    public static Result<String> getJsonSchema(HttpServerExchange exchange, String url, String host, int offset, int limit) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"schema\",\"action\":\"getJsonSchema\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get schema for host and id with exchange and url. The result contains a schema object.
     *
     * @param exchange HttpServerExchange
     * @param host host name
     * @param url of the target server
     * @param id of the schema
     * @return Result the schema object in JSON
     */
    public static Result<String> getJsonSchemaById(HttpServerExchange exchange, String url, String host, String id) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"schema\",\"action\":\"getJsonSchemaById\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"id\":\"%s\"}}", host, id);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get schema for host and id with exchange. The result contains a schema object.
     *
     * @param exchange HttpServerExchange
     * @param host host name
     * @param id of the schema
     * @return Result the schema object in JSON
     */
    public static Result<String> getJsonSchemaById(HttpServerExchange exchange, String host, String id) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"schema\",\"action\":\"getJsonSchemaById\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"id\":\"%s\"}}", host, id);
        if (config.isPortalByServiceUrl()) {
            return callQueryExchangeUrl(s, exchange, config.getPortalQueryServiceUrl());
        } else {
            return callQueryExchange(s, exchange);
        }
    }

    /**
     * Get schema list for host or categories or tags with exchange and url. The result contains a list of detailed schema objects.
     *
     * @param exchange HttpServerExchange
     * @param host host name
     * @param url url of target server
     * @param offset of the records
     * @param limit of the records
     * @param categories list of categories
     * @param tags list of tags
     * @return Result the schema object in JSON
     */
    public static Result<String> getJsonSchemaList(HttpServerExchange exchange, String url, String host, int offset, int limit, List<String> categories, List<String> tags) {
        Map<String, Object> map = new HashMap<>();
        map.put("host", "lightapi.net");
        map.put("service", "schema");
        map.put("action", "getJsonSchemaList");
        map.put("version", "0.1.0");
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("host", host);
        dataMap.put("offset", offset);
        dataMap.put("limit", limit);
        dataMap.put("categories", categories);
        dataMap.put("tags", tags);
        map.put("data", dataMap);
        return callQueryExchangeUrl(JsonMapper.toJson(map), exchange, url);
    }

    /**
     * Get rule for host and ruleId with exchange. The result contains a rule object.
     *
     * @param exchange HttpServerExchange
     * @param host host name
     * @param url of the target server
     * @param ruleId of the rule
     * @return Result the rule object in JSON
     */
    public static Result<String> getRuleById(HttpServerExchange exchange, String url, String host, String ruleId) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"rule\",\"action\":\"getRuleById\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"ruleId\":\"%s\"}}", host, ruleId);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get rule list for host with exchange and url. The result contains a list of rules.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param offset of the records
     * @param limit of the records
     * @return Result list of rule objects in JSON
     */
    public static Result<String> getRuleByHost(HttpServerExchange exchange, String url, String host, int offset, int limit) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"rule\",\"action\":\"getRuleByHost\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"offset\":%s,\"limit\":%s}}", host, offset, limit);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get rule list for host with rule type, exchange and url. The result contains a list of rules.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param ruleType rule type
     * @return Result list of rule objects in JSON
     */
    public static Result<String> getRuleByType(HttpServerExchange exchange, String url, String host, String ruleType) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"rule\",\"action\":\"getRuleByType\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"ruleType\":\"%s\"}}", host, ruleType);
        return callQueryExchangeUrl(s, exchange, url);
    }

    /**
     * Get rule list for host with rule group id, exchange and url. The result contains a list of rules.
     *
     * @param exchange HttpServerExchange
     * @param url url of target server
     * @param host host name
     * @param groupId rule groupId
     * @return Result list of rule objects in JSON
     */
    public static Result<String> getRuleByGroup(HttpServerExchange exchange, String url, String host, String groupId) {
        final String s = String.format("{\"host\":\"lightapi.net\",\"service\":\"rule\",\"action\":\"getRuleByGroup\",\"version\":\"0.1.0\",\"data\":{\"host\":\"%s\",\"groupId\":\"%s\"}}", host, groupId);
        return callQueryExchangeUrl(s, exchange, url);
    }

}

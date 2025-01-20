package net.lightapi.portal;

import com.networknt.client.Http2Client;
import com.networknt.cluster.Cluster;
import com.networknt.config.Config;
import com.networknt.config.JsonMapper;
import com.networknt.monad.Failure;
import com.networknt.monad.Result;
import com.networknt.monad.Success;
import com.networknt.security.JwtVerifier;
import com.networknt.server.Server;
import com.networknt.server.ServerConfig;
import com.networknt.service.SingletonServiceFactory;
import com.networknt.status.Status;
import io.undertow.UndertowOptions;
import io.undertow.client.ClientConnection;
import io.undertow.client.ClientRequest;
import io.undertow.client.ClientResponse;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.OptionMap;

import java.net.URI;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a client utility that is shared by all command side services to update data from
 * the command side. It creates a HTTP/2 connection to the command side and cache that connection
 * until the connection is closed.
 *
 * @author Steve Hu
 */
public class HybridCommandClient {
    static final Logger logger = LoggerFactory.getLogger(HybridCommandClient.class);
    public static final PortalClientConfig config = (PortalClientConfig) Config.getInstance().getJsonObjectConfig(PortalClientConfig.CONFIG_NAME, PortalClientConfig.class);
    static String tag = ServerConfig.getInstance().getEnvironment();
    // Get the singleton Cluster instance
    static Cluster cluster = SingletonServiceFactory.getBean(Cluster.class);
    // Get the singleton Http2Client instance
    static Http2Client client = Http2Client.getInstance();
    static ClientConnection connection;
    {
        if (!config.isPortalByServiceUrl()) {
            String host = cluster.serviceToUrl("https", config.getPortalCommandServiceId(), tag, null);
            try {
                connection = client.connect(new URI(host), Http2Client.WORKER, Http2Client.SSL, Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true)).get();
            } catch (Exception e) {
                logger.error("Exception:", e);
            }
        }

    }
    static final String GENERIC_EXCEPTION = "ERR10014";
    static final String ESTABLISH_CONNECTION_ERROR = "ERR10053";
    static Map<String, ClientConnection> connCache = new ConcurrentHashMap<>();

    public static Result<String> callCommandWithToken(String command, String token) {
        try {
            if(connection == null || !connection.isOpen()) {
                // The connection is close or not created.
                String host = cluster.serviceToUrl("https", config.getPortalCommandServiceId(), tag, null);
                connection = client.connect(new URI(host), Http2Client.WORKER, Http2Client.SSL, Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true)).get();
            }
            return callCommandWithToken(connection, command, token);
        } catch (Exception e) {
            logger.error("Exception:", e);
            Status status = new Status(ESTABLISH_CONNECTION_ERROR, e.getMessage());
            return Failure.of(status);
        }
    }

    public static Result<String> callCommandWithToken(String command, String token, String url) {
        try {
            ClientConnection conn = connCache.get(url);
            if(conn == null || !conn.isOpen()) {
                conn = client.connect(new URI(url), Http2Client.WORKER, Http2Client.SSL, Http2Client.BUFFER_POOL, OptionMap.create(UndertowOptions.ENABLE_HTTP2, true)).get();
                connCache.put(url, conn);
            }
            return callCommandWithToken(conn, command, token);
        } catch (Exception e) {
            logger.error("Exception:", e);
            Status status = new Status(ESTABLISH_CONNECTION_ERROR, e.getMessage());
            return Failure.of(status);
        }
    }

    public  static Result<String> callCommandWithToken(ClientConnection connection, String command, String token) {
        Result<String> result = null;
        try {

            // Create one CountDownLatch that will be reset in the callback function
            final CountDownLatch latch = new CountDownLatch(1);
            // Create an AtomicReference object to receive ClientResponse from callback function
            final AtomicReference<ClientResponse> reference = new AtomicReference<>();
            String message = "/portal/command?cmd=" + URLEncoder.encode(command, "UTF-8");
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
        }
        return result;
    }


    /**
     * Create a refresh token from the oauth-kafka. The token will be a client credential token so that there is no user_id
     * in the JWT to bypass the match verification. This is an internal method that is called between oauth and portal
     * services and a client credential token must be provided.
     *
     * @param refreshTokenMap map contains all the properties including refreshToken
     * @param token a client credential JWT token
     * @return Result of refreshToken
     */
    public static Result<String> createRefreshToken(Map<String, Object> refreshTokenMap, String token) {
        Map<String, Object> commandMap = new HashMap<>();
        commandMap.put("host", "lightapi.net");
        commandMap.put("service", "oauth");
        commandMap.put("action", "createRefreshToken");
        commandMap.put("version", "0.1.0");
        commandMap.put("data", refreshTokenMap);
        String command = JsonMapper.toJson(commandMap);
        if(logger.isTraceEnabled()) logger.trace("command = " + command);
        if (config.isPortalByServiceUrl()) {
            return callCommandWithToken(command, token, config.getPortalCommandServiceUrl());
        } else {
            return callCommandWithToken(command, token);
        }
    }

    /**
     * Create Auth code from oauth-kafka.
     * @param codeMap auth code map
     * @param token access token
     * @return Result of authCode
     */
    public static Result<String> createAuthCode(Map<String, Object> codeMap, String token) {
        Map<String, Object> commandMap = new HashMap<>();
        commandMap.put("host", "lightapi.net");
        commandMap.put("service", "oauth");
        commandMap.put("action", "createAuthCode");
        commandMap.put("version", "0.1.0");
        commandMap.put("data", codeMap);
        if (config.isPortalByServiceUrl()) {
            return callCommandWithToken(JsonMapper.toJson(commandMap), token, config.getPortalCommandServiceUrl());
        } else {
            return callCommandWithToken(JsonMapper.toJson(commandMap), token);
        }

    }

    /**
     * Delete Auth code from oauth-kafka
     * @param authCode auth code
     * @param token access token
     * @return Result of authCode
     */
    public static Result<String> deleteAuthCode(String hostId, String authCode, String token) {
        final String command = String.format("{\"host\":\"lightapi.net\",\"service\":\"oauth\",\"action\":\"deleteAuthCode\",\"version\":\"0.1.0\",\"data\":{\"hostId\":\"%s\",\"authCode\":\"%s\"}}", hostId, authCode);
        if (config.isPortalByServiceUrl()) {
            return callCommandWithToken(command, token, config.getPortalCommandServiceUrl());
        } else {
            return callCommandWithToken(command, token);
        }
    }

    /**
     * Create a reference token from the oauth-kafka. The token will be the mapping between a uuid to a JWT token and
     * it is exchanged on the light-router.
     *
     * @param refTokenMap map contains all the properties including refreshToken
     * @param token a client credential JWT token
     * @return Result of refreshToken
     */
    public static Result<String> createRefToken(Map<String, Object> refTokenMap, String token) {
        Map<String, Object> commandMap = new HashMap<>();
        commandMap.put("host", "lightapi.net");
        commandMap.put("service", "oauth");
        commandMap.put("action", "createRefToken");
        commandMap.put("version", "0.1.0");
        commandMap.put("data", refTokenMap);
        String command = JsonMapper.toJson(commandMap);
        if(logger.isTraceEnabled()) logger.trace("command = " + command);
        if (config.isPortalByServiceUrl()) {
            return callCommandWithToken(command, token, config.getPortalCommandServiceUrl());
        } else {
            return callCommandWithToken(command, token);
        }
    }

    /**
     * Create a sociate user with bootstrap token from light-spa-4j statelessAuthHandler.
     *
     * @param userMap map contains all the properties for the social user
     * @param token a client credential JWT token
     * @return Result of refreshToken
     */
    public static Result<String> createSocialUser(Map<String, Object> userMap, String token) {
        Map<String, Object> commandMap = new HashMap<>();
        commandMap.put("host", "lightapi.net");
        commandMap.put("service", "user");
        commandMap.put("action", "createSocialUser");
        commandMap.put("version", "0.1.0");
        commandMap.put("data", userMap);
        String command = JsonMapper.toJson(commandMap);
        if(logger.isTraceEnabled()) logger.trace("command = " + command);
        if (config.isPortalByServiceUrl()) {
            return callCommandWithToken(command, token, config.getPortalCommandServiceUrl());
        } else {
            return callCommandWithToken(command, token);
        }
    }

}

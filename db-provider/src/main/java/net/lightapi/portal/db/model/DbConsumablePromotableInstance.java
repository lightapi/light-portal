package net.lightapi.portal.db.model;

import com.networknt.utility.UuidUtil;
import net.lightapi.portal.util.YamlMapper;
import org.apache.commons.lang3.tuple.Pair;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.function.Predicate;

public final class DbConsumablePromotableInstance {

    // Exposed Keys
    public static final String PROPERTY_NAME_KEY = "propertyName";
    public static final String PROPERTY_VALUE_KEY = "propertyValue";
    public static final String GENERATED_ID_KEY = "newGeneratedId";
    public static final String API_ID_KEY = "apiId";
    public static final String API_VERSION_KEY = "apiVersion";
    public static final String API_NAME_KEY = "apiName";
    public static final String API_TYPE_KEY = "apiType";
    public static final String API_STATUS_KEY = "apiStatus";
    public static final String API_SERVICE_ID_KEY = "apiServiceId";
    public static final String API_PATH_PREFIXES_KEY = "apiPathPrefixes";
    public static final String APP_ID_KEY = "appId";
    public static final String APP_NAME_KEY = "appName";
    public static final String APP_VERSION_KEY = "appVersion";

    // Internal Keys
    private static final String CONFIGS_KEY = "configs";
    private static final String APIS_KEY = "apis";
    private static final String APPS_KEY = "apps";
    private static final String UID_KEY = "uid";
    private static final String PATH_PREFIXES_KEY = "path_prefixes";
    private static final String NAME = "name";
    private static final String VERSION = "version";
    private static final String STATUS = "status";
    private static final String TYPE = "type";
    private static final String SERVICE_ID = "serviceId";
    private static final String SERVER_CONFIGURATION_NAME = "server";
    private static final String ENVIRONMENT_PROPERTY_NAME = "environment";
    private static final String SERVICE_ID_PROPERTY_NAME = "serviceId";
    private static final String DOT = ".";
    private static final String EMPTY = "";
    private static final String DASH = "-";

    private final UUID hostId;
    private final UUID instanceId;
    private final Map<String, Object> promotableConfigs;
    private final String updateUser;
    private final OffsetDateTime updateTimestamp;

    public DbConsumablePromotableInstance(
        String hostId,
        String instanceId,
        Map<String, Object> promotableConfigs,
        String updateUser,
        String updateTimestamp
    ) {
        this.hostId = UUID.fromString(hostId);
        this.instanceId = UUID.fromString(instanceId);
        this.promotableConfigs = promotableConfigs != null ? new HashMap<>(promotableConfigs) : Collections.emptyMap();
        this.updateUser = updateUser != null ? updateUser.trim() : EMPTY;
        this.updateTimestamp = safeParseOrDefault(updateTimestamp, OffsetDateTime.now());
    }

    public UUID getHostId() {
        return hostId;
    }

    public UUID getInstanceId() {
        return instanceId;
    }

    public String getUpdateUser() {
        return updateUser;
    }

    public OffsetDateTime getUpdateTimestamp() {
        return updateTimestamp;
    }

    public Map<String, Object> getRawPromotableConfigs() {
        return promotableConfigs;
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, String>> getInstanceConfigs() {
        Map<String, Object> instanceConfigs = (Map<String, Object>) promotableConfigs.get(CONFIGS_KEY);
        if (Objects.isNull(instanceConfigs) || instanceConfigs.isEmpty()) {
            return Collections.emptyList();
        }

        return instanceConfigs
            .entrySet()
            .stream()
            .filter(entry -> isPropertyIdentifiedByFullNameQualifiedForMutation().test(entry.getKey()))
            .map(DbConsumablePromotableInstance::getConfigsForDb)
            .filter(Objects::nonNull)
            .toList();
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, String>> getApis() {
        List<Map<String, Object>> apis = (List<Map<String, Object>>) promotableConfigs.get(APIS_KEY);
        if (Objects.isNull(apis) || apis.isEmpty()) {
            return Collections.emptyList();
        }

        return apis
            .stream()
            .filter(api -> {
                var apiIdVersion = getApiIdAndVersion((String) api.get(UID_KEY));
                return apiIdVersion.getLeft() != null && !apiIdVersion.getLeft().isBlank()
                    && apiIdVersion.getRight() != null && !apiIdVersion.getRight().isBlank();
            })
            .map(api -> {
                var apiIdVersion = getApiIdAndVersion((String) api.get(UID_KEY));
                UUID generatedId = UuidUtil.getUUID();
                Map<String, String> map = new HashMap<>();
                map.put(API_ID_KEY, apiIdVersion.getLeft());
                map.put(API_VERSION_KEY, apiIdVersion.getRight());
                map.put(API_NAME_KEY, Objects.nonNull(api.get(NAME)) ? api.get(NAME).toString() : EMPTY);
                map.put(API_STATUS_KEY, Objects.nonNull(api.get(STATUS)) ? api.get(STATUS).toString() : EMPTY);
                map.put(API_TYPE_KEY, Objects.nonNull(api.get(TYPE)) ? api.get(TYPE).toString() : EMPTY);
                map.put(API_SERVICE_ID_KEY, Objects.nonNull(api.get(SERVICE_ID)) ? api.get(SERVICE_ID).toString() : EMPTY);
                map.put(GENERATED_ID_KEY, generatedId.toString());
                return map;
            })
            .toList();
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getInstanceApis() {
        List<Map<String, Object>> apis = (List<Map<String, Object>>) promotableConfigs.get(APIS_KEY);
        if (Objects.isNull(apis) || apis.isEmpty()) {
            return Collections.emptyList();
        }

        return apis
            .stream()
            .filter(api -> {
                var apiIdVersion = getApiIdAndVersion((String) api.get(UID_KEY));
                return apiIdVersion.getLeft() != null && !apiIdVersion.getLeft().isBlank()
                    && apiIdVersion.getRight() != null && !apiIdVersion.getRight().isBlank()
                    && api.get(PATH_PREFIXES_KEY) != null && !((Collection<String>) api.get(PATH_PREFIXES_KEY)).isEmpty();
            })
            .map(api -> {
                var apiIdVersion = getApiIdAndVersion((String) api.get(UID_KEY));
                List<String> pathPrefixes = (List<String>) api.get(PATH_PREFIXES_KEY);
                UUID generatedId = UuidUtil.getUUID();

                Map<String, Object> map = new HashMap<>();
                map.put(API_ID_KEY, apiIdVersion.getLeft());
                map.put(API_VERSION_KEY, apiIdVersion.getRight());
                map.put(GENERATED_ID_KEY, generatedId.toString());
                map.put(API_PATH_PREFIXES_KEY, pathPrefixes);
                return map;
            })
            .toList();
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, String>> getInstanceApiConfigs() {
        List<Map<String, Object>> apis = (List<Map<String, Object>>) promotableConfigs.get(APIS_KEY);
        if (Objects.isNull(apis) || apis.isEmpty()) {
            return Collections.emptyList();
        }

        return apis
            .stream()
            .filter(api -> {
                var apiIdVersion = getApiIdAndVersion((String) api.get(UID_KEY));
                var configs = api.get(CONFIGS_KEY) != null ? (Map<String, Object>) api.get(CONFIGS_KEY) : Collections.emptyMap();
                return apiIdVersion.getLeft() != null && !apiIdVersion.getLeft().isBlank()
                    && apiIdVersion.getRight() != null && !apiIdVersion.getRight().isBlank()
                    && configs != null && !configs.isEmpty();
            })
            .map(api -> {
                var apiIdVersion = getApiIdAndVersion((String) api.get(UID_KEY));
                return ((Map<String, Object>) api.get(CONFIGS_KEY))
                    .entrySet().stream()
                    .map(entry -> {
                        var configMap = getConfigsForDb(entry);
                        if (configMap != null) {
                            configMap.put(API_ID_KEY, apiIdVersion.getLeft());
                            configMap.put(API_VERSION_KEY, apiIdVersion.getRight());
                        }
                        return configMap;
                    })
                    .filter(Objects::nonNull)
                    .toList();
            })
            .filter(collection -> !collection.isEmpty())
            .flatMap(Collection::stream)
            .toList();
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, String>> getInstanceApps() {
        List<Map<String, String>> apps = new ArrayList<>();

        List<Map<String, Object>> appsFromConfigs = (List<Map<String, Object>>) promotableConfigs.get(APPS_KEY);
        if (Objects.nonNull(appsFromConfigs) && !appsFromConfigs.isEmpty()) {
            var standaloneApps = appsFromConfigs.stream()
                .filter(app -> {
                    String appId = (String) app.get(UID_KEY);
                    return appId != null && !appId.isBlank();
                })
                .map(app -> {
                    String appId = (String) app.get(UID_KEY);
                    UUID generatedId = UuidUtil.getUUID();
                    Map<String, String> map = new HashMap<>();
                    map.put(GENERATED_ID_KEY, generatedId.toString());
                    map.put(APP_ID_KEY, appId);
                    map.put(APP_NAME_KEY, Objects.nonNull(app.get(NAME)) ? app.get(NAME).toString() : EMPTY);
                    map.put(APP_VERSION_KEY, Objects.nonNull(app.get(VERSION)) ? app.get(VERSION).toString() : EMPTY);
                    return map;
                })
                .toList();

            apps.addAll(standaloneApps);
        }

        List<Map<String, Object>> apis = (List<Map<String, Object>>) promotableConfigs.get(APIS_KEY);
        if (Objects.nonNull(apis) && !apis.isEmpty()) {
            var appsFromApis = apis.stream()
                .filter(api -> {
                    var appsFromApi = api.get(APPS_KEY) != null ? (List<Map<String, Object>>) api.get(APPS_KEY) : Collections.emptyList();
                    return appsFromApi != null && !appsFromApi.isEmpty();
                })
                .map(api -> (List<Map<String, Object>>) api.get(APPS_KEY))
                .flatMap(Collection::stream)
                .filter(app -> {
                    String appId = (String) app.get(UID_KEY);
                    return appId != null && !appId.isBlank();
                })
                .map(app -> {
                    String appId = (String) app.get(UID_KEY);
                    UUID generatedId = UuidUtil.getUUID();
                    Map<String, String> map = new HashMap<>();
                    map.put(GENERATED_ID_KEY, generatedId.toString());
                    map.put(APP_ID_KEY, appId);
                    map.put(APP_NAME_KEY, Objects.nonNull(app.get(NAME)) ? app.get(NAME).toString() : EMPTY);
                    map.put(APP_VERSION_KEY, Objects.nonNull(app.get(VERSION)) ? app.get(VERSION).toString() : EMPTY);
                    return map;
                })
                .toList();

            apps.addAll(appsFromApis);
        }

        return apps;
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, String>> getInstanceAppConfigs() {
        List<Map<String, Object>> apps = (List<Map<String, Object>>) promotableConfigs.get(APPS_KEY);

        if (Objects.isNull(apps) || apps.isEmpty()) {
            return Collections.emptyList();
        }

        return apps
            .stream()
            .filter(app -> {
                var appId = (String) app.get(UID_KEY);
                var configs = app.get(CONFIGS_KEY) != null ? (Map<String, Object>) app.get(CONFIGS_KEY) : Collections.emptyMap();
                return appId != null && !appId.isBlank()
                    && configs != null && !configs.isEmpty();
            })
            .map(app -> {
                var appId = (String) app.get(UID_KEY);
                return ((Map<String, Object>) app.get(CONFIGS_KEY))
                    .entrySet().stream()
                    .map(entry -> {
                        var configMap = getConfigsForDb(entry);
                        if (configMap != null) {
                            configMap.put(APP_ID_KEY, appId);
                        }
                        return configMap;
                    })
                    .filter(Objects::nonNull)
                    .toList();
            })
            .filter(collection -> !collection.isEmpty())
            .flatMap(Collection::stream)
            .toList();
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, String>> getInstanceAppApis() {
        List<Map<String, Object>> apis = (List<Map<String, Object>>) promotableConfigs.get(APIS_KEY);
        if (Objects.isNull(apis) || apis.isEmpty()) {
            return Collections.emptyList();
        }

        return apis.stream()
            .map(api -> {
                var apiIdVersion = getApiIdAndVersion((String) api.get(UID_KEY));
                List<Map<String, Object>> appsFromApi = api.get(APPS_KEY) != null ? (List<Map<String, Object>>) api.get(APPS_KEY) : Collections.emptyList();

                if (appsFromApi == null || appsFromApi.isEmpty() || apiIdVersion.getLeft() == null || apiIdVersion.getLeft().isBlank() ||
                    apiIdVersion.getRight() == null || apiIdVersion.getRight().isBlank()) {
                    return Collections.<Map<String, String>>emptyList();
                }

                return appsFromApi.stream()
                    .filter(app -> {
                        String appId = (String) app.get(UID_KEY);
                        return appId != null && !appId.isBlank();
                    })
                    .map(app -> {
                        String appId = (String) app.get(UID_KEY);
                        Map<String, String> map = new HashMap<>();
                        map.put(APP_ID_KEY, appId);
                        map.put(API_ID_KEY, apiIdVersion.getLeft());
                        map.put(API_VERSION_KEY, apiIdVersion.getRight());
                        return map;
                    })
                    .toList();
            })
            .filter(collection -> !collection.isEmpty())
            .flatMap(Collection::stream)
            .toList();
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, String>> getInstanceAppApiConfigs() {
        List<Map<String, Object>> apis = (List<Map<String, Object>>) promotableConfigs.get(APIS_KEY);
        if (Objects.isNull(apis) || apis.isEmpty()) {
            return Collections.emptyList();
        }

        return apis.stream()
            .map(api -> {
                var apiIdVersion = getApiIdAndVersion((String) api.get(UID_KEY));
                List<Map<String, Object>> appsFromApi = api.get(APPS_KEY) != null ? (List<Map<String, Object>>) api.get(APPS_KEY) : Collections.emptyList();

                if (appsFromApi == null || appsFromApi.isEmpty() || apiIdVersion.getLeft() == null || apiIdVersion.getLeft().isBlank() ||
                    apiIdVersion.getRight() == null || apiIdVersion.getRight().isBlank()) {
                    return Collections.<Map<String, String>>emptyList();
                }

                return appsFromApi.stream()
                    .filter(app -> {
                        String appId = (String) app.get(UID_KEY);
                        var configs = app.get(CONFIGS_KEY) != null ? (Map<String, Object>) app.get(CONFIGS_KEY) : Collections.emptyMap();
                        return appId != null && !appId.isBlank()
                            && configs != null && !configs.isEmpty();
                    })
                    .map(app -> {
                        String appId = (String) app.get(UID_KEY);
                        return ((Map<String, Object>) app.get(CONFIGS_KEY))
                            .entrySet().stream()
                            .map(entry -> {
                                var configMap = getConfigsForDb(entry);
                                if (configMap != null) {
                                    configMap.put(APP_ID_KEY, appId);
                                    configMap.put(API_ID_KEY, apiIdVersion.getLeft());
                                    configMap.put(API_VERSION_KEY, apiIdVersion.getRight());
                                }
                                return configMap;
                            })
                            .filter(Objects::nonNull)
                            .toList();
                    })
                    .filter(collection -> !collection.isEmpty())
                    .flatMap(Collection::stream)
                    .toList();
            })
            .filter(collection -> !collection.isEmpty())
            .flatMap(Collection::stream)
            .toList();
    }

    private static Pair<String, String> getApiIdAndVersion(String uid) {
        // split uid into id and version (e.g. apiId-apiVersion)
        String[] uidParts = uid != null ? uid.split(DASH, 2) : new String[0];
        String apiId = uidParts.length > 0 ? uidParts[0] : EMPTY;
        String apiVersion = uidParts.length > 1 ? uidParts[1] : EMPTY;
        return Pair.of(apiId, apiVersion);
    }

    private static Map<String, String> getConfigsForDb(Map.Entry<String, Object> entry) {
        String key = entry.getKey();
        Object value = entry.getValue();
        if (Objects.isNull(value)) {
            return null;
        }

        String strValue = valueToString(value);
        if (Objects.isNull(strValue)) {
            return null;
        }

        Map<String, String> map = new HashMap<>();
        map.put(PROPERTY_NAME_KEY, key);
        map.put(PROPERTY_VALUE_KEY, strValue);
        return map;
    }

    private static String valueToString(Object value) {
        if (value instanceof Boolean || value instanceof Number || value instanceof CharSequence) {
            return String.valueOf(value);
        }

        if (value instanceof Map<?,?> || value instanceof Collection<?>) {
            try {
                return YamlMapper.toMinifiedYamlString(value);
            } catch (YamlMapper.YamlConversionException e) {
                return null;
            }
        }

        return null;
    }

    private static OffsetDateTime safeParseOrDefault(String timestamp, OffsetDateTime defaultValue) {
        if (timestamp == null) {
            return defaultValue;
        }

        try {
            return OffsetDateTime.parse(timestamp);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private static Predicate<String> isPropertyIdentifiedByFullNameQualifiedForMutation() {
        return nameToCheck -> getBlacklistedPropertyFullNames()
            .stream()
            .noneMatch(blacklistedName -> blacklistedName.equalsIgnoreCase(nameToCheck));
    }

    private static List<String> getBlacklistedPropertyFullNames() {
        return List.of(
            getServerEnvironmentPropertyFullName(),
            getServerServiceIdPropertyFullName()
        );
    }

    private static String getServerEnvironmentPropertyFullName() {
        return SERVER_CONFIGURATION_NAME
            .concat(DOT)
            .concat(ENVIRONMENT_PROPERTY_NAME);
    }

    private static String getServerServiceIdPropertyFullName() {
        return SERVER_CONFIGURATION_NAME
            .concat(DOT)
            .concat(SERVICE_ID_PROPERTY_NAME);
    }
}

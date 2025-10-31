package com.amazonaws.kvstranscribestreaming;

import com.salesforce.scv.SCVLoggingUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConfigManager {
    private static final Map<String, String> envConfigCache = new ConcurrentHashMap<>();

    /**
     * Configuration object that holds all config values loaded from environment variables
     */
    public static class SecretConfig {
        private final Map<String, String> configValues;
        private final String sourceType;

        private SecretConfig(Map<String, String> configValues, String sourceType) {
            this.configValues = Map.copyOf(configValues);
            this.sourceType = sourceType;
        }

        public String getConfigValue(String key) {
            return configValues.get(key);
        }

        public String getRequiredConfigValue(String key) {
            String value = configValues.get(key);
            if (value == null) {
                throw new RuntimeException("Required configuration key '" + key + "' not found in environment variables");
            }
            return value;
        }

        public String getSourceSecretName() {
            return sourceType;
        }
    }

    /**
     * Get environment-based configuration object.
     * This method loads configuration from environment variables instead of AWS Secrets Manager.
     *
     * @param secretName This parameter is ignored when using environment variables
     * @return SecretConfig object containing all configuration values from environment
     */
    public static SecretConfig getSecretConfig(String secretName) {
        SCVLoggingUtil.debug("com.amazonaws.kvstranscribestreaming.ConfigManager.getSecretConfig",
                SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                "Using environment variables for configuration",
                null);

        // Get config values from environment variables
        Map<String, String> configValues = getConfigValuesFromEnvironment();
        return new SecretConfig(configValues, "environment");
    }

    /**
     * Load configuration values from environment variables
     */
    private static Map<String, String> getConfigValuesFromEnvironment() {
        if (envConfigCache.isEmpty()) {
            synchronized (ConfigManager.class) {
                if (envConfigCache.isEmpty()) {
                    SCVLoggingUtil.debug("com.amazonaws.kvstranscribestreaming.ConfigManager.getConfigValuesFromEnvironment",
                            SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                            "Loading configuration from environment variables",
                            null);

                    // Load all required environment variables
                    envConfigCache.put("WEBSOCKET_ENDPOINT", System.getenv("WEBSOCKET_ENDPOINT"));
                    envConfigCache.put("WEBSOCKET_CONNECTION_TIMEOUT_SECONDS", System.getenv("WEBSOCKET_CONNECTION_TIMEOUT_SECONDS"));
                    envConfigCache.put("WEBSOCKET_MAX_RECONNECT_ATTEMPTS", System.getenv("WEBSOCKET_MAX_RECONNECT_ATTEMPTS"));
                    envConfigCache.put("TRANSCRIBE_REGION", System.getenv("TRANSCRIBE_REGION"));
                    envConfigCache.put("APP_REGION", System.getenv("APP_REGION"));
                    envConfigCache.put("START_SELECTOR_TYPE", System.getenv("START_SELECTOR_TYPE"));

                    // Set default values if not provided
                    if (envConfigCache.get("WEBSOCKET_CONNECTION_TIMEOUT_SECONDS") == null) {
                        envConfigCache.put("WEBSOCKET_CONNECTION_TIMEOUT_SECONDS", "30");
                    }
                    if (envConfigCache.get("WEBSOCKET_MAX_RECONNECT_ATTEMPTS") == null) {
                        envConfigCache.put("WEBSOCKET_MAX_RECONNECT_ATTEMPTS", "3");
                    }
                    if (envConfigCache.get("TRANSCRIBE_REGION") == null) {
                        envConfigCache.put("TRANSCRIBE_REGION", "us-east-1");
                    }
                    if (envConfigCache.get("APP_REGION") == null) {
                        envConfigCache.put("APP_REGION", "us-east-1");
                    }
                    if (envConfigCache.get("START_SELECTOR_TYPE") == null) {
                        envConfigCache.put("START_SELECTOR_TYPE", "FRAGMENT_NUMBER");
                    }

                    SCVLoggingUtil.info("com.amazonaws.kvstranscribestreaming.ConfigManager.getConfigValuesFromEnvironment",
                            SCVLoggingUtil.EVENT_TYPE.TRANSCRIPTION,
                            "Configuration loaded from environment variables: " + envConfigCache.toString(),
                            null);
                }
            }
        }
        return envConfigCache;
    }
}
package com.nodeunify.jupiter.postman.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ulisesbocchio.jasyptspringboot.annotation.EnableEncryptableProperties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Data
@Configuration
@ConfigurationProperties("app")
@EnableEncryptableProperties
public class AppConfig {
    private ConnectConfig connect = new ConnectConfig();

    @Data
    public static class ConnectConfig {
        private SourceConfig source = new SourceConfig();
        private SinkConfig sink = new SinkConfig();
        private List<ConnectorConfig> connector = new ArrayList<>();
    }

    @Data
    public static class SourceConfig {
        private GTAConfig gta = new GTAConfig();
        private TDFConfig tdf = new TDFConfig();
    }

    @Data
    public static class GTAConfig {
        private boolean active;
        private List<FENSConfig> fens = new ArrayList<>();
        private String username;
        private String password;
        private List<String> subscriptions = new ArrayList<>();
    }

    @Data
    public static class FENSConfig {
        private String ip;
        private int port;
    }

    @Data
    public static class TDFConfig {
        private boolean active;
        private Map<String, Long> env = new HashMap<>();
        private String host;
        private String port;
        private String username;
        private String password;
        private String markets;
        private String subscriptions = "";
        private Map<String, String> proxy = new HashMap<>();
    }

    @Data
    public static class SinkConfig {
        private KafkaConfig kafka = new KafkaConfig();
        private RedisConfig redis = new RedisConfig();
    }

    @Data
    public static class KafkaConfig {
        private boolean active;
    }

    @Data
    public static class RedisConfig {
        private boolean active;
    }

    @Data
    public static class ConnectorConfig {
        private String in;
        private List<String> out = new ArrayList<>();
    }

}

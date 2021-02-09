package com.nodeunify.jupiter.postman.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Range;
import com.ulisesbocchio.jasyptspringboot.annotation.EnableEncryptableProperties;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
        private CTPConfig ctp = new CTPConfig();
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
    public static class CTPConfig {
        private List<String> tradingHours;
        
        /**
         * 解析盘中时段
         */
        public List<Range<Integer>> getTimeRanges() {
            final DateTimeFormatter parser = DateTimeFormat.forPattern("HH:mm");
            List<Range<Integer>> timeRanges = new ArrayList<>();
            for (String tradingHour : this.tradingHours) {
                if (tradingHour.contains("-")) {
                    String[] parts = tradingHour.split("-");
                    DateTime startTime = DateTime.parse(parts[0], parser);
                    DateTime endTime = DateTime.parse(parts[1], parser);
                    if (startTime.isAfter(endTime)) {
                        // 夜盘跨日时间段
                        timeRanges.add(Range.atLeast(startTime.getMillisOfDay()));
                        timeRanges.add(Range.lessThan(endTime.getMillisOfDay()));
                    } else {
                        timeRanges.add(Range.closedOpen(startTime.getMillisOfDay(), endTime.getMillisOfDay()));
                    }
                }
            }
            return timeRanges;
        }
    }

    @Data
    public static class SinkConfig {
        private KafkaConfig kafka = new KafkaConfig();
        private RedisConfig redis = new RedisConfig();
        private InfluxConfig influx = new InfluxConfig();
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
    public static class InfluxConfig {
        private boolean active;
    }

    @Data
    public static class ConnectorConfig {
        private String in;
        private List<String> out = new ArrayList<>();
    }

}

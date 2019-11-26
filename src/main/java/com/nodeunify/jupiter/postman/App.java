package com.nodeunify.jupiter.postman;

import com.nodeunify.jupiter.postman.config.AppConfig;
import com.nodeunify.jupiter.postman.connector.DataConnector;
import com.nodeunify.jupiter.postman.sink.ISink;
import com.nodeunify.jupiter.postman.sink.KafkaSink;
import com.nodeunify.jupiter.postman.sink.RedisSink;
import com.nodeunify.jupiter.postman.source.CTPSource;
import com.nodeunify.jupiter.postman.source.GTASource;
import com.nodeunify.jupiter.postman.source.ISource;
import com.nodeunify.jupiter.postman.source.TDFSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.source.InvalidConfigurationPropertyValueException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class App implements CommandLineRunner {

    @Autowired(required = false)
    GTASource gtaSource;

    @Autowired(required = false)
    TDFSource tdfSource;

    @Autowired(required = false)
    CTPSource ctpSource;

    @Autowired(required = false)
    KafkaSink kafkaSink;

    @Autowired(required = false)
    RedisSink redisSink;

    @Autowired
    AppConfig appConfig;

    @Autowired
    DataConnector dataConnector;

    public static void main(String[] args) {
        log.info("Start running Postman App");
        SpringApplication.run(App.class, args);
    }

    private ISource getSource(String in) {
        if ("gta".equals(in)) {
            return gtaSource;
        } else if ("tdf".equals(in)) {
            return tdfSource;
        } else if ("ctp".equals(in)) {
            return ctpSource;
        } else {
            throw new InvalidConfigurationPropertyValueException("app.connect.connector.in", in,
                    "Source is not supported");
        }
    }

    private ISink getSink(String out) {
        if ("kafka".equals(out)) {
            return kafkaSink;
        } else if ("redis".equals(out)) {
            return redisSink;
        } else {
            throw new InvalidConfigurationPropertyValueException("app.connect.connector.out", out,
                    "Sink is not supported");
        }
    }

    @Override
    public void run(String... args) {
        appConfig.getConnect().getConnector().forEach(connector -> {
            ISource source = getSource(connector.getIn());
            ISink[] sinks = connector.getOut().stream().map(this::getSink).toArray(ISink[]::new);
            log.debug("Starting data stream. Source: {}; Sink: {}", connector.getIn(), connector.getOut());
            dataConnector.start(source, sinks);
        });
    }
}

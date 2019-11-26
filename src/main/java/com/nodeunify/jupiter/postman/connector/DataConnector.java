package com.nodeunify.jupiter.postman.connector;

import com.google.protobuf.GeneratedMessageV3;
import com.nodeunify.jupiter.postman.sink.ISink;
import com.nodeunify.jupiter.postman.source.ISource;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import io.reactivex.Flowable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class DataConnector {

    @Async("executorService")
    public void start(ISource source, ISink... sinks) {
        if (source == null) {
            log.error("Data stream cannot be started", new IllegalArgumentException("Data source is not defined"));
            return;
        }
        if (sinks == null || sinks.length == 0) {
            log.error("Data stream cannot be started", new IllegalArgumentException("Data sink is not defined"));
            return;
        }
        try {
            Flowable<GeneratedMessageV3> stream = source.readStream();
            log.debug("Reading data stream from: {}", source.getClass().getSimpleName());
            for (ISink sink : sinks) {
                stream.subscribe(sink);
                log.debug("Writing data stream to: {}", sink.getClass().getSimpleName());
            }
        } catch (Exception e) {
            log.error("Data stream cannot be started", e);
        }

    }

    public void stop() {

    }

}

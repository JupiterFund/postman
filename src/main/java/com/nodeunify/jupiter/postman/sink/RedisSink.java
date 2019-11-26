package com.nodeunify.jupiter.postman.sink;

import com.google.protobuf.GeneratedMessageV3;
// import com.nodeunify.jupiter.datastream.v1.StockData;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@ConditionalOnProperty(value = "app.connect.sink.redis.active", havingValue = "true", matchIfMissing = false)
public class RedisSink implements ISink {

    @Override
    public void accept(GeneratedMessageV3 t) throws Exception {
        // StockData stockData = (StockData) t;
        log.debug("Quotation {}", t);
    }
}

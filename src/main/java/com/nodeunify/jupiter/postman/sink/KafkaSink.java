package com.nodeunify.jupiter.postman.sink;

import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.zip.DataFormatException;

import com.google.protobuf.GeneratedMessageV3;
import com.nodeunify.jupiter.datastream.v1.FutureData;
import com.nodeunify.jupiter.datastream.v1.StockData;
import com.nodeunify.jupiter.datastream.v1.Transaction;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@ConditionalOnProperty(
    value = "app.connect.sink.kafka.active", 
    havingValue = "true", 
    matchIfMissing = false)
public class KafkaSink implements ISink {

    // TODO: 通过配置文件设置监听的Topic
    private static final String KAFKA_TOPIC_STOCK_DATA = "datasource.gta.stockdata";
    private static final String KAFKA_TOPIC_FUTURE_DATA = "test.prod.datasource.ctp.futuredata";
    private static final String KAFKA_TOPIC_TRANSACTION = "datasource.gta.transaction";
    private static final String KAFKA_TOPIC_ORDER = "datasource.gta.order";
    private static final String KAFKA_TOPIC_OrderQueue = "datasource.gta.orderqueue";

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Override
    public void accept(GeneratedMessageV3 message) throws Exception {
        /*
         * Kafka数据结构： Message topic: 数据类型 key: 数据代码 value: 二进制代码
         */
        FieldDescriptor fieldDescriptor = message.getDescriptorForType().findFieldByName("code");
        String key = (String) message.getField(fieldDescriptor);
        String topic = null;
        if (message instanceof StockData) {
            topic = KAFKA_TOPIC_STOCK_DATA;
        }
        if (message instanceof FutureData) {
            topic = KAFKA_TOPIC_FUTURE_DATA;
        }
        if (message instanceof Transaction) {
            topic = KAFKA_TOPIC_TRANSACTION;
        }
        if (topic == null) {
            Exception e = new DataFormatException("Unknown message type");
            log.error("Received invalid message", e);
            throw e;
        }
        log.debug("Write message into kafka. Topic: {}; Key: {}", topic, key);
        kafkaTemplate.send(new ProducerRecord<String, byte[]>(topic, key, message.toByteArray()));
    }

}

package com.nodeunify.jupiter.postman.sink;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.google.protobuf.GeneratedMessageV3;
import com.nodeunify.jupiter.datastream.v1.FutureData;
import com.nodeunify.jupiter.postman.dto.influxdb.FutureDataMeasurement;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@ConditionalOnProperty(value = "app.connect.sink.influx.active", havingValue = "true", matchIfMissing = false)
public class InfluxSink implements ISink {

    @Value("${spring.influxdb.url:}")
    private String url;
    @Value("${spring.influxdb.user:}")
    private String user;
    @Value("${spring.influxdb.password:}")
    private String password;
    @Value("${spring.influxdb.quotation-db:}")
    private String database;
    @Value("${spring.influxdb.rentention-policy:one_month}")
    private String retentionPolicy;
    @Value("${spring.influxdb.batch.actions:}")
    private int influxActions;
    @Value("${spring.influxdb.batch.flush-duration:}")
    private int influxFlushDuration;

    private InfluxDB influxDB;

    @PostConstruct
    public void postConstruct() {
        influxDB = InfluxDBFactory.connect(url, user, password);
        try {
            influxDB.setDatabase(database);
            influxDB.enableBatch(influxActions, influxFlushDuration, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("Exception during Influx DB initializing.", e);
        } finally {
            influxDB.setRetentionPolicy(retentionPolicy);
        }
        influxDB.setLogLevel(InfluxDB.LogLevel.NONE);
    }

    @PreDestroy
    public void preDestroy() {
        influxDB.flush();
        influxDB.close();
    }

    @Override
    public void accept(GeneratedMessageV3 message) throws Exception {
        log.debug("Quotation {}", message);
        if (message instanceof FutureData) {
            FutureData fd = (FutureData) message;
            Point point = build(fd);
            influxDB.write(database, retentionPolicy, point);
        }
    }

    private Point build(FutureData fd) {
        Long currentLong = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        Long ctpLong = ctpTimeConvert(fd.getDate(), fd.getTime());
        int timeDelta = (int) (currentLong - ctpLong);
        // log.debug("{} - {} -- differ: {}", currentLong, ctpLong, timeDelta);
        FutureDataMeasurement fdm = FutureDataMeasurement.builder()
            .code(fd.getCode()).market(String.valueOf(fd.getMarketValue())).ctpDate(fd.getDate())
            .ctpTime(fd.getTime()).timeDelta(timeDelta)
            .tradeDate(String.valueOf(fd.getTradeDate())).preClosePx(priceConvert(fd.getPreClosePx()))
            .openPx(priceConvert(fd.getOpenPx())).highPx(priceConvert(fd.getHighPx()))
            .lowPx(priceConvert(fd.getLowPx())).lastPx(priceConvert(fd.getLastPx()))
            .closePx(priceConvert(fd.getClosePx()))
            .totalVolumeTrade(priceConvert(fd.getTotalVolumeTrade()))
            .totalValueTrade(fd.getTotalValueTrade())
            .preOpenInterest(fd.getPreOpenInterest()).openInterest(fd.getOpenInterest())
            .preSettlePrice(priceConvert(fd.getPreSettlePrice()))
            .settlePrice(priceConvert(fd.getSettlePrice()))
            .priceUpLimit(priceConvert(fd.getPriceUpLimit()))
            .priceDownLimit(priceConvert(fd.getPriceDownLimit()))
            .preDelta(fd.getPreDelta()).currDelta(fd.getCurrDelta())  // bid & offer list in the future
            .auctionPrice(priceConvert(fd.getAuctionPrice()))
            .auctionQty(fd.getAuctionQty())
            .avgPrice(fd.getAvgPrice())
            .build();
        String codePrefix = getCodePrefix(fd.getCode());
        log.debug("Write point into InfluxDB. Measurement: {}", codePrefix);
        // log.debug(fdm.toString());
        Point point = Point.measurement(codePrefix).addFieldsFromPOJO(fdm)
            .time(ctpLong, TimeUnit.MILLISECONDS)
            .build();
        return point;
    }

    private float priceConvert(long rowPrice) {
        return (float) rowPrice / 1000;
    }

    private long ctpTimeConvert(int ctpDate, int ctpTime) {
        int ctpYear = ctpDate / 10000;
        int ctpMonth = (ctpDate % 10000) / 100;
        int ctpDay = ctpDate % 100;
        int ctpHour = ctpTime / 10000_000;
        int ctpMinute = (ctpTime % 10000_000) / 100_000;
        int ctpSecond = (ctpTime % 100_000) / 1000;
        int ctpNano = (ctpTime % 1000) * 1000_000;
        return LocalDateTime.of(ctpYear, ctpMonth, ctpDay, ctpHour, ctpMinute, ctpSecond, ctpNano).toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    private String getCodePrefix(String ctpCode) {
        Pattern rPrefix = Pattern.compile("^([a-zA-Z])+");
        Matcher mPrefix = rPrefix.matcher(ctpCode);
        if (mPrefix.find()) {
            return mPrefix.group(0);
        } else {
            return "others";
        }
    }
}

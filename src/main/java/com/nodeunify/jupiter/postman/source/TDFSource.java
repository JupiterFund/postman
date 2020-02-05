package com.nodeunify.jupiter.postman.source;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.security.auth.login.LoginException;

import com.google.protobuf.GeneratedMessageV3;
import com.nodeunify.jupiter.commons.mapper.DatastreamMapper;
import com.nodeunify.jupiter.datastream.v1.Order;
import com.nodeunify.jupiter.datastream.v1.OrderQueue;
import com.nodeunify.jupiter.datastream.v1.StockData;
import com.nodeunify.jupiter.datastream.v1.Transaction;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import cn.com.wind.td.tdf.DATA_TYPE_FLAG;
import cn.com.wind.td.tdf.TDFClient;
import cn.com.wind.td.tdf.TDF_ENVIRON_SETTING;
import cn.com.wind.td.tdf.TDF_ERR;
import cn.com.wind.td.tdf.TDF_MARKET_DATA;
import cn.com.wind.td.tdf.TDF_MSG;
import cn.com.wind.td.tdf.TDF_MSG_ID;
import cn.com.wind.td.tdf.TDF_OPEN_SETTING;
import cn.com.wind.td.tdf.TDF_PROXY_SETTING;
import cn.com.wind.td.tdf.TDF_PROXY_TYPE;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@ConditionalOnProperty(
    value = "app.connect.source.tdf.active", 
    havingValue = "true", 
    matchIfMissing = false)
public class TDFSource implements ISource {

    private static final Logger latencyLogger = LoggerFactory.getLogger("LatencyLogger");
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("HHmmssSSS");
    private static final DateTimeFormatter printer = DateTimeFormat.forPattern("HH:mm:ss.SSS");

    private boolean quitFlag;
    private TDFClient client;

    @Value("${app.connect.source.tdf.env.heart-beat-interval}")
    private long heartBeatInterval;
    @Value("${app.connect.source.tdf.env.missed-beart-count}")
    private long MissedBeartCount;
    @Value("${app.connect.source.tdf.env.open-time-out}")
    private long openTimeOut;
    @Value("${app.connect.source.tdf.host}")
    private String tdfHost;
    @Value("${app.connect.source.tdf.port}")
    private String tdfPort;
    @Value("${app.connect.source.tdf.username}")
    private String username;
    @Value("${app.connect.source.tdf.password}")
    private String password;
    @Value("${app.connect.source.tdf.markets}")
    private String markets;
    @Value("${app.connect.source.tdf.subscriptions:}")
    private String subscriptions;
    @Value("${app.connect.source.tdf.proxy.host:#{null}}")
    private Optional<String> proxyHost;
    @Value("${app.connect.source.tdf.proxy.port:#{null}}")
    private Optional<String> proxyPort;
    @Value("${app.connect.source.tdf.proxy.username:#{null}}")
    private Optional<String> proxyUsername;
    @Value("${app.connect.source.tdf.proxy.password:#{null}}")
    private Optional<String> proxyPassword;

    @PostConstruct
    public void postConstruct() {
        client = new TDFClient();
        // Environments of TDFClient
        client.setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_HEART_BEAT_INTERVAL, heartBeatInterval);
        client.setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_MISSED_BEART_COUNT, MissedBeartCount);
        client.setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_OPEN_TIME_OUT, openTimeOut);
    }

    @PreDestroy
    public void preDestroy() {
        quitFlag = true;
        int err = client.close();
        if (err != TDF_ERR.TDF_ERR_SUCCESS) {
            log.warn("Close TDF connection failed");
        }
        log.debug("Close TDF connection successful");
    }

    private String formatDateValue(String dateValue) {
        StringBuilder sb = new StringBuilder(dateValue);
        if (sb.length() < 9) {
            sb.insert(0, "0");
        }
        return sb.toString();
    }

    private List<GeneratedMessageV3> mapTo(TDF_MSG tdfMsg) {
        // @formatter:off
        List<GeneratedMessageV3> messages = IntStream.range(0, tdfMsg.getAppHead().getItemCount())
            .mapToObj(i -> tdfMsg.getMsgData(i))
            .map(tdfMsgData -> {
                switch (tdfMsg.getDataType()) {
                case TDF_MSG_ID.MSG_DATA_MARKET:
                    TDF_MARKET_DATA tdfMarketData = tdfMsgData.getMarketData();
                    DateTime genTime = formatter.parseDateTime(formatDateValue(String.valueOf(tdfMarketData.getTime())));
                    DateTime serverTime = formatter.parseDateTime(formatDateValue(String.valueOf(tdfMsg.getServerTime()))).withDate(LocalDate.now());
                    DateTime recvTime = DateTime.now();
                    latencyLogger.trace("TDF StockData", 
                    	"TDF", "StockData", tdfMarketData.getCode(), tdfMarketData.getActionDay(), printer.print(genTime), printer.print(serverTime), printer.print(recvTime));
                    StockData stockData = DatastreamMapper.MAPPER.map(tdfMarketData);
                    return stockData;
                case TDF_MSG_ID.MSG_DATA_ORDER:
                    Order.Builder order = Order.newBuilder();
                    return order.build();
                case TDF_MSG_ID.MSG_DATA_TRANSACTION:
                    Transaction.Builder transaction = Transaction.newBuilder();
                    return transaction.build();
                case TDF_MSG_ID.MSG_DATA_ORDERQUEUE:
                    OrderQueue.Builder orderQueue = OrderQueue.newBuilder();
                    return orderQueue.build();
                case TDF_MSG_ID.MSG_DATA_INDEX:
                case TDF_MSG_ID.MSG_DATA_FUTURE:
                default:
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        // @formatter:on
        return messages;
    }

    @SneakyThrows
    @Override
    public Flowable<GeneratedMessageV3> readStream() {
        quitFlag = false;
        TDF_OPEN_SETTING tdfOpenSetting = new TDF_OPEN_SETTING();
        tdfOpenSetting.setIp(tdfHost);
        tdfOpenSetting.setPort(tdfPort);
        tdfOpenSetting.setUser(username);
        tdfOpenSetting.setPwd(password);
        tdfOpenSetting.setMarkets(markets);
        tdfOpenSetting.setSubScriptions(subscriptions);
        tdfOpenSetting.setTypeFlags(DATA_TYPE_FLAG.DATA_TYPE_NONE);
        tdfOpenSetting.setTime(0);
        tdfOpenSetting.setConnectionID(0);
        int err;
        if (proxyHost.isPresent() && proxyPort.isPresent()) {
            TDF_PROXY_SETTING tdfProxySetting = new TDF_PROXY_SETTING();
            tdfProxySetting.setProxyType(TDF_PROXY_TYPE.TDF_PROXY_HTTP11);
            tdfProxySetting.setProxyHostIp(proxyHost.get());
            tdfProxySetting.setProxyPort(proxyPort.get());
            if (proxyUsername.isPresent()) {
                tdfProxySetting.setProxyUser(proxyUsername.get());
            }
            if (proxyPassword.isPresent()) {
                tdfProxySetting.setProxyPwd(proxyPassword.get());
            }
            err = client.openProxy(tdfOpenSetting, tdfProxySetting);
        } else {
            err = client.open(tdfOpenSetting);
        }
        if (err != TDF_ERR.TDF_ERR_SUCCESS) {
            Exception e = new LoginException();
            log.error("Open TDF connection failed");
            throw e;
        }
        log.debug("Open TDF connection and login successful");
        Flowable<GeneratedMessageV3> flowable = Flowable.create(subscriber -> {
            while (!quitFlag) {
                TDF_MSG tdfMsg = client.getMessage(100);
                if (tdfMsg == null) {
                    continue;
                }
                mapTo(tdfMsg).stream().forEach(message -> subscriber.onNext(message));
            }
            log.debug("RxJava2 Subscriber onComplete");
            subscriber.onComplete();
        }, BackpressureStrategy.BUFFER);
        return flowable
            .subscribeOn(Schedulers.io())
            .publish()
            .autoConnect()
            .observeOn(Schedulers.io());
    }
}

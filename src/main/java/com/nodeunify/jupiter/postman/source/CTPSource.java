package com.nodeunify.jupiter.postman.source;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.nodeunify.jupiter.commons.mapper.DatastreamMapper;
import com.nodeunify.jupiter.datastream.v1.FutureData;
import com.nodeunify.jupiter.trader.ctp.v1.Instrument;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.style.ToStringCreator;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import ctp.thostmduserapi.CThostFtdcDepthMarketDataField;
import ctp.thostmduserapi.CThostFtdcMdApi;
import ctp.thostmduserapi.CThostFtdcMdSpi;
import ctp.thostmduserapi.CThostFtdcReqUserLoginField;
import ctp.thostmduserapi.CThostFtdcRspInfoField;
import ctp.thostmduserapi.CThostFtdcRspUserLoginField;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.schedulers.Schedulers;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@ConditionalOnProperty(value = "app.connect.source.ctp.active", havingValue = "true", matchIfMissing = false)
public class CTPSource implements ISource {

    private static final Logger latencyLogger = LoggerFactory.getLogger("LatencyLogger");
    private static final DateTimeFormatter printer = DateTimeFormat.forPattern("HH:mm:ss.SSS");
    private Set<String> queryUUIDs = Sets.newConcurrentHashSet();

    private CThostFtdcMdApi mdApi;
    private MdSpiImpl mdSpiImpl;

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Value("${spring.kafka.topic.trader.ctp.qry.instrument}")
    private String KAFKA_TOPIC_QUERY_INSTRUMENT;
    @Value("${spring.kafka.topic.trader.ctp.rsp.instrument}")
    private String KAFKA_TOPIC_RESPONSE_INSTRUMENT;
    @Value("${app.connect.source.ctp.md-front:}#{T(java.util.Collections).emptyList()}")
    private List<String> ctpMdAddressList;
    @Value("${app.connect.source.ctp.broker-id}")
    private String brokerId;
    @Value("${app.connect.source.ctp.username}")
    private String userId;
    @Value("${app.connect.source.ctp.password}")
    private String password;
    @Value("${app.connect.source.ctp.subscriptions:}#{T(java.util.Collections).emptyList()}")
    private List<String> subscriptions;
    @Value("#{appConfig.getConnect().getSource().getCtp().getTimeRanges()}")
    private List<Range<Integer>> timeRanges;

    static {
        System.loadLibrary("thostmduserapi_se");
        System.loadLibrary("thostmduserapi_wrap");
    }

    @PostConstruct
    public void postConstruct() {
        mdApi = CThostFtdcMdApi.CreateFtdcMdApi();
        mdSpiImpl = new MdSpiImpl();
        mdApi.RegisterSpi(mdSpiImpl);
        ctpMdAddressList.forEach(ctpMdAddress -> mdApi.RegisterFront(ctpMdAddress));
    }

    @PreDestroy
    public void preDestroy() {
        mdApi.Release();
    }

    @Override
    public Flowable<GeneratedMessageV3> readStream() throws Exception {
        Flowable<GeneratedMessageV3> flowable = Flowable.create(emitter -> {
            mdSpiImpl.setEmitter(emitter);
            mdApi.Init();
            mdApi.Join();
        }, BackpressureStrategy.BUFFER);
        return flowable.subscribeOn(Schedulers.io()).publish().autoConnect().observeOn(Schedulers.io());
    }

    @KafkaListener(id = "instrumentListener", topics = "${spring.kafka.topic.trader.ctp.rsp.instrument}", autoStartup = "false")
    public void listen(ConsumerRecord<String, byte[]> record) {
        Optional<byte[]> recordOptional = Optional.fromNullable(record.value());
        if (recordOptional.isPresent()) {
            try {
                Instrument instrument = Instrument.parseFrom(recordOptional.get());
                if (queryUUIDs.contains(instrument.getUUID())) {
                    String instrumentID = instrument.getInstrumentID();
                    log.debug("Instrument record: {}", instrumentID);
                    String[] subscription = new String[] { instrumentID };
                    // Subscribe all market based on the existing instruments from trader service
                    mdApi.SubscribeMarketData(subscription, 1);
                }
            } catch (InvalidProtocolBufferException e) {
                log.error("Error while parsing instrument message", e);
            }
        }
    }

    class MdSpiImpl extends CThostFtdcMdSpi {

        private FlowableEmitter<GeneratedMessageV3> emitter;

        public void setEmitter(FlowableEmitter<GeneratedMessageV3> emitter) {
            this.emitter = emitter;
        }

        @Override
        public void OnFrontConnected() {
            log.debug("On Front Connected");
            CThostFtdcReqUserLoginField field = new CThostFtdcReqUserLoginField();
            field.setBrokerID(brokerId);
            field.setUserID(userId);
            field.setPassword(password);
            mdApi.ReqUserLogin(field, 0);
        }

        @Override
        public void OnFrontDisconnected(int nReason) {
            log.debug("On Front Disconnected: {}", nReason);
        }

        @Override
        public void OnHeartBeatWarning(int nTimeLapse) {
            log.debug("On Heart Beat Warning: {}", nTimeLapse);
        }

        @Override
        public void OnRspError(CThostFtdcRspInfoField pRspInfo, int nRequestID, boolean bIsLast) {
            log.error("On Response Error: {}, {}", nRequestID, bIsLast);
        }

        public void OnRspUserLogin(CThostFtdcRspUserLoginField pRspUserLogin, CThostFtdcRspInfoField pRspInfo,
                int nRequestID, boolean bIsLast) {
            if (pRspUserLogin != null) {
                log.debug("Brokerid {}", pRspUserLogin.getBrokerID());
            }
            if (pRspInfo != null) {
                log.debug("OnRspUserLogin, ErrorMsg {}", pRspInfo.getErrorMsg());
            }
            log.debug("Time ranges: {}", timeRanges);
            if (subscriptions.isEmpty()) {
                log.debug("Start instrumentListener");
                registry.getListenerContainer("instrumentListener").start();
                log.debug("Send instrument query to CTP trader");
                String uuid = UUID.randomUUID().toString();
                queryUUIDs.add(uuid);
                Instrument instrument = Instrument.newBuilder().setUUID(uuid).setExchangeID("").build();
                kafkaTemplate.send(
                        new ProducerRecord<String, byte[]>(KAFKA_TOPIC_QUERY_INSTRUMENT, instrument.toByteArray()));
            } else {
                // Subscribe given instruments
                // Subscription is only successful one by one for single instrument
                subscriptions.parallelStream().forEach((instrumentID) -> {
                    String[] subscription = new String[] { instrumentID };
                    mdApi.SubscribeMarketData(subscription, 1);
                });
            }
        }

        public void OnRtnDepthMarketData(CThostFtdcDepthMarketDataField pDepthMarketData) {
            if (pDepthMarketData == null) {
                log.error("Received null market data");
            } else {
                trace(pDepthMarketData);
                if (!validate(pDepthMarketData)) {
                    log.warn("Received invalid market data");
                } else {
                    sanitize(pDepthMarketData);
                    String code = pDepthMarketData.getInstrumentID();
                    String actionDay = pDepthMarketData.getActionDay();
                    DateTime genTime = printer.parseDateTime(
                            pDepthMarketData.getUpdateTime() + "." + pDepthMarketData.getUpdateMillisec());
                    DateTime recvTime = DateTime.now();
                    if (isMarketOpen(genTime)) {
                        try {
                            latencyLogger.trace("CTP FutureData", "CTP", "FutureData", code, actionDay,
                                    printer.print(genTime), printer.print(genTime), printer.print(recvTime));
                            FutureData futureData = DatastreamMapper.MAPPER.map(pDepthMarketData);
                            emitter.onNext(futureData);
                        } catch (Exception e) {
                            log.error("Error while mapping market data", e);
                        }
                    } else {
                        log.warn("Market is still closed");
                    }
                }
            }
        }

        /**
         * 判断是否属于盘中时段。如果未定义具体时段，则默认为正常开盘时段。常用于 测试开发环境。如果已定义具体时段，则只在指定时间段内接收数据。
         * 
         * 可根据需要定义多个时间段，例如早盘，夜盘。多个时间段之间以逗号分隔。 时间段定义格式可参照: {起始时间 - 截至时间},{起始时间 - 截至时间},
         * ...
         * 
         * @param dateTime
         * @return boolean
         */
        private boolean isMarketOpen(DateTime dateTime) {
            return timeRanges.size() == 0 ? true
                    : timeRanges.stream().filter(timeRange -> timeRange.contains(dateTime.getMillisOfDay())).findAny()
                            .isPresent();
        }

        /**
         * 检查收到的原始数据，判断是否有效。
         * 
         * @param pDepthMarketData
         * @return boolean
         */
        private boolean validate(CThostFtdcDepthMarketDataField pDepthMarketData) {
            if (Strings.isNullOrEmpty(pDepthMarketData.getActionDay())) {
                return false;
            }
            return true;
        }

        /**
         * 按需调整原始数据的部分字段，以满足后期进一步转换的需要。
         * 
         * @param pDepthMarketData
         */
        private void sanitize(CThostFtdcDepthMarketDataField pDepthMarketData) {
            // 问题: 数据转型时发生溢出错误
            // 原因: SimNow全真环境返回的数据含有很多字段的值是9223372036854775807。
            // 猜测SimNow将Long型的最大值作为字段double型字段的默认值。当jupiter-commons转换为Int型时发生错误。
            // 临时解决: 将CurrDelta和PreDelta两个Int型字段设置为0。产品环境也存在此问题。
            // 最终方法：在jupiter-commons中加入数据验证，或者为溢出错误设置默认值。
            pDepthMarketData.setCurrDelta(0.0);
            pDepthMarketData.setPreDelta(0.0);
        }

        /**
         * 跟踪输出所有原始数据
         * 
         * @param pDepthMarketData
         */
        private void trace(CThostFtdcDepthMarketDataField pDepthMarketData) {
            String marketData = new ToStringCreator(pDepthMarketData)
                    .append("InstrumentID", pDepthMarketData.getInstrumentID())
                    .append("ExchangeID", pDepthMarketData.getExchangeID())
                    .append("ExchangeInstID", pDepthMarketData.getExchangeInstID())
                    .append("ActionDay", pDepthMarketData.getActionDay())
                    .append("TradingDay", pDepthMarketData.getTradingDay())
                    .append("UpdateTime", pDepthMarketData.getUpdateTime())
                    .append("UpdateMillisec", pDepthMarketData.getUpdateMillisec())
                    .append("LastPrice", pDepthMarketData.getLastPrice())
                    .append("OpenPrice", pDepthMarketData.getOpenPrice())
                    .append("ClosePrice", pDepthMarketData.getClosePrice())
                    .append("PreClosePrice", pDepthMarketData.getPreClosePrice())
                    .append("HighestPrice", pDepthMarketData.getHighestPrice())
                    .append("LowestPrice", pDepthMarketData.getLowestPrice())
                    .append("AveragePrice", pDepthMarketData.getAveragePrice())
                    .append("Turnover", pDepthMarketData.getTurnover()).append("Volume", pDepthMarketData.getVolume())
                    .append("UpperLimitPrice", pDepthMarketData.getUpperLimitPrice())
                    .append("LowerLimitPrice", pDepthMarketData.getLowerLimitPrice())
                    .append("PreDelta", pDepthMarketData.getPreDelta())
                    .append("CurrDelta", pDepthMarketData.getCurrDelta())
                    .append("PreOpenInterest", pDepthMarketData.getPreOpenInterest())
                    .append("OpenInterest", pDepthMarketData.getOpenInterest())
                    .append("PreSettlementPrice", pDepthMarketData.getPreSettlementPrice())
                    .append("SettlementPrice", pDepthMarketData.getSettlementPrice())
                    .append("AskPrice1", pDepthMarketData.getAskPrice1())
                    .append("AskPrice2", pDepthMarketData.getAskPrice2())
                    .append("AskPrice3", pDepthMarketData.getAskPrice3())
                    .append("AskPrice4", pDepthMarketData.getAskPrice4())
                    .append("AskPrice5", pDepthMarketData.getAskPrice5())
                    .append("AskVolume1", pDepthMarketData.getAskVolume1())
                    .append("AskVolume2", pDepthMarketData.getAskVolume2())
                    .append("AskVolume3", pDepthMarketData.getAskVolume3())
                    .append("AskVolume4", pDepthMarketData.getAskVolume4())
                    .append("AskVolume5", pDepthMarketData.getAskVolume5())
                    .append("BidPrice1", pDepthMarketData.getBidPrice1())
                    .append("BidPrice2", pDepthMarketData.getBidPrice2())
                    .append("BidPrice3", pDepthMarketData.getBidPrice3())
                    .append("BidPrice4", pDepthMarketData.getBidPrice4())
                    .append("BidPrice5", pDepthMarketData.getBidPrice5())
                    .append("BidVolume1", pDepthMarketData.getBidVolume1())
                    .append("BidVolume2", pDepthMarketData.getBidVolume2())
                    .append("BidVolume3", pDepthMarketData.getBidVolume3())
                    .append("BidVolume4", pDepthMarketData.getBidVolume4())
                    .append("BidVolume5", pDepthMarketData.getBidVolume5()).toString();
            log.trace("marketData {}", marketData);
        }
    }
}

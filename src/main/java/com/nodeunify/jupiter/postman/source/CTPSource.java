package com.nodeunify.jupiter.postman.source;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.google.common.base.Optional;
import com.google.protobuf.GeneratedMessageV3;
import com.nodeunify.jupiter.commons.mapper.DatastreamMapper;
import com.nodeunify.jupiter.datastream.v1.FutureData;

import org.apache.kafka.clients.consumer.ConsumerRecord;
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
@ConditionalOnProperty(
    value = "app.connect.source.ctp.active", 
    havingValue = "true", 
    matchIfMissing = false)
public class CTPSource implements ISource {

    private static final Logger latencyLogger = LoggerFactory.getLogger("LatencyLogger");
    private static final DateTimeFormatter printer = DateTimeFormat.forPattern("HH:mm:ss.SSS");

    private CThostFtdcMdApi mdApi;
    private MdSpiImpl mdSpiImpl;

    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Value("#{'tcp://' + '${app.connect.source.ctp.ip}' + ':' + '${app.connect.source.ctp.port}'}")
    private String ctpMdAddress;
    @Value("${app.connect.source.ctp.broker-id}")
    private String brokerId;
    @Value("${app.connect.source.ctp.username}")
    private String userId;
    @Value("${app.connect.source.ctp.password}")
    private String password;
    @Value("${app.connect.source.ctp.subscriptions:}#{T(java.util.Collections).emptyList()}")
    private List<String> subscriptions;
    
    static{
		System.loadLibrary("thostmduserapi_se");
		System.loadLibrary("thostmduserapi_wrap");
	}

    @PostConstruct
    public void postConstruct() {
        mdApi = CThostFtdcMdApi.CreateFtdcMdApi();
        mdSpiImpl = new MdSpiImpl();
        mdApi.RegisterSpi(mdSpiImpl);
		mdApi.RegisterFront(ctpMdAddress);
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

    @KafkaListener(id = "instrumentListener", topics = "${spring.kafka.topic.instrument}", autoStartup = "false")
    public void listen(ConsumerRecord<String, String> record) {
        Optional<String> recordOptional = Optional.fromNullable(record.value());
        if (recordOptional.isPresent()) {
            log.debug("Instrument record: {}", recordOptional.get());
            String[] instrumentIDs = new String[] {recordOptional.get()};
            // Subscribe all market based on the existing instruments from trader service
            mdApi.SubscribeMarketData(instrumentIDs, 1);
        }
    }

    class MdSpiImpl extends CThostFtdcMdSpi {

        private FlowableEmitter<GeneratedMessageV3> emitter;

        public void setEmitter(FlowableEmitter<GeneratedMessageV3> emitter) {
            this.emitter = emitter;
        }

        public void OnFrontConnected() {
            log.debug("On Front Connected");
            CThostFtdcReqUserLoginField field = new CThostFtdcReqUserLoginField();
            field.setBrokerID(brokerId);
            field.setUserID(userId);
            field.setPassword(password);
            mdApi.ReqUserLogin(field, 0);
        }

        public void OnRspUserLogin(CThostFtdcRspUserLoginField pRspUserLogin, CThostFtdcRspInfoField pRspInfo,
			int nRequestID, boolean bIsLast) {
            if (pRspUserLogin != null) {
                log.debug("Brokerid {}", pRspUserLogin.getBrokerID());
            }
            if (pRspInfo != null) {
                log.debug("OnRspUserLogin, ErrorMsg {}", pRspInfo.getErrorMsg());
            }
            if (subscriptions.isEmpty()) {
                log.debug("Start instrumentListener");
                registry.getListenerContainer("instrumentListener").start();
            } else {
                // Subscribe given instruments
                mdApi.SubscribeMarketData(subscriptions.stream().toArray(String[]::new), 1);
            }
        }

	    public void OnRtnDepthMarketData(CThostFtdcDepthMarketDataField pDepthMarketData) {
            if (pDepthMarketData != null) {
                trace(pDepthMarketData);
                String code = pDepthMarketData.getInstrumentID();
                String actionDay = pDepthMarketData.getActionDay();
                DateTime genTime = printer.parseDateTime(pDepthMarketData.getUpdateTime() + "." + pDepthMarketData.getUpdateMillisec());
                DateTime recvTime = DateTime.now();
                latencyLogger.trace("CTP FutureData", 
                    "CTP", "FutureData", code, actionDay, printer.print(genTime), printer.print(genTime), printer.print(recvTime));
                // 问题: 数据转型时发生溢出错误
                // 原因: SimNow全真环境返回的数据含有很多字段的值是9223372036854775807。
                // 猜测SimNow将Long型的最大值作为字段double型字段的默认值。当jupiter-commons转换为Int型时发生错误。
                // 临时解决: 将CurrDelta和PreDelta两个Int型字段设置为0。产品环境也存在此问题。
                // 最终方法：在jupiter-commons中加入数据验证，或者为溢出错误设置默认值。
                pDepthMarketData.setCurrDelta(0.0);
                pDepthMarketData.setPreDelta(0.0);
                try {
                    FutureData futureData = DatastreamMapper.MAPPER.map(pDepthMarketData);
                    emitter.onNext(futureData);
                } catch (Exception e) {
                    log.error("Error while mapping market data", e);
                }
            } else {
                log.error("Returnd null market data");
            }
        }

        private void trace(CThostFtdcDepthMarketDataField pDepthMarketData) {
            // Issue #2: Trace data shoule be logged by LatencyLogger, however not working
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
                .append("Turnover", pDepthMarketData.getTurnover())
                .append("Volume", pDepthMarketData.getVolume())
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
                .append("BidVolume5", pDepthMarketData.getBidVolume5())
                .toString();
            log.trace("marketData {}", marketData);
        }
    }
}

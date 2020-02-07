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
                    // Issue #2: Trace data shoule be logged by LatencyLogger, however not working
                    // TODO: to be removed
                    log.debug("Mapping error, {}, {}, {}", code, actionDay, genTime);
                    log.error("Error while mapping market data", e);
                }
            } else {
                log.error("Returnd null market data");
            }
        }
    }
}

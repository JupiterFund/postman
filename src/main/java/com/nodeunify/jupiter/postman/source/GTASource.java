package com.nodeunify.jupiter.postman.source;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.security.auth.login.LoginException;

import com.google.protobuf.GeneratedMessageV3;
import com.gta.qts.c2j.adaptee.IGTAQTSApi;
import com.gta.qts.c2j.adaptee.impl.GTAQTSApiBaseImpl;
import com.gta.qts.c2j.adaptee.structure.QTSDataType.MsgType;
import com.gta.qts.c2j.adaptee.structure.QTSDataType.RetCode;
import com.nodeunify.jupiter.postman.config.AppConfig;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.source.InvalidConfigurationPropertyValueException;
import org.springframework.stereotype.Service;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@ConditionalOnProperty(
    value = "app.connect.source.gta.active", 
    havingValue = "true", 
    matchIfMissing = false)
public class GTASource implements ISource {

    private IGTAQTSCallbackExtension callback;
    private IGTAQTSApi baseService;

    @Value("#{appConfig.getConnect().getSource().getGta().getFens()}")
    private List<AppConfig.FENSConfig> fens;
    @Value("${app.connect.source.gta.username}")
    private String username;
    @Value("${app.connect.source.gta.password}")
    private String password;
    @Value("${app.connect.source.gta.subscriptions}")
    private List<String> subscriptions;
    @Value("${app.connect.source.gta.codes}")
    private String codes;

    @PostConstruct
    public void postConstruct() {
        callback = new GTACallback();
        baseService = GTAQTSApiBaseImpl.getInstance().CreateInstance(callback);
        baseService.BaseInit();
        baseService.BaseSetTimeout(30);
        fens.forEach(e -> baseService.BaseRegisterService(e.getIp(), (short) e.getPort()));
    }

    @PreDestroy
    public void preDestroy() {
        baseService.BaseUninit();
        GTAQTSApiBaseImpl.getInstance().ReleaseInstance(baseService);
    }

    @SneakyThrows
    private void login() {
        int ret = baseService.BaseLoginX(username, password, "NetType=0");
        if (RetCode.Ret_Success != RetCode.fetchByCode(ret)) {
            throw new LoginException("Login GTA failed");
        }
    }

    private int getMsgTypeCode(String msgType) {
        switch (msgType) {
        case "SZSEL2_Order":
            return MsgType.SZSEL2_Order.code;
        case "SSEL2_Quotation":
            return MsgType.SSEL2_Quotation.code;
        case "SZSEL2_Quotation":
            return MsgType.SZSEL2_Quotation.code;
        default:
            throw new InvalidConfigurationPropertyValueException("app.connect.source.gta.subscriptions", msgType,
                    "Message type of subscription is not supported");
        }
    }

    @SneakyThrows
    private void subscribe(String msgType) {
        int msgTypeCode = getMsgTypeCode(msgType);
        int ret = baseService.BaseSubscribe(msgTypeCode, codes);
        if (RetCode.Ret_Success != RetCode.fetchByCode(ret)) {
            throw new RuntimeException("Subscribe GTA data failed");
        }
    }

    @SneakyThrows
    @Override
    public Flowable<GeneratedMessageV3> readStream() {
        login();
        log.debug("Login GTA successful");
        subscriptions.forEach(this::subscribe);
        log.debug("Subscribe GTA data successful");
        Flowable<GeneratedMessageV3> flowable = Flowable.create(emitter -> {
            callback.setEmitter(emitter);
        }, BackpressureStrategy.BUFFER);
        return flowable
            .subscribeOn(Schedulers.io())
            .publish()
            .autoConnect()
            .observeOn(Schedulers.io());
    }

}

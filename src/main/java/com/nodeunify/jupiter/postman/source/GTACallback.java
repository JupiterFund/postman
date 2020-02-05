package com.nodeunify.jupiter.postman.source;

import java.io.UnsupportedEncodingException;

import com.google.protobuf.GeneratedMessageV3;
import com.gta.qts.c2j.adaptee.structure.CFFEXL2_Quotation;
import com.gta.qts.c2j.adaptee.structure.CFFEXL2_Static;
import com.gta.qts.c2j.adaptee.structure.CZCEL1_Quotation;
import com.gta.qts.c2j.adaptee.structure.CZCEL1_Static;
import com.gta.qts.c2j.adaptee.structure.DCEL1_ArbiQuotation;
import com.gta.qts.c2j.adaptee.structure.DCEL1_Quotation;
import com.gta.qts.c2j.adaptee.structure.DCEL1_Static;
import com.gta.qts.c2j.adaptee.structure.DCEL2_ArbiQuotation;
import com.gta.qts.c2j.adaptee.structure.DCEL2_MarchPriceQty;
import com.gta.qts.c2j.adaptee.structure.DCEL2_OrderStatistic;
import com.gta.qts.c2j.adaptee.structure.DCEL2_Quotation;
import com.gta.qts.c2j.adaptee.structure.DCEL2_RealTimePrice;
import com.gta.qts.c2j.adaptee.structure.DCEL2_Static;
import com.gta.qts.c2j.adaptee.structure.ESUNNY_Index;
import com.gta.qts.c2j.adaptee.structure.SHFEL1_Quotation;
import com.gta.qts.c2j.adaptee.structure.SHFEL1_Static;
import com.gta.qts.c2j.adaptee.structure.SSEIOL1_Quotation;
import com.gta.qts.c2j.adaptee.structure.SSEIOL1_Static;
import com.gta.qts.c2j.adaptee.structure.SSEL1_Quotation;
import com.gta.qts.c2j.adaptee.structure.SSEL1_Static;
import com.gta.qts.c2j.adaptee.structure.SSEL2_Auction;
import com.gta.qts.c2j.adaptee.structure.SSEL2_Index;
import com.gta.qts.c2j.adaptee.structure.SSEL2_Overview;
import com.gta.qts.c2j.adaptee.structure.SSEL2_Quotation;
import com.gta.qts.c2j.adaptee.structure.SSEL2_Static;
import com.gta.qts.c2j.adaptee.structure.SSEL2_Transaction;
import com.gta.qts.c2j.adaptee.structure.SSE_IndexPress;
import com.gta.qts.c2j.adaptee.structure.SZSEL1_Bulletin;
import com.gta.qts.c2j.adaptee.structure.SZSEL1_Quotation;
import com.gta.qts.c2j.adaptee.structure.SZSEL1_Static;
import com.gta.qts.c2j.adaptee.structure.SZSEL2_Index;
import com.gta.qts.c2j.adaptee.structure.SZSEL2_Order;
import com.gta.qts.c2j.adaptee.structure.SZSEL2_Quotation;
import com.gta.qts.c2j.adaptee.structure.SZSEL2_Static;
import com.gta.qts.c2j.adaptee.structure.SZSEL2_Status;
import com.gta.qts.c2j.adaptee.structure.SZSEL2_Transaction;
import com.nodeunify.jupiter.commons.mapper.DatastreamMapper;
import com.nodeunify.jupiter.datastream.v1.StockData;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.FlowableEmitter;

public class GTACallback implements IGTAQTSCallbackExtension {

    private static final Logger latencyLogger = LoggerFactory.getLogger("LatencyLogger");
    // TODO: check DateTimeFormatter for hour value
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("HHmmssSSS");
    private static final DateTimeFormatter printer = DateTimeFormat.forPattern("HH:mm:ss.SSS");

    private FlowableEmitter<GeneratedMessageV3> emitter;

    public static String byteArr2StringAndTrim(byte[] bytes) {
        try {
            return new String(bytes, 0, bytes.length, "UTF-8").trim();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

    private String formatDateValue(String dateValue) {
        StringBuilder sb = new StringBuilder(dateValue);
        if (sb.length() < 9) {
            sb.insert(0, "0");
        }
        return sb.toString();
    }

    @Override
    public void setEmitter(FlowableEmitter<GeneratedMessageV3> emitter) {
        this.emitter = emitter;
    }

    @Override
    public void OnConnectionState(int arg0, int arg1) {

    }

    @Override
    public void OnLoginState(int arg0) {

    }

    @Override
    public void OnSubscribe_CFFEXL2_Quotation(CFFEXL2_Quotation data) {

    }

    @Override
    public void OnSubscribe_CFFEXL2_Static(CFFEXL2_Static data) {

    }

    @Override
    public void OnSubscribe_CZCEL1_Quotation(CZCEL1_Quotation data) {

    }

    @Override
    public void OnSubscribe_CZCEL1_Static(CZCEL1_Static data) {

    }

    @Override
    public void OnSubscribe_DCEL1_ArbiQuotation(DCEL1_ArbiQuotation data) {

    }

    @Override
    public void OnSubscribe_DCEL1_Quotation(DCEL1_Quotation data) {

    }

    @Override
    public void OnSubscribe_DCEL1_Static(DCEL1_Static data) {

    }

    @Override
    public void OnSubscribe_DCEL2_ArbiQuotation(DCEL2_ArbiQuotation data) {

    }

    @Override
    public void OnSubscribe_DCEL2_MarchPriceQty(DCEL2_MarchPriceQty data) {

    }

    @Override
    public void OnSubscribe_DCEL2_OrderStatistic(DCEL2_OrderStatistic data) {

    }

    @Override
    public void OnSubscribe_DCEL2_Quotation(DCEL2_Quotation data) {

    }

    @Override
    public void OnSubscribe_DCEL2_RealTimePrice(DCEL2_RealTimePrice data) {

    }

    @Override
    public void OnSubscribe_DCEL2_Static(DCEL2_Static data) {

    }

    @Override
    public void OnSubscribe_ESUNNY_Index(ESUNNY_Index data) {

    }

    @Override
    public void OnSubscribe_SHFEL1_Quotation(SHFEL1_Quotation data) {

    }

    @Override
    public void OnSubscribe_SHFEL1_Static(SHFEL1_Static data) {

    }

    @Override
    public void OnSubscribe_SSEIOL1_Quotation(SSEIOL1_Quotation data) {

    }

    @Override
    public void OnSubscribe_SSEIOL1_Static(SSEIOL1_Static data) {

    }

    @Override
    public void OnSubscribe_SSEL1_Quotation(SSEL1_Quotation data) {

    }

    @Override
    public void OnSubscribe_SSEL1_Static(SSEL1_Static data) {

    }

    @Override
    public void OnSubscribe_SSEL2_Auction(SSEL2_Auction data) {

    }

    @Override
    public void OnSubscribe_SSEL2_Index(SSEL2_Index data) {

    }

    @Override
    public void OnSubscribe_SSEL2_Overview(SSEL2_Overview data) {

    }

    @Override
    public void OnSubscribe_SSEL2_Quotation(SSEL2_Quotation data) {
        // @formatter:off
        long time = data.Time % 1000000000;
        // TODO: 测试
        int actionDay = Integer.parseInt(("" + data.Time).substring(0, 8));
        DateTime genTime = formatter.parseDateTime(formatDateValue(String.valueOf(time)));
        DateTime serverTime = formatter.parseDateTime(formatDateValue(String.valueOf(data.LocalTimeStamp)));
        DateTime recvTime = DateTime.now();
        String code = byteArr2StringAndTrim(data.Symbol);
        latencyLogger.trace("GTA StockData", 
            "GTA", "StockData", code, actionDay, printer.print(genTime), printer.print(serverTime), printer.print(recvTime));
        StockData stockData = DatastreamMapper.MAPPER.map(data);
        emitter.onNext(stockData);
        // @formatter:on
    }

    @Override
    public void OnSubscribe_SSEL2_Static(SSEL2_Static data) {

    }

    @Override
    public void OnSubscribe_SSEL2_Transaction(SSEL2_Transaction data) {

    }

    @Override
    public void OnSubscribe_SSE_IndexPress(SSE_IndexPress data) {

    }

    @Override
    public void OnSubscribe_SZSEL1_Bulletin(SZSEL1_Bulletin data) {

    }

    @Override
    public void OnSubscribe_SZSEL1_Quotation(SZSEL1_Quotation data) {

    }

    @Override
    public void OnSubscribe_SZSEL1_Static(SZSEL1_Static data) {

    }

    @Override
    public void OnSubscribe_SZSEL2_Index(SZSEL2_Index data) {

    }

    @Override
    public void OnSubscribe_SZSEL2_Order(SZSEL2_Order data) {

    }

    @Override
    public void OnSubscribe_SZSEL2_Quotation(SZSEL2_Quotation data) {
        // @formatter:off
        long time = data.Time % 1000000000;
        // TODO: 测试
        int actionDay = Integer.parseInt(("" + data.Time).substring(0, 8));
        DateTime genTime = formatter.parseDateTime(formatDateValue(String.valueOf(time)));
        DateTime serverTime = formatter.parseDateTime(formatDateValue(String.valueOf(data.LocalTimeStamp)));
        DateTime recvTime = DateTime.now();
        String code = byteArr2StringAndTrim(data.Symbol);
        latencyLogger.trace("GTA StockData", 
            "GTA", "StockData", code, actionDay, printer.print(genTime), printer.print(serverTime), printer.print(recvTime));
        StockData stockData = DatastreamMapper.MAPPER.map(data);
        emitter.onNext(stockData);
        // @formatter:on
    }

    @Override
    public void OnSubscribe_SZSEL2_Static(SZSEL2_Static data) {

    }

    @Override
    public void OnSubscribe_SZSEL2_Status(SZSEL2_Status data) {

    }

    @Override
    public void OnSubscribe_SZSEL2_Transaction(SZSEL2_Transaction data) {

    }

}

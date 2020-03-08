package com.nodeunify.jupiter.postman.dto.influxdb;

import org.influxdb.annotation.Column;
import org.influxdb.annotation.Measurement;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
@Measurement(name = "ctp")
public class FutureDataMeasurement {
    /*
    社区版 influx 模型设计注意点：
    1. tag 必须为 string 类型
    2. field 值只可以为 string、float、int 或 boolean 中的一种（注意，没有 long、timestamp 类型）
    3. field 名称不可以使用保留字，例如 time
    4. 每条记录必须有一个时间戳 time，类型为 long （精确到毫秒）
     */
    @Column(name = "code", tag = true)
    private String code;

    @Column(name = "market", tag = true)
    private String market;

    @Column(name = "ctp_date")  // 因为 influx 保留字，必须改名
    private int ctpDate;

    @Column(name = "ctp_time")
    private int ctpTime;

    @Column(name = "time_delta")
    private int timeDelta; // 记录时间戳（long型）与实际时间的差值（毫秒）

    @Column(name = "trade_date")
    private String tradeDate;

    @Column(name = "pre_close_px")
    private float preClosePx;

    @Column(name = "open_px")
    private float openPx;

    @Column(name = "high_px")
    private float highPx;

    @Column(name = "low_px")
    private float lowPx;

    @Column(name = "last_px")
    private float lastPx;

    @Column(name = "close_px")
    private float closePx;

    @Column(name = "total_volume_trade")
    private float totalVolumeTrade;

    @Column(name = "total_value_trade")
    private float totalValueTrade;

    @Column(name = "pre_open_interest")
    private float preOpenInterest;

    @Column(name = "open_interest")
    private float openInterest;

    @Column(name = "pre_settle_price")
    private float preSettlePrice;

    @Column(name = "settle_price")
    private float settlePrice;

    @Column(name = "price_up_limit")
    private float priceUpLimit;

    @Column(name = "price_down_limit")
    private float priceDownLimit;

    @Column(name = "pre_delta")
    private int preDelta;

    @Column(name = "curr_delta")
    private int currDelta;

    /* need process
    private List<Long> bidPrice;
    private List<Long> bidQty;
    private List<Long> offerPrice;
    private List<Long> offerQty;
     */

    @Column(name = "auction_price")
    private float auctionPrice;

    @Column(name = "auction_qty")
    private float auctionQty;

    @Column(name = "avg_price")
    private int avgPrice;
}
package com.cryptostream.flink.model;

/**
 * Dữ liệu nến 1 phút (OHLCV) raw từ Binance kline stream.
 * Chỉ lưu khi nến đã đóng (isClosed = true) để tránh ghi record trùng.
 *
 * Schema Binance kline event (field "k"):
 *   t: open time (epoch-ms)
 *   T: close time (epoch-ms)
 *   s: symbol
 *   o: open price
 *   h: high price
 *   l: low price
 *   c: close price
 *   v: base asset volume
 *   n: number of trades
 *   x: is kline closed?
 */
public class KlineRecord {
    public String symbol;
    public long   openTime;
    public long   closeTime;
    public double open;
    public double high;
    public double low;
    public double close;
    public double volume;
    public long   numTrades;

    public KlineRecord() {}

    public KlineRecord(String symbol, long openTime, long closeTime,
                       double open, double high, double low, double close,
                       double volume, long numTrades) {
        this.symbol    = symbol;
        this.openTime  = openTime;
        this.closeTime = closeTime;
        this.open      = open;
        this.high      = high;
        this.low       = low;
        this.close     = close;
        this.volume    = volume;
        this.numTrades = numTrades;
    }

    @Override
    public String toString() {
        return String.format("[Kline] %s O=%.2f H=%.2f L=%.2f C=%.2f V=%.4f trades=%d",
                symbol, open, high, low, close, volume, numTrades);
    }
}

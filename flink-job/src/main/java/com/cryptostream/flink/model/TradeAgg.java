package com.cryptostream.flink.model;

/**
 * Kết quả tổng hợp sau Tumbling Window 1 phút trên stream trades.
 * Mỗi record tương ứng 1 cửa sổ thời gian của 1 symbol.
 *
 * Các trường:
 *  - symbol      : tên cặp giao dịch (VD: BTCUSDT)
 *  - windowStart : epoch-ms bắt đầu cửa sổ
 *  - windowEnd   : epoch-ms kết thúc cửa sổ
 *  - tradeCount  : số lệnh khớp trong cửa sổ
 *  - totalVolume : tổng khối lượng (qty)
 *  - vwap        : Volume-Weighted Average Price = Σ(price*qty) / Σqty
 *  - minPrice    : giá thấp nhất
 *  - maxPrice    : giá cao nhất
 */
public class TradeAgg {
    public String symbol;
    public long   windowStart;
    public long   windowEnd;
    public long   tradeCount;
    public double totalVolume;
    public double vwap;
    public double minPrice;
    public double maxPrice;

    public TradeAgg() {}

    public TradeAgg(String symbol, long windowStart, long windowEnd,
                    long tradeCount, double totalVolume, double vwap,
                    double minPrice, double maxPrice) {
        this.symbol      = symbol;
        this.windowStart = windowStart;
        this.windowEnd   = windowEnd;
        this.tradeCount  = tradeCount;
        this.totalVolume = totalVolume;
        this.vwap        = vwap;
        this.minPrice    = minPrice;
        this.maxPrice    = maxPrice;
    }

    @Override
    public String toString() {
        return String.format("[TradeAgg] %s [%d-%d] count=%d vol=%.4f vwap=%.2f lo=%.2f hi=%.2f",
                symbol, windowStart, windowEnd, tradeCount, totalVolume, vwap, minPrice, maxPrice);
    }
}

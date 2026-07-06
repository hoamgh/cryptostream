package com.cryptostream.flink.model;

/**
 * Kết quả tổng hợp sau Sliding Window 30s (slide 10s) trên stream depth.
 *
 * Các trường tính toán trong cửa sổ:
 *  - symbol        : tên cặp
 *  - windowStart   : epoch-ms bắt đầu cửa sổ
 *  - windowEnd     : epoch-ms kết thúc cửa sổ
 *  - avgSpread     : spread trung bình = avg(bestAsk - bestBid)
 *  - avgBestBid    : giá bid tốt nhất trung bình
 *  - avgBestAsk    : giá ask tốt nhất trung bình
 *  - snapshotCount : số snapshot order book trong cửa sổ
 */
public class DepthAgg {
    public String symbol;
    public long   windowStart;
    public long   windowEnd;
    public double avgSpread;
    public double avgBestBid;
    public double avgBestAsk;
    public long   snapshotCount;

    public DepthAgg() {}

    public DepthAgg(String symbol, long windowStart, long windowEnd,
                    double avgSpread, double avgBestBid, double avgBestAsk,
                    long snapshotCount) {
        this.symbol        = symbol;
        this.windowStart   = windowStart;
        this.windowEnd     = windowEnd;
        this.avgSpread     = avgSpread;
        this.avgBestBid    = avgBestBid;
        this.avgBestAsk    = avgBestAsk;
        this.snapshotCount = snapshotCount;
    }

    @Override
    public String toString() {
        return String.format("[DepthAgg] %s [%d-%d] spread=%.4f bid=%.2f ask=%.2f snapshots=%d",
                symbol, windowStart, windowEnd, avgSpread, avgBestBid, avgBestAsk, snapshotCount);
    }
}

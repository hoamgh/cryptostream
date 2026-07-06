package com.cryptostream.flink.process;

import com.cryptostream.flink.model.TradeAgg;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * ProcessWindowFunction cho Tumbling Window 1 phút trên stream trades.
 *
 * Input  : Tuple2<String, double[]> — (symbol, [price, qty])
 *           Dùng Tuple2 vì keyBy giữ nguyên type của stream sau flatMap.
 *
 * Output : TradeAgg — kết quả tổng hợp 1 phút cho 1 symbol
 */
public class TradeWindowFunction
        extends ProcessWindowFunction<Tuple2<String, double[]>, TradeAgg, String, TimeWindow> {

    @Override
    public void process(String symbol,
                        Context ctx,
                        Iterable<Tuple2<String, double[]>> elements,
                        Collector<TradeAgg> out) {

        long   tradeCount   = 0;
        double totalVolume  = 0.0;
        double sumPriceXQty = 0.0;
        double minPrice     = Double.MAX_VALUE;
        double maxPrice     = -Double.MAX_VALUE;

        for (Tuple2<String, double[]> t : elements) {
            double price = t.f1[0];
            double qty   = t.f1[1];
            tradeCount++;
            totalVolume  += qty;
            sumPriceXQty += price * qty;
            if (price < minPrice) minPrice = price;
            if (price > maxPrice) maxPrice = price;
        }

        double vwap = (totalVolume > 0) ? sumPriceXQty / totalVolume : 0.0;

        out.collect(new TradeAgg(
                symbol,
                ctx.window().getStart(),
                ctx.window().getEnd(),
                tradeCount,
                totalVolume,
                vwap,
                minPrice,
                maxPrice
        ));
    }
}

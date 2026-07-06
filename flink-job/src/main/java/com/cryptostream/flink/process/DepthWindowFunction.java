package com.cryptostream.flink.process;

import com.cryptostream.flink.model.DepthAgg;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * ProcessWindowFunction cho Sliding Window 30s (slide 10s) trên stream depth.
 *
 * Input  : Tuple2<String, double[]> — (symbol, [bestBid, bestAsk])
 * Output : DepthAgg — spread trung bình và giá bid/ask trung bình trong cửa sổ
 */
public class DepthWindowFunction
        extends ProcessWindowFunction<Tuple2<String, double[]>, DepthAgg, String, TimeWindow> {

    @Override
    public void process(String symbol,
                        Context ctx,
                        Iterable<Tuple2<String, double[]>> elements,
                        Collector<DepthAgg> out) {

        long   count      = 0;
        double sumBid     = 0.0;
        double sumAsk     = 0.0;
        double sumSpread  = 0.0;

        for (Tuple2<String, double[]> t : elements) {
            double bid = t.f1[0];
            double ask = t.f1[1];
            count++;
            sumBid    += bid;
            sumAsk    += ask;
            sumSpread += (ask - bid);
        }

        if (count == 0) return;

        out.collect(new DepthAgg(
                symbol,
                ctx.window().getStart(),
                ctx.window().getEnd(),
                sumSpread / count,
                sumBid    / count,
                sumAsk    / count,
                count
        ));
    }
}

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.util.Collector;

public class CollectTotalTipsPerDriverPerHour extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
    @Override
    public void process(
          Long key,
          Context context,
          Iterable<TaxiFare> fares,
          Collector<Tuple3<Long, Long, Float>> out) {
        TaxiFare taxiFare = fares.iterator().next();
        out.collect(Tuple3.of(context.window().getEnd(), key, taxiFare.tip));
    }
}
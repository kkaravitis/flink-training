package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.util.Collector;

/*
 * Wraps the pre-aggregated result into a tuple along with the window's timestamp and key.
 */
public class AddTips extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {

@Override
public void process(
      Long key,
      Context context,
      Iterable<TaxiFare> fares,
      Collector<Tuple3<Long, Long, Float>> out) {

            float sumOfTips = 0F;
            for (TaxiFare f : fares) {
                  sumOfTips += f.tip;
            }
            out.collect(Tuple3.of(context.window().getEnd(), key, sumOfTips));
      }
}

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;

public enum ExecutionStrategy {
    BY_PROCESS_WINDOW_FUNCTION {
        @Override
        public DataStream<Tuple3<Long, Long, Float>> getHourlyTips(DataStream<TaxiFare> fares, TumblingEventTimeWindows oneHourWindowAssigner) {
            return fares
                  .keyBy((TaxiFare fare) -> fare.driverId)
                  .window(oneHourWindowAssigner)
                  .process(new AddTips());
        }
    },
    BY_REDUCE_FUNCTION {
        @Override
        public DataStream<Tuple3<Long, Long, Float>> getHourlyTips(DataStream<TaxiFare> fares, TumblingEventTimeWindows oneHourWindowAssigner) {
            return fares
                  .keyBy((TaxiFare fare) -> fare.driverId)
                  .window(oneHourWindowAssigner)
                  .reduce(new ReduceTotalDriverTips(), new CollectTotalTipsPerDriverPerHour());
        }
    },
    BY_KEYED_PROCESS_FUNCTION {
        @Override
        public DataStream<Tuple3<Long, Long, Float>> getHourlyTips(DataStream<TaxiFare> fares, TumblingEventTimeWindows oneHourWindowAssigner) {
            return fares
                  .keyBy((TaxiFare fare) -> fare.driverId)
                  .process(new PseudoWindow(Time.hours(1)));
        }
    };

    public abstract DataStream<Tuple3<Long, Long, Float>> getHourlyTips(DataStream<TaxiFare> fares, TumblingEventTimeWindows oneHourWindowAssigner);

}

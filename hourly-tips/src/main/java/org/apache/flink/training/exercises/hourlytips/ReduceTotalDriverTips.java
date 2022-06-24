package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;

public class ReduceTotalDriverTips implements ReduceFunction<TaxiFare> {
    public TaxiFare reduce(TaxiFare fare1, TaxiFare fare2) {
        return new TaxiFare(
              0L,
              0L,
              fare1.driverId,
              null,
              null,
              fare1.tip + fare2.tip,
              0L,
              0L);
    }
}
package org.apache.flink.training.exercises.hourlytips;

import java.util.Optional;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.util.Collector;

public class PseudoWindow extends KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>> {

    /**
     * The time window duration. In our example will be one hour as we collect the total tips per hour :)
     */
    private final long durationMsec;

    /***
     * {@link MapState} state for keeping the calculated sum of tips for the driver at the end of each window.
     * The key of the map is the end of the window (millis) and the value is the sum of tips
     */
    private transient MapState<Long, Float> sumOfTipsPerWindow;

    public PseudoWindow(Time duration) {
        durationMsec = duration.toMilliseconds();
    }

    /**
     * Called during initialization
     * @param conf The configuration containing the parameters attached to the contract.
     * @throws Exception
     */
    @Override
    public void open(Configuration conf) throws Exception {
        // Initialize the state structure
        MapStateDescriptor<Long, Float> sumOfTipsPerWindowDesc = new MapStateDescriptor<Long, Float>("sumOfTipsPerWindow", Long.class, Float.class);
        sumOfTipsPerWindow = getRuntimeContext().getMapState(sumOfTipsPerWindowDesc);
    }


    /**
     * Called as each fare arrives to be processed.
     *
     * @param fare The input fare.
     * @param context A {@link Context} that allows querying the timestamp of the element and getting a
     *     {@link TimerService} for registering timers and querying the time. The context is only
     *     valid during the invocation of this method, do not store it.
     * @param out The collector for returning result values.
     * @throws Exception
     */
    @Override
    public void processElement(
          TaxiFare fare,
          Context context,
          Collector<Tuple3<Long, Long, Float>> out) throws Exception {

        TimerService timerService = context.timerService();
        long eventTime = fare.getEventTimeMillis();

        if (eventTime <= timerService.currentWatermark()) {
            // watermarks:  they define when to stop waiting for earlier events
            // A watermark for time t is an assertion that the stream is (probably) now complete up through time t
            // As the eventTime is earlier than the watermark time, the that means that this event window has been passed
            // and this event is late. Its window has already been completed/triggered

        } else { // In case of this is the current window of the event

            // Round up the eventTime to the end of the time window containing this event.
            long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec);

            // Schedule a callback for when the windows has been completed.
            // Registers a timer to be fired when the event time watermark passes the given time.
            timerService.registerEventTimeTimer(endOfWindow);

            // Calculate the new sum of tips of the driver during this time window.
            Float sumOfDriverTipsForWindow = Optional.ofNullable(sumOfTipsPerWindow.get(endOfWindow)).orElse(0F);
            sumOfDriverTipsForWindow += fare.tip;
            sumOfTipsPerWindow.put(endOfWindow, sumOfDriverTipsForWindow);
        }
    }


    /**
     *
     * Called when the current watermark indicates that a window is now complete.
     * When the current time window reaches its end, then we collect its outcome, the sum of tips of the driver during this window
     *
     * @param timestamp The timestamp of the firing timer.
     * @param context An {@link OnTimerContext} that allows querying the timestamp, the {@link TimeDomain},
     * and the key of the firing timer and getting a {@link TimerService} for
     *     registering timers and querying the time. The context is only valid during the invocation
     *     of this method, do not store it.
     * @param out The collector for returning result values.
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
        Long driverId = context.getCurrentKey();
        // Get the result for the hour that just ended
        Float sumOfTips = sumOfTipsPerWindow.get(timestamp);
        out.collect(Tuple3.of(timestamp, driverId, sumOfTips));

        // Clear the map entry from the state
        sumOfTipsPerWindow.remove(timestamp);
    }

}

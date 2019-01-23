// package main.java.f22;


// import org.apache.flink.api.common.state.ValueState;
// import org.apache.flink.api.common.state.ValueStateDescriptor;
// import org.apache.flink.api.java.tuple.Tuple2;
// import org.apache.flink.configuration.Configuration;
// import org.apache.flink.streaming.api.functions.ProcessFunction;
// import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
// import org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext;
// import org.apache.flink.util.Collector;

// import org.json.JSONObject;
// import java.util.List;
// import java.util.ArrayList;

// /**
//  * The implementation of the ProcessFunction that maintains the count and timeouts
//  */
// public class FUUProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

//     /** The state that is maintained by this process function */
//     private ValueState<FUUState> state;

//     @Override
//     public void open(Configuration parameters) throws Exception {
//         state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", FUUState.class));
//     }

//     @Override
//     public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out)
//             throws Exception {

//         // retrieve the current count
//         FUUState current = state.value();
//         if (current == null) {
//             current = new FUUState();
//             current.ids = new ArrayList<String>();
//         }

//         // update the state's count
//         current.count++;

//         // set the state's timestamp to the record's assigned event time timestamp
//         current.lastModified = ctx.timestamp();

//         // write the state back
//         state.update(current);

//         // schedule the next timer 60 seconds from the current event time
//         ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
//     }

//     @Override
//     public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out)
//             throws Exception {

//         // get the state for the key that scheduled the timer
//         CountWithTimestamp result = state.value();

//         // check if this is an outdated timer or the latest timer
//         if (timestamp == result.lastModified + 60000) {
//             // emit the state on timeout
//             out.collect(new Tuple2<String, Long>(result.key, result.count));
//         }
//     }
// }
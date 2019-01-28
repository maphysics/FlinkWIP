package main.java.f22;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

public class CheckEventIds implements CoFlatMapFunction<Tuple2<String,String>, Tuple2<String,JSONObject>, String> {
    private static final long serialVersionUID = 1L;
    List<String> ids;
    String control = "";

    public void flatMap1(Tuple2<String, String> input, Collector<String> col1){
        String message = input.f1;
        if (message.equals("start")){
            // Pause processing event
            control = "pause";
            return;
        } else if (message.equals("stop")) {
            // Start processing event
            control = "processing";
        }

        if (control.equals("pause")) {
        	if(ids.equals(null)) { ids = new ArrayList<>(); }
            ids.add(message);
        }
    }

    public void flatMap2(Tuple2<String,JSONObject> event, Collector<String> col2){
        if (control.equals("processing")) {
            String eventId = event.f1.get("id").toString();
            if (ids.contains(eventId)){
                col2.collect(event.f1.get("id").toString());
            }
        }
    }

}
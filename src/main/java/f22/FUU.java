package main.java.f22;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.common.functions.JoinFunction;
import org.json.JSONObject;

public class FUU {
	public static void main(String[] args) throws Exception {
		String bootstrap_servers = "d995aff1-kafka0.pub.va.eventador.io:9092";
		String sink = "defaultsink";
		String[] opts = {
				"--read-topic", sink,
				"--bootstrap.servers", bootstrap_servers,
				"--group.id", "summat",
			    "--auto.offset.reset", "earliest",
			    "--flink.starting-position", "earliest"
				};
		final ParameterTool params = ParameterTool.fromArgs(opts);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.getConfig().setGlobalJobParameters(params);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        AbstractDeserializationSchema<Tuple1<String>> deserializationSchema = new MyDeserializationSchema();
        FlinkKafkaConsumer011<Tuple1<String>> idConsumer = new FlinkKafkaConsumer011<Tuple1<String>>(
            "defaultsink",
            deserializationSchema,
            params.getProperties());
        DataStream<Tuple1<String>> idStream = env.addSource(idConsumer);

        // Create datastream for processing ``eventstream``
        FlinkKafkaConsumer011<Tuple1<JSONObject>> eventConsumer = new FlinkKafkaConsumer011<Tuple1<JSONObject>>(
            "eventsink",
            new EventDeserializer(),
            params.getProperties());
        DataStream<Tuple1<JSONObject>> eventStream = env.addSource(eventConsumer); 

        // // Create a window containing the ids for the last hour of processing time
        // // Aggregate ids into a List<String> that are the ids
        // idStream
        //     .keyBy(0)
        //     .window(TumblingProcessingTimeWindows.of(Time.hours(1)))
        //     .aggregate(new IdAggregate());
        
        eventStream.join(idStream)
            .where(new KeySelectorJSONObjectId())
            .equalTo(new KeySelectorString())
            .window(TumblingProcessingTimeWindows.of(Time.hours(1)))
            .apply (new JoinFunction<Tuple1<JSONObject>, Tuple1<String>, Tuple1<JSONObject>> (){
                @Override
                public Tuple1<JSONObject> join(Tuple1<JSONObject> first, Tuple1<String> second) {
                    return first;
                }
            });   

        env.execute("FU");
	}
}

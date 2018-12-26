package main.java.f22;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
// Debugging imports
import org.json.JSONObject;
// import org.apache.flink.util.OutputTag;

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
        System.out.println(new JSONObject(params.toMap()).toString(2));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.getConfig().setGlobalJobParameters(params);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        AbstractDeserializationSchema<String> deserializationSchema = new MyDeserializationSchema();
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>(
                params.getRequired("read-topic"),
                deserializationSchema,
                params.getProperties());
        DataStream<String> mStream = env.addSource(consumer);
        
        CheckById cbd = new CheckById();
        mStream
        .map(cbd)
        .print();
        env.execute("FU");
	}
}

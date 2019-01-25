package main.java.experimentation;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.apache.flink.api.common.functions.RichMapFunction;



public class FUU_PlaygroundTwo {

    public static ParameterTool init(){
        String bootstrap_servers = "***";
		String truststore_location = "***";
		String truststore_password = "***";
		String username = "***";
		String password = "***";
		// 
		String module = "org.apache.kafka.common.security.scram.ScramLoginModule";
        String jaasConfig = String.format("%s required username=\"%s\" password=\"%s\";", module, username, password);
		String[] opts = {
				"--read-topic", "defaultsink",
				//"--write-topic", "defaultsink",
				"--bootstrap.servers", bootstrap_servers,
				"--group.id", "summat",
				"--producer.sasl.jaas.config", module,
				"--producer.security.protocol", "SASL_SSL",
				"--security.protocol", "SASL_SSL",
			    "--security.inter.broker.protocol", "SASL_SSL",
			    "--sasl.enabled.mechanisms", "SCRAM-SHA-256",
			    "--sasl.mechanism.inter.broker.protocol", "SCRAM-SHA-256",
				"--sasl.mechanism", "SCRAM-SHA-256",
			    "--sasl.jaas.config", jaasConfig,
			    "--ssl.truststore.type", "jks",
			    "--ssl.truststore.location", truststore_location,
			    "--ssl.truststore.password", truststore_password,
			    "--auto.offset.reset", "earliest",
			    "--flink.starting-position", "earliest"
				};
		return ParameterTool.fromArgs(opts);
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = init();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.getConfig().setGlobalJobParameters(params);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        AbstractDeserializationSchema<Tuple3<Long,String, Object>> deserializationSchema = new MyDeserializationSchemaTuple();
        FlinkKafkaConsumer011<Tuple3<Long, String, Object>> consumer = new FlinkKafkaConsumer011<Tuple3<Long, String, Object>>(
                params.getRequired("read-topic"),
                deserializationSchema,
                params.getProperties());
        DataStream<Tuple3<Long, String, Object>> idStream = env.addSource(consumer);

        // Create datastream for processing ``eventstream``
        FlinkKafkaConsumer011<Tuple3<Long, String, Object>> eventConsumer = new FlinkKafkaConsumer011<Tuple3<Long, String, Object>>(
            "eventsink",
            new EventDeserializerJSONObject(),
            params.getProperties());
        DataStream<Tuple3<Long,String,Object>> eventStream = env.addSource(eventConsumer); 

        CheckById cbd = new CheckById();
        idStream.union(eventStream).keyBy(0).flatMap(cbd).print();
    }

    public static class EventDeserializerJSONObject extends AbstractDeserializationSchema<Tuple3<Long,String,Object>> {
        private static final long serialVersionUID = 1L;
        @Override
        public Tuple3<Long,String,Object> deserialize(byte[] message) throws IOException {
            JSONObject obj = new JSONObject(new String(message, "UTF-8"));
            
            return Tuple3.of(1L, "event", obj);
        }
    }

    public static class MyDeserializationSchemaTuple extends AbstractDeserializationSchema<Tuple3<Long,String,Object>> {
        private static final long serialVersionUID = 1L;
        @Override
        public Tuple3<Long,String, Object> deserialize(byte[] message) throws IOException {
            if (new String(message, "UTF-8") == "pause"){
                return Tuple3.of(1L, "pause", "");
            } else if (new String(message, "UTF-8")== "go") {
                return Tuple3.of(1L, "go", "");
            } else {
                JSONObject obj = new JSONObject(new String(message, "UTF-8"));
                String id = obj.toMap().get("id").toString();
                return Tuple3.of(1L, "id", id);
            }
        }
    }
    
    public static class CheckById extends RichFlatMapFunction<Tuple3<Long,String,Object>, Tuple3<Long,String,Object>> {
        private static final long serialVersionUID = 1L;

        List<String> ids;
        @Override
        public void flatMap(Tuple3<Long,String,Object> input, Collector<Tuple3<Long,String,Object>> result) throws Exception {
            Boolean generating = false;
            List<JSONObject> holding = new ArrayList<>();

            switch (input.f1){
                case "event": 
                    if (generating) {
                        JSONObject event = new JSONObject(input.f2.toString());
                        holding.add(event);
                    } else {
                        JSONObject event = new JSONObject(input.f2.toString());
                        if (ids.contains(event.get("id"))) {
                            result.collect(input);
                        }
                    }
                case "pause":
                    generating = true;
                    ids = new ArrayList<>();
                case "id":
                    ids.add(input.f1);
                case "go":
                    for (JSONObject event : holding){
                        if (ids.contains(event.get("id"))) {
                            result.collect(input);
                        }
                    }
            }
        }

    }
    
}
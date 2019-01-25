package main.java.experimentation;

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
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.json.JSONObject;


public class FUU_PlayGround {

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

        AbstractDeserializationSchema<Tuple2<Long, String>> deserializationSchema = new MyDeserializationSchemaTuple();
        FlinkKafkaConsumer011<Tuple2<Long, String>> consumer = new FlinkKafkaConsumer011<Tuple2<Long, String>>(
                params.getRequired("read-topic"),
                deserializationSchema,
                params.getProperties());
        DataStream<Tuple2<Long, String>> idStream = env.addSource(consumer);

        // Create datastream for processing ``eventstream``
        FlinkKafkaConsumer011<Tuple2<Long, JSONObject>> eventConsumer = new FlinkKafkaConsumer011<Tuple2<Long, JSONObject>>(
            "eventsink",
            new EventDeserializerJSONObject(),
            params.getProperties());
        DataStream<Tuple2<Long,JSONObject>> eventStream = env.addSource(eventConsumer); 

        ConnectedStreams<Tuple2<Long, String>, Tuple2<Long,JSONObject>> colStreams = idStream.connect(eventStream);

        CheckEventIds cei = new CheckEventIds();
        DataStream<JSONObject> selectedEvents = colStreams.keyBy(0, 0).flatMap(cei);

        selectedEvents.print();
    }

    public static class EventDeserializerJSONObject extends AbstractDeserializationSchema<Tuple2<Long,JSONObject>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Long,JSONObject> deserialize(byte[] message) throws IOException {
            JSONObject obj = new JSONObject(new String(message, "UTF-8"));
            
            return Tuple2.of(1L, obj);
        }
    }

    public static class MyDeserializationSchemaTuple extends AbstractDeserializationSchema<Tuple2<Long, String>> {

        private static final long serialVersionUID = 1L;
        @Override
        public Tuple2<Long, String> deserialize(byte[] message) throws IOException {
            JSONObject obj = new JSONObject(new String(message, "UTF-8"));
            String id = obj.toMap().get("id").toString();
            return new Tuple2<Long, String>(1L, id);
        }
    }

    public static class CheckEventIds implements CoFlatMapFunction<Tuple2<Long,String>, Tuple2<Long,JSONObject>, JSONObject> {
        private static final long serialVersionUID = 1L;
        List<String> ids = new ArrayList<>();
        String control = "";

        public void flatMap1(Tuple2<Long, String> input, Collector<JSONObject> col1){
            String message = input.f1;
            if (message == "start"){
                // Pauase processing event
                control = "pause";
            } else if (message == "stop"){
                // Start processing event again
                control = "processing";
            }

            if (control == "pause"){
                ids = new ArrayList<>();
                ids.add(message);
            }
        }

        public void flatMap2(Tuple2<Long,JSONObject> event, Collector<JSONObject> col2){
            if (control == "process"){
                String eventId = event.f1.get("ids").toString();
                if (ids.contains(eventId)){
                    col2.collect(event.f1);
                } 
            }
        }

    }
    
}
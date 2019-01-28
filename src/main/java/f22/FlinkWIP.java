package f22;

import java.util.Properties;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.json.JSONObject;

public class FlinkWIP {
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		ClassLoader classLoader = FlinkWIP.class.getClassLoader();
        String truststore_path = classLoader.getResource("eventador_truststore.jks").getPath();

        // Configure ScramLogin via jaas
        String module = "org.apache.kafka.common.security.scram.ScramLoginModule";
        String jaasConfig = String.format("%s required username=\"%s\" password=\"%s\";", module, params.getRequired("username"), params.getRequired("password"));

        // Flink and Kafka parameters
        Properties kparams = params.getProperties();
        kparams.setProperty("auto.offset.reset", "earliest");
        kparams.setProperty("flink.starting-position", "earliest");
        kparams.setProperty("group.id", params.getRequired("group.id"));
        kparams.setProperty("bootstrap.servers", params.getRequired("bootstrap.servers"));;

        // Topics
        kparams.setProperty("skus-topic", "skus");
        kparams.setProperty("offers-topic", "offers");
        kparams.setProperty("write-topic", "sink2flink");
        
        // SASL parameters
        kparams.setProperty("security.protocol", "SASL_SSL");
        kparams.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        kparams.setProperty("sasl.jaas.config", jaasConfig);
        kparams.setProperty("ssl.truststore.type", "jks");
        kparams.setProperty("ssl.truststore.location", truststore_path);
        kparams.setProperty("ssl.truststore.password", params.getRequired("truststore.password"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(300000);
        env.getConfig().setGlobalJobParameters(params);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        AbstractDeserializationSchema<Tuple2<String, String>> deserializationSchema = new MyDeserializationSchema();
        FlinkKafkaConsumer011<Tuple2<String, String>> consumer = new FlinkKafkaConsumer011<Tuple2<String, String>>(
                params.getRequired("skus-topic"),
                deserializationSchema,
                params.getProperties());
        DataStream<Tuple2<String, String>> idStream = env.addSource(consumer);

        // Create datastream for processing `offers`
        FlinkKafkaConsumer011<Tuple2<String, JSONObject>> eventConsumer = new FlinkKafkaConsumer011<Tuple2<String, JSONObject>>(
        	params.getRequired("offers-topic"),
            new EventDeserializerJSONObject(),
            params.getProperties());
        DataStream<Tuple2<String,JSONObject>> eventStream = env.addSource(eventConsumer); 

        ConnectedStreams<Tuple2<String, String>, Tuple2<String,JSONObject>> colStreams = idStream.connect(eventStream);

        DataStream<String> stream = colStreams
        .keyBy(0, 0)
        .flatMap(new CheckEventIds());

        stream.addSink(new FlinkKafkaProducer011<String>(
                params.getRequired("write-topic"),
                new SimpleStringSchema(),
                params.getProperties()));
        env.execute("FlinkWIP");
    }
}

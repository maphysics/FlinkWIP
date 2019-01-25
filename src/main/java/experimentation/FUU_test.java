package main.java.f22;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.json.JSONObject;
import java.util.List;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
// import org.apache.flink.util.OutputTag;

public class FUU_test {
	public static void main(String[] args) throws Exception {
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
		final ParameterTool params = ParameterTool.fromArgs(opts);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.getConfig().setGlobalJobParameters(params);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        AbstractDeserializationSchema<String> deserializationSchema = new MyDeserializationSchema();
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>(
                params.getRequired("read-topic"),
                deserializationSchema,
                params.getProperties());
        DataStream<String> mStream = env.addSource(consumer);

        // Create datastream for processing ``eventstream``
        FlinkKafkaConsumer011<JSONObject> eventConsumer = new FlinkKafkaConsumer011<JSONObject>(
            "eventsink",
            new EventDeserializerJSONObject(),
            params.getProperties());
        DataStream<JSONObject> eventStream = env.addSource(eventConsumer); 

        CheckById cbd = new CheckById();

        DataStream<List<String>> idsStream = mStream.map(cbd);

        JoinedStreams<JSONObject, JSONObject> result = eventStream
        .join(idsStream)
        .where(new KeySelectorEvents())
        .equalTo(new KeySelectorSelf());

        
        // FlinkKafkaProducer011<Tuple2<String, JSONObject>> myProducer = new FlinkKafkaProducer011<Tuple2<String, JSONObject>>(
        //     "selectedsink",                  // target topic
        //     new KeyedSerializationSchemaSelectedEvents(),
        //     params.getProperties());   // serialization schema
        
        FlinkKafkaProducer011<List<String>> myProducer = new FlinkKafkaProducer011<List<String>>(
            "selectedsink",                  // target topic
            new KeyedSerializationSchemaSelectedEvents(),
            params.getProperties());   // serialization schema

        // versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
        // this method is not available for earlier Kafka versions
        myProducer.setWriteTimestampToKafka(true);
        
        result.addSink(myProducer);

        env.execute("FUU_test");
	}
}

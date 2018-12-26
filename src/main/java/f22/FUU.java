package f22;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.OutputTag;

public class FUU {
	public static void main(String[] args) throws Exception {
		String bootstrap_servers = "localhost:9092";
		String sink = "tufuf";
		// Turning off ssl as local doesn't have it
		// String truststore_location = "***";
		// String truststore_password = "***";
		// String username = "***";
		// String password = "***";
		// 
		// Turning off jaas as my local doesn't use it, add later if still experiencing issues
		// String module = "org.apache.kafka.common.security.scram.ScramLoginModule";
        // String jaasConfig = String.format("%s required username=\"%s\" password=\"%s\";", module, username, password);
		String[] opts = {
				"--read-topic", sink,
				//"--write-topic", "defaultsink",
				"--bootstrap.servers", bootstrap_servers,
				"--group.id", "summat",
				// "--producer.sasl.jaas.config", module,
				// "--producer.security.protocol", "SASL_SSL",
				// "--security.protocol", "SASL_SSL",
			    // "--security.inter.broker.protocol", "SASL_SSL",
			    // "--sasl.enabled.mechanisms", "SCRAM-SHA-256",
			    // "--sasl.mechanism.inter.broker.protocol", "SCRAM-SHA-256",
				// "--sasl.mechanism", "SCRAM-SHA-256",
			    // "--sasl.jaas.config", jaasConfig,
			    // "--ssl.truststore.type", "jks",
			    // "--ssl.truststore.location", truststore_location,
			    // "--ssl.truststore.password", truststore_password,
			    "--auto.offset.reset", "earliest",
			    "--flink.starting-position", "earliest"
				};
		final ParameterTool params = ParameterTool.fromArgs(opts);

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

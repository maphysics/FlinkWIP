// package main.java.experimentation;

// import java.io.IOException;

// import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
// import org.json.JSONObject;
// import org.apache.flink.api.java.tuple.Tuple2;

// public class MyDeserializationSchemaTuple extends AbstractDeserializationSchema<Tuple2<Long, String>> {

// 	private static final long serialVersionUID = 1L;
// 	@Override
// 	public Tuple2<Long, String> deserialize(byte[] message) throws IOException {
// 		JSONObject obj = new JSONObject(new String(message, "UTF-8"));
// 		String id = obj.toMap().get("id").toString();
// 		return new Tuple2<Long, String>(1L, id);
// 	}
// }

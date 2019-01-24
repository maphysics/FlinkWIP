package f22;

import java.io.IOException;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONObject;

public class MyDeserializationSchema extends AbstractDeserializationSchema<Tuple2<String, String>> {
	private static final long serialVersionUID = 1L;

	@Override
    public Tuple2<String, String> deserialize(byte[] message) throws IOException {
		JSONObject obj = new JSONObject(new String(message, "UTF-8"));
		String sku = obj.toMap().get("sku").toString();
        return Tuple2.of(null, sku);
    }
}

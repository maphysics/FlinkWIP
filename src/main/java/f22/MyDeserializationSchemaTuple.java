package main.java.f22;

import java.io.IOException;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.json.JSONObject;
import org.apache.flink.api.java.tuple.Tuple1;

public class MyDeserializationSchemaTuple extends AbstractDeserializationSchema<Tuple1<String>> {

	private static final long serialVersionUID = 1L;
	@Override
    public Tuple1<String> deserialize(byte[] message) throws IOException {
		JSONObject obj = new JSONObject(new String(message, "UTF-8"));
		String id = obj.toMap().get("id").toString();
        return new Tuple1<String>(id);
    }
}

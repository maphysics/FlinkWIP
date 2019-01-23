package main.java.f22;

import java.io.IOException;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.json.JSONObject;

public class MyDeserializationSchema extends AbstractDeserializationSchema<String> {
    private static final long serialVersionUID = 1L;

	@Override
    public String deserialize(byte[] message) throws IOException {
        JSONObject obj = new JSONObject(new String(message, "UTF-8"));
        // Assumption that the flush message does not contain a key id
        if (obj.toMap().containsKey("id")){
            return obj.toMap().get("id").toString();
        } else {
            return "flush";
        }
    }
}
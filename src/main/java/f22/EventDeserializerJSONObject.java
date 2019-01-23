package main.java.f22;

import java.io.IOException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.json.JSONObject;

public class EventDeserializerJSONObject extends AbstractDeserializationSchema<JSONObject> {
    private static final long serialVersionUID = 1L;

	@Override
    public JSONObject deserialize(byte[] message) throws IOException {
        JSONObject obj = new JSONObject(new String(message, "UTF-8"));
        
        return obj;
    }
}

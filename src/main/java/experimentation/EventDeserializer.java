package main.java.experimentation;

import java.io.IOException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.json.JSONObject;
import org.apache.flink.api.java.tuple.Tuple1;

public class EventDeserializer extends AbstractDeserializationSchema<Tuple1<JSONObject>> {
    private static final long serialVersionUID = 1L;

	@Override
    public Tuple1<JSONObject> deserialize(byte[] message) throws IOException {
        JSONObject obj = new JSONObject(new String(message, "UTF-8"));
        
        return new Tuple1<JSONObject>(obj);
    }
}

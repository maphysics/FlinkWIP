package main.java.f22;

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.json.JSONObject;
import org.apache.flink.api.java.tuple.Tuple1;


public class KeyedSerializationSchemaSelectedEvents implements KeyedSerializationSchema<Tuple1<JSONObject>> {
    private static final long serialVersionUID = 1L;

    @Override
    public String getTargetTopic(Tuple1<JSONObject> element) {
        return "selectedsink";
    }

    @Override
    public byte[] serializeKey(Tuple1<JSONObject> element) {
        return "event".getBytes();
    }

    @Override
    public byte[] serializeValue(Tuple1<JSONObject> element) {
        return element.f0.toString().getBytes();
    }
}
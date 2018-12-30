package main.java.f22;

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.json.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;


public class KeyedSerializationSchemaSelectedEvents implements KeyedSerializationSchema<Tuple2<String, JSONObject>> {
    private static final long serialVersionUID = 1L;

    @Override
    public String getTargetTopic(Tuple2<String, JSONObject> element) {
        return "selectedsink";
    }

    @Override
    public byte[] serializeKey(Tuple2<String, JSONObject> element) {
        return "event".getBytes();
    }

    @Override
    public byte[] serializeValue(Tuple2<String, JSONObject> element) {
        return element.f0.toString().getBytes();
    }
}
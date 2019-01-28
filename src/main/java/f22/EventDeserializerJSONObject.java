package f22;

import java.io.IOException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONObject;

public class EventDeserializerJSONObject extends AbstractDeserializationSchema<Tuple2<String, JSONObject>> {
    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<String, JSONObject> deserialize(byte[] message) throws IOException {
        return Tuple2.of(null, new JSONObject(new String(message, "UTF-8")));
    }
}

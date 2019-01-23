package main.java.experimentation;

import org.apache.flink.api.java.functions.*;
import org.json.JSONObject;
import org.apache.flink.api.java.tuple.Tuple1;

public class KeySelectorJSONObjectId implements KeySelector<Tuple1<JSONObject>, String> {
    private static final long serialVersionUID = 1L;
    // assumes key for id is "id" in JSONObject
    @Override
    public String getKey(Tuple1<JSONObject> value) throws Exception {
        return value.f0.getString("id");
    }

}
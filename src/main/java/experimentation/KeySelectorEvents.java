package main.java.experimentation;

import org.apache.flink.api.java.functions.*;
import org.json.JSONObject;

public class KeySelectorEvents implements KeySelector<JSONObject, String> {
    private static final long serialVersionUID = 1L;
    // assumes key for id is "id" in JSONObject
    @Override
    public String getKey(JSONObject value) throws Exception {
        return value.getString("id");
    }

}
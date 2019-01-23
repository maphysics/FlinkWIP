package main.java.experimentation;

import org.apache.flink.api.java.functions.*;
import java.util.List;

public class KeySelectorSelf implements KeySelector<List<String>, String> {
    private static final long serialVersionUID = 1L;
    // assumes key for id is "id" in JSONObject
    @Override
    public String getKey(List<String> value) throws Exception {
        return value.toString();
    }

}
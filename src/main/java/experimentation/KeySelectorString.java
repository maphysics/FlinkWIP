package main.java.experimentation;

import org.apache.flink.api.java.functions.*;
import org.apache.flink.api.java.tuple.Tuple1;

public class KeySelectorString implements KeySelector<Tuple1<String>, String> {
    private static final long serialVersionUID = 1L;
    // assumes key for id is "id" in JSONObject
    @Override
    public String getKey(Tuple1<String> value) throws Exception {
        return value.f0;
    }

}
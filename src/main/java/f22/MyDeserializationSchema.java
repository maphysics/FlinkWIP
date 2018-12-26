package main.java.f22;

import java.io.IOException;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
// import org.json.JSONObject;

public class MyDeserializationSchema extends AbstractDeserializationSchema<String> {

	private static final long serialVersionUID = 1L;
	@Override
    public String deserialize(byte[] message) throws IOException {
		//JSONObject obj = new JSONObject(new String(message, "UTF-8"));
		//obj.toMap().get("id").toString();
		//System.out.println(new String(message));
        return new String(message);
    }
}

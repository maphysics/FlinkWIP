package main.java.f22;

import java.util.List;
import java.util.ArrayList;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;

public class CheckById extends RichMapFunction<String, List<String>> {
    private static final long serialVersionUID = 1L;
	private transient ValueState<List<String>> sum;

    @Override
    public List<String> map(String input) throws Exception {
        // take the current state
        List<String> current = new ArrayList<String>();
        if (sum.value() != null) {
            current = sum.value();
        }

        current.add(input);
        // update the state
        sum.update(current);
        return current;
    }
}

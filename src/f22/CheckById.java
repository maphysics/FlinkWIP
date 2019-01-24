package f22;

import org.apache.flink.api.common.functions.RichMapFunction;
import java.util.List;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class CheckById extends RichMapFunction<Tuple2<String, String>, List<String>> {
	private static final long serialVersionUID = 1L;
	private ListState<String> sum;

	@Override
    public List<String> map(Tuple2<String, String> input) throws Exception {
    	sum.add(input.f1);
    	return (List<String>) sum.get();
    }

	@Override
	public void open(Configuration cfg) {
        sum = getRuntimeContext().getListState(new ListStateDescriptor<>("skus", String.class));
    }
}

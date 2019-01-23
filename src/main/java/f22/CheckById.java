package main.java.f22;

import java.util.List;
import java.util.ArrayList;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;

/** Purpose of map is to update the list of ids
* Ids are stored in the ValueState object ids
* The map returns the input string without changing it
*/ 
public class CheckById extends RichMapFunction<String, List<String>> {
    private static final long serialVersionUID = 1L;
    private ValueState<List<String>> ids;

    @Override
    public List<String> map(String input) throws Exception {
        // Assumes there is a flush message in the stream
        if ( input == "flush" ) {
            ids.update(new ArrayList<String>());
            return ids.value();
        } else {
            // take the current state
            List<String> current = new ArrayList<String>();
            if (ids.value() != null) {
                current = ids.value();
            }

            current.add(input);
            // update the state
            ids.update(current);
            return current;
        }
    }
}

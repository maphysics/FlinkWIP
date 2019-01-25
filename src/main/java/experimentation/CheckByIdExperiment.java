// package main.java.experimentation;

// import java.util.List;
// import java.util.ArrayList;
// import org.apache.flink.api.common.functions.RichMapFunction;
// import org.apache.flink.api.common.state.ValueState;
// import org.json.JSONObject;

// import org.apache.flink.api.common.state.ListState;
// import org.apache.flink.api.common.state.ListStateDescriptor;

// public class CheckByIdExperiment extends RichMapFunction<JSONObject, List<String>> {
//     private static final long serialVersionUID = 1L;

//     @Override
//     public List<String> map(JSONObject input) throws Exception {



//         // // Assumes there is a flush message in the stream
//         // // if ( input == "flush" ) {
//         // //     ids.update(new ArrayList<String>());
//         // //     return ids.value();
//         // // } else {
//         //     // take the current state
//         //     List<String> current = new ArrayList<String>();
//         //     if (ids.value() != null) {
//         //         current = ids.value();
//         //     }

//         //     current.add(input.getString("id"));
//         //     // update the state
//         //     ids.update(current);
//         //     return current;
//         // // }
//     }

// }

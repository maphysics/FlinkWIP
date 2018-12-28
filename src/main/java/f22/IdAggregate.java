package main.java.f22;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import java.util.ArrayList;
import java.util.List;

public class IdAggregate implements AggregateFunction<Tuple1<String>, Tuple1<List<String>>, List<String>> {
  @Override
  public Tuple1<List<String>> createAccumulator() {
    return new Tuple1<>(new ArrayList<String>());
  }

  @Override
  public Tuple1<List<String>> add(Tuple1<String> value, Tuple1<List<String>> accumulator) {
    List<String> currentIds  = accumulator.f0;
    String newId = value.f0;
    currentIds.add(newId);
    return new Tuple1<>(currentIds);
  }

  @Override
  public List<String> getResult(Tuple1<List<String>> accumulator) {
    return accumulator.f0;
  }

  @Override
  public Tuple1<List<String>> merge(Tuple1<List<String>> a, Tuple1<List<String>> b) {
    a.f0.addAll(b.f0);
    return new Tuple1<List<String>>(a.f0);
  }
}

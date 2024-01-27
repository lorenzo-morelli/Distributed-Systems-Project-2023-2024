package it.polimi.common;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Function;

public class ReduceOperator implements Operator {
    private Function<List<Integer>, Integer> reduceFunction;

    public ReduceOperator(Function<List<Integer>, Integer> reduceFunction) {
        this.reduceFunction = reduceFunction;
    }

    @Override
    public List<KeyValuePair> execute(List<KeyValuePair> input) {
        Map<Integer, List<Integer>> groupedValues = new HashMap<>();

        // Group values by key
        for (KeyValuePair pair : input) {
            groupedValues.computeIfAbsent(pair.getKey(), k -> new ArrayList<>()).add(pair.getValue());
        }

        List<KeyValuePair> output = new ArrayList<>();

        // Apply the reduce function to each group
        for (Map.Entry<Integer, List<Integer>> entry : groupedValues.entrySet()) {
            int key = entry.getKey();
            List<Integer> values = entry.getValue();
            
            // Apply the reduce function and create a KeyValuePair with the result
            int reducedValue = reduceFunction.apply(values);
            output.add(new KeyValuePair(key,reducedValue));
        }

        return output;
    }
}

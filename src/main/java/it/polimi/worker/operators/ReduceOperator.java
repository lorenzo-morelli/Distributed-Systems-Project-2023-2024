package it.polimi.worker.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Function;

import it.polimi.common.KeyValuePair;
import it.polimi.worker.models.Operator;

/**
 * The ReduceOperator class is an operator used to reduce the values of the key-value pairs with the same key.
 * It contains the function that reduces the values.
 * @param reduceFunction represents the function that reduces the values.
 * @return the key-value pairs with the reduced values.
 * @see Operator
 */
public class ReduceOperator implements Operator {
    private final Function<List<Integer>, Integer> reduceFunction;
    /**
     * The constructor creates a new ReduceOperator.
     * @param reduceFunction represents the function that reduces the values.
     */
    public ReduceOperator(Function<List<Integer>, Integer> reduceFunction) {
        this.reduceFunction = reduceFunction;
    }
    /**
     * The execute method executes the operator.
     * It reduces the values of the key-value pairs with the same key.
     * @param input represents the input data on which the operator is executed.
     * @return the key-value pairs with the reduced values.
     */
    @Override
    public List<KeyValuePair> execute(List<KeyValuePair> input) {
        Map<Integer, List<Integer>> groupedValues = new HashMap<>();

        for (KeyValuePair pair : input) {
            groupedValues.computeIfAbsent(pair.key(), k -> new ArrayList<>()).add(pair.value());
        }

        List<KeyValuePair> output = new ArrayList<>();

        for (Map.Entry<Integer, List<Integer>> entry : groupedValues.entrySet()) {
            int key = entry.getKey();
            List<Integer> values = entry.getValue();

            int reducedValue = reduceFunction.apply(values);
            output.add(new KeyValuePair(key, reducedValue));
        }

        return output;
    }
}

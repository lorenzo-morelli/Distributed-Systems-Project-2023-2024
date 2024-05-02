package it.polimi.worker.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import it.polimi.common.KeyValuePair;
import it.polimi.worker.models.Operator;

/**
 * The FilterOperator class is an operator used to filter the key-value pairs.
 * It contains the function that filters the key-value pairs.
 *
 * @see Operator
 */
public class FilterOperator implements Operator {
    private final Predicate<Integer> filterFunction;

    /**
     * The constructor creates a new FilterOperator.
     *
     * @param filterFunction represents the function that filters the key-value pairs.
     */
    public FilterOperator(Predicate<Integer> filterFunction) {
        this.filterFunction = filterFunction;
    }

    /**
     * The execute method executes the operator.
     * It filters the key-value pairs that satisfy the filter function.
     *
     * @param input represents the input data on which the operator is executed.
     * @return the key-value pairs that satisfy the filter function.
     */
    @Override
    public List<KeyValuePair> execute(List<KeyValuePair> input) {
        List<KeyValuePair> output = new ArrayList<>();

        for (KeyValuePair pair : input) {
            if (filterFunction.test(pair.value())) {
                output.add(pair);
            }
        }
        return output;
    }
}

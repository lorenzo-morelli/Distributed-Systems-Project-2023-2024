package it.polimi.worker.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import it.polimi.common.KeyValuePair;
import it.polimi.worker.models.Operator;

public class FilterOperator implements Operator {
    private final Predicate<Integer> filterFunction;

    public FilterOperator(Predicate<Integer> filterFunction) {
        this.filterFunction = filterFunction;
    }

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

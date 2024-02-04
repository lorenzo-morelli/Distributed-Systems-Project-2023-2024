package it.polimi.common.operators;

import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import it.polimi.common.KeyValuePair;
import it.polimi.common.Operator;

public class MapOperator implements Operator {
    private final IntUnaryOperator function;

    public MapOperator(IntUnaryOperator function) {
        this.function = function;
    }

    @Override
    public List<KeyValuePair> execute(List<KeyValuePair> input) {
        // Implementation of the map operator
        return input.stream()
                .map(kv -> new KeyValuePair(kv.getKey(), function.applyAsInt(kv.getValue())))
                .collect(Collectors.toList());
    }
}
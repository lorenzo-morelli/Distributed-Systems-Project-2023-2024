package it.polimi.worker.operators;

import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import it.polimi.common.KeyValuePair;
import it.polimi.worker.models.Operator;

/**
 * The MapOperator class is an operator used to apply a function to the value of the key-value pair.
 * It contains the function that is applied to the value.
 *
 * @see Operator
 */
public class MapOperator implements Operator {
    private final IntUnaryOperator function;

    /**
     * The constructor creates a new MapOperator.
     *
     * @param function represents the function that is applied to the value.
     */
    public MapOperator(IntUnaryOperator function) {
        this.function = function;
    }

    /**
     * The execute method executes the operator.
     * It applies the function to the value of the key-value pair.
     *
     * @param input represents the input data on which the operator is executed.
     * @return the key-value pairs with the transformed value.
     */
    @Override
    public List<KeyValuePair> execute(List<KeyValuePair> input) {
        return input.stream()
                .map(kv -> new KeyValuePair(kv.key(), function.applyAsInt(kv.value())))
                .collect(Collectors.toList());
    }
}
package it.polimi.worker.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntUnaryOperator;

import it.polimi.common.KeyValuePair;
import it.polimi.worker.models.Operator;

public class ChangeKeyOperator implements Operator {
    private final IntUnaryOperator function;

    public ChangeKeyOperator(IntUnaryOperator function) {
        this.function = function;
    }

    @Override
    public List<KeyValuePair> execute(List<KeyValuePair> input) {
        List<KeyValuePair> output = new ArrayList<>();

        for (KeyValuePair pair : input) {
            // Apply the key transformation function to change the key
            int transformedKey = function.applyAsInt(pair.value());
            KeyValuePair transformedPair = new KeyValuePair(transformedKey, pair.value());
            output.add(transformedPair);
        }

        return output;
    }
}

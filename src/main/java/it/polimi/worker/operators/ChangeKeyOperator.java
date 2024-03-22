package it.polimi.worker.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntUnaryOperator;

import it.polimi.common.KeyValuePair;
import it.polimi.worker.models.Operator;
/**
 * The ChangeKeyOperator class is an operator used to change the key of the key-value pair.
 * It contains the function that changes the key.
 * @param function represents the function that changes the key.
 * @return the key-value pairs with the changed key.
 * @see Operator
 */
public class ChangeKeyOperator implements Operator {
    private final IntUnaryOperator function;

    /**
     * The constructor creates a new ChangeKeyOperator.
     * @param function represents the function that changes the key.
     */
    public ChangeKeyOperator(IntUnaryOperator function) {
        this.function = function;
    }

    /**
     * The execute method executes the operator.
     * It applies the key transformation function to change the key.
     * @param input represents the input data on which the operator is executed.
     * @return the key-value pairs with the changed key.
     */
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

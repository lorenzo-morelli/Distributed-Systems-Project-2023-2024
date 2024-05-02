package it.polimi.worker.models;

import java.util.List;

import it.polimi.common.KeyValuePair;

/**
 * The Operator interface is used to represent the operator.
 * It contains the execute method that executes the operator.
 *
 * @see KeyValuePair
 */
public interface Operator {
    /**
     * The execute method executes the operator.
     *
     * @param input represents the input data on which the operator is executed.
     * @return the output data after the operator is executed.
     */
    List<KeyValuePair> execute(List<KeyValuePair> input);
}
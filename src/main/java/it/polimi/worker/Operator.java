package it.polimi.worker;

import java.util.List;

import it.polimi.common.KeyValuePair;

// Operator interface
public interface Operator {
    List<KeyValuePair> execute(List<KeyValuePair> input);
}